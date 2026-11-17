"""数据集市持久化仓库：分区、事实行、迁移记录与表版本都落在 SQLite。

历史项目里这张库最开始只有按日期目录分区和一张迁移记录表（schema v1）。
这里把建库 + 升级拆成显式版本步骤：
- v1：集市表、分区、事实、迁移记录的最小结构；
- v2：补上事实粒度 / 关联维度 / 度量字段、版本指针、迁移批次与核对字段；
- v3：迁移记录补上数据入口与回滚范围，支持同表多来源、按版本回查。

打开任何历史库都会按 schema_meta 里的版本逐级补齐，旧数据原样保留。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from app.config import settings
from app.mart_seed import (
    LEGACY_FORMAT,
    LEGACY_VERSION,
    MART_TABLE_DEFS,
    build_legacy_dataset,
)

SCHEMA_VERSION = 3

MIGRATION_FIELDS = [
    "table_id", "source_version", "target_version", "mode", "status", "batch_no",
    "total_partitions", "migrated_partitions", "total_facts", "migrated_facts",
    "grain_signature", "dim_signature", "measure_signature", "source_signature",
    "plan", "checkpoint", "requested_by", "created_at", "started_at", "finished_at",
    "rolled_back_at", "confirm_token", "reconciled_at", "reconcile_status",
    "reconcile_signature", "error", "source", "rollback_scope",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


class MartRepository:
    """单连接 + 可重入锁的 SQLite 仓库；复合操作由 service 用 locked() 包起来。"""

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or settings.mart_db_path
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=15)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_or_upgrade()
        self._seed_if_needed()

    # ------------------------------------------------------------------ 基础
    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @contextmanager
    def locked(self) -> Iterator[sqlite3.Connection]:
        """串行化写事务：并发提交（迁移、核对）最终只落一条确认结果。"""
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def read(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_dict(row) or {} for row in rows]

    def schema_version(self) -> int:
        with self._lock:
            if not self._table_exists(self._conn, "schema_meta"):
                return 0
            row = self._conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
        return int(row["value"]) if row else 0

    # -------------------------------------------------------------- 建库升级
    @staticmethod
    def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return row is not None

    @staticmethod
    def _columns(conn: sqlite3.Connection, name: str) -> set[str]:
        return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({name})")}

    def _init_or_upgrade(self) -> None:
        with self._lock:
            conn = self._conn
            if not self._table_exists(conn, "schema_meta"):
                if self._table_exists(conn, "mart_table"):
                    # 更早的库没有 schema_meta，视为 v1。
                    conn.execute("CREATE TABLE IF NOT EXISTS schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                    conn.execute("INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version','1')")
                else:
                    self._create_v1(conn)
            version = self.schema_version()
            if version == 1:
                self._upgrade_v1_to_v2(conn)
                version = 2
            if version == 2:
                self._upgrade_v2_to_v3(conn)
                version = 3
            if version != SCHEMA_VERSION:
                raise RuntimeError(f"数据集市库版本 {version} 不受支持，当前代码支持 {SCHEMA_VERSION}")
            conn.commit()

    def _create_v1(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE mart_table (
                table_id INTEGER PRIMARY KEY,
                table_code TEXT NOT NULL UNIQUE,
                table_name TEXT NOT NULL,
                subject TEXT NOT NULL DEFAULT '',
                legacy_version TEXT NOT NULL DEFAULT 'legacy'
            );
            CREATE TABLE mart_partition (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER NOT NULL,
                partition_date TEXT NOT NULL,
                version TEXT NOT NULL,
                format TEXT NOT NULL,
                path TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                UNIQUE(table_id, version, partition_date)
            );
            CREATE TABLE mart_fact (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER NOT NULL,
                partition_date TEXT NOT NULL,
                version TEXT NOT NULL,
                grain_key TEXT NOT NULL,
                dims TEXT NOT NULL,
                measures TEXT NOT NULL,
                status TEXT NOT NULL,
                UNIQUE(table_id, version, grain_key)
            );
            CREATE TABLE mart_migration (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER NOT NULL,
                source_version TEXT NOT NULL,
                target_version TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(table_id, target_version)
            );
            CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta(key, value) VALUES('schema_version','1');
            """
        )
        conn.commit()

    def _upgrade_v1_to_v2(self, conn: sqlite3.Connection) -> None:
        conn.execute("BEGIN")
        table_columns = {
            "fact_grain": "TEXT NOT NULL DEFAULT '按日'",
            "dimensions": "TEXT NOT NULL DEFAULT '[]'",
            "measures": "TEXT NOT NULL DEFAULT '[]'",
            "refresh_cycle": "TEXT NOT NULL DEFAULT '每日'",
            "current_version": f"TEXT NOT NULL DEFAULT '{LEGACY_VERSION}'",
            "version_seq": "INTEGER NOT NULL DEFAULT 0",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
        }
        for name, ddl in table_columns.items():
            if name not in self._columns(conn, "mart_table"):
                conn.execute(f"ALTER TABLE mart_table ADD COLUMN {name} {ddl}")

        for name, ddl in {
            "migrated_from": "TEXT",
            "migration_id": "INTEGER",
        }.items():
            if name not in self._columns(conn, "mart_partition"):
                conn.execute(f"ALTER TABLE mart_partition ADD COLUMN {name} {ddl}")
        if "source_path" not in self._columns(conn, "mart_fact"):
            conn.execute("ALTER TABLE mart_fact ADD COLUMN source_path TEXT")

        migration_columns = {
            "batch_no": "INTEGER NOT NULL DEFAULT 0",
            "total_partitions": "INTEGER NOT NULL DEFAULT 0",
            "migrated_partitions": "INTEGER NOT NULL DEFAULT 0",
            "total_facts": "INTEGER NOT NULL DEFAULT 0",
            "migrated_facts": "INTEGER NOT NULL DEFAULT 0",
            "grain_signature": "TEXT NOT NULL DEFAULT ''",
            "dim_signature": "TEXT NOT NULL DEFAULT ''",
            "measure_signature": "TEXT NOT NULL DEFAULT ''",
            "source_signature": "TEXT NOT NULL DEFAULT ''",
            "plan": "TEXT NOT NULL DEFAULT '[]'",
            "checkpoint": "TEXT NOT NULL DEFAULT '{}'",
            "requested_by": "TEXT NOT NULL DEFAULT ''",
            "started_at": "TEXT NOT NULL DEFAULT ''",
            "finished_at": "TEXT NOT NULL DEFAULT ''",
            "rolled_back_at": "TEXT NOT NULL DEFAULT ''",
            "confirm_token": "TEXT NOT NULL DEFAULT ''",
            "reconciled_at": "TEXT NOT NULL DEFAULT ''",
            "reconcile_status": "TEXT NOT NULL DEFAULT ''",
            "reconcile_signature": "TEXT NOT NULL DEFAULT ''",
            "error": "TEXT NOT NULL DEFAULT ''",
        }
        for name, ddl in migration_columns.items():
            if name not in self._columns(conn, "mart_migration"):
                conn.execute(f"ALTER TABLE mart_migration ADD COLUMN {name} {ddl}")

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS mart_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER NOT NULL,
                version TEXT NOT NULL,
                format TEXT NOT NULL,
                status TEXT NOT NULL,
                published_at TEXT NOT NULL DEFAULT '',
                UNIQUE(table_id, version)
            );
            CREATE INDEX IF NOT EXISTS idx_mart_partition_table ON mart_partition(table_id, version, status);
            CREATE INDEX IF NOT EXISTS idx_mart_fact_table ON mart_fact(table_id, version, status);
            CREATE INDEX IF NOT EXISTS idx_mart_migration_table ON mart_migration(table_id, status);
            """
        )
        conn.execute("UPDATE schema_meta SET value = '2' WHERE key = 'schema_version'")
        conn.commit()

    def _upgrade_v2_to_v3(self, conn: sqlite3.Connection) -> None:
        """v3：迁移记录补上来源与回滚范围，支持同表多来源、按版本回查。"""
        conn.execute("BEGIN")
        for name, ddl in {
            "source": "TEXT NOT NULL DEFAULT '旧版集市分区'",
            "rollback_scope": "TEXT NOT NULL DEFAULT '目标版本分区'",
        }.items():
            if name not in self._columns(conn, "mart_migration"):
                conn.execute(f"ALTER TABLE mart_migration ADD COLUMN {name} {ddl}")
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_mart_migration_version
                ON mart_migration(table_id, target_version);
            """
        )
        conn.execute("UPDATE schema_meta SET value = '3' WHERE key = 'schema_version'")
        conn.commit()

    # ------------------------------------------------------------------ 种子
    def _seed_if_needed(self) -> None:
        with self.locked() as conn:
            count = conn.execute("SELECT COUNT(*) AS c FROM mart_table").fetchone()["c"]
            if count:
                # 历史库补全景字段，不覆盖已有的版本指针。
                for definition in MART_TABLE_DEFS:
                    self._backfill_table_definition(conn, definition)
                return
            for definition in MART_TABLE_DEFS:
                partitions, facts = build_legacy_dataset(definition)
                self._insert_table_row(conn, definition)
                for partition in partitions:
                    self._insert_partition(conn, partition)
                for fact in facts:
                    self._insert_fact(conn, fact)
                self._ensure_version(conn, definition["table_id"], LEGACY_VERSION, LEGACY_FORMAT, "当前")
                conn.execute(
                    "UPDATE mart_table SET updated_at = ? WHERE table_id = ?",
                    (utc_now(), definition["table_id"]),
                )

    def _backfill_table_definition(self, conn: sqlite3.Connection, definition: dict[str, Any]) -> None:
        row = conn.execute(
            "SELECT * FROM mart_table WHERE table_id = ? OR table_code = ?",
            (definition["table_id"], definition["table_code"]),
        ).fetchone()
        if row is None:
            return
        table_id = int(row["table_id"])
        updates: dict[str, Any] = {}
        if not row["fact_grain"] or row["dimensions"] in ("", "[]"):
            updates["fact_grain"] = definition["fact_grain"]
            updates["dimensions"] = json.dumps(definition["dimensions"], ensure_ascii=False)
            updates["measures"] = json.dumps(definition["measures"], ensure_ascii=False)
            updates["refresh_cycle"] = definition["refresh_cycle"]
        if not row["current_version"]:
            updates["current_version"] = LEGACY_VERSION
        if updates:
            assignments = ", ".join(f"{key} = ?" for key in updates)
            conn.execute(
                f"UPDATE mart_table SET {assignments} WHERE table_id = ?",
                (*updates.values(), table_id),
            )
        partition_count = conn.execute(
            "SELECT COUNT(*) AS c FROM mart_partition WHERE table_id = ?", (table_id,)
        ).fetchone()["c"]
        if not partition_count:
            partitions, facts = build_legacy_dataset(definition)
            for partition in partitions:
                self._insert_partition(conn, partition)
            for fact in facts:
                self._insert_fact(conn, fact)
        self._ensure_version(conn, table_id, LEGACY_VERSION, LEGACY_FORMAT, "当前")

    @staticmethod
    def _insert_table_row(conn: sqlite3.Connection, definition: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO mart_table(
                table_id, table_code, table_name, subject, legacy_version,
                fact_grain, dimensions, measures, refresh_cycle,
                current_version, version_seq, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(definition["table_id"]), definition["table_code"], definition["table_name"],
                definition.get("subject", ""), LEGACY_VERSION, definition["fact_grain"],
                json.dumps(definition["dimensions"], ensure_ascii=False),
                json.dumps(definition["measures"], ensure_ascii=False),
                definition["refresh_cycle"], LEGACY_VERSION, 0, utc_now(),
            ),
        )

    @staticmethod
    def _insert_partition(conn: sqlite3.Connection, row: dict[str, Any]) -> int:
        cursor = conn.execute(
            """
            INSERT INTO mart_partition(table_id, partition_date, version, format, path,
                                      row_count, status, migrated_from, migration_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["table_id"]), row["partition_date"], row["version"], row["format"],
                row["path"], int(row.get("row_count", 0)), row["status"],
                row.get("migrated_from"), row.get("migration_id"),
            ),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _insert_fact(conn: sqlite3.Connection, row: dict[str, Any]) -> int:
        cursor = conn.execute(
            """
            INSERT INTO mart_fact(table_id, partition_date, version, grain_key, dims,
                                  measures, status, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["table_id"]), row["partition_date"], row["version"], row["grain_key"],
                row["dims"], row["measures"], row["status"], row.get("source_path"),
            ),
        )
        return int(cursor.lastrowid)

    # ------------------------------------------------------------------ 表
    def list_tables(self) -> list[dict[str, Any]]:
        rows = self.read("SELECT * FROM mart_table ORDER BY table_id")
        return [self._decorate_table(row) for row in rows]

    def get_table(self, table_id: int) -> dict[str, Any] | None:
        rows = self.read("SELECT * FROM mart_table WHERE table_id = ?", (table_id,))
        return self._decorate_table(rows[0]) if rows else None

    def find_table_by_code(self, table_code: str) -> dict[str, Any] | None:
        rows = self.read("SELECT * FROM mart_table WHERE table_code = ?", (table_code,))
        return self._decorate_table(rows[0]) if rows else None

    def _decorate_table(self, row: dict[str, Any]) -> dict[str, Any]:
        for field in ("dimensions", "measures"):
            try:
                row[field] = json.loads(row.get(field) or "[]")
            except json.JSONDecodeError:
                row[field] = []
        table_id = int(row["table_id"])
        row["partition_count"] = len(self.read(
            "SELECT id FROM mart_partition WHERE table_id = ? AND version = ?",
            (table_id, row.get("current_version") or LEGACY_VERSION),
        ))
        row["fact_count"] = len(self.read(
            "SELECT id FROM mart_fact WHERE table_id = ? AND version = ?",
            (table_id, row.get("current_version") or LEGACY_VERSION),
        ))
        return row

    def insert_table(self, definition: dict[str, Any]) -> dict[str, Any]:
        with self.locked() as conn:
            self._insert_table_row(conn, definition)
            self._ensure_version(conn, definition["table_id"], LEGACY_VERSION, LEGACY_FORMAT, "当前")
        table = self.get_table(int(definition["table_id"]))
        assert table is not None
        return table

    def create_table_with_legacy(self, definition: dict[str, Any]) -> dict[str, Any]:
        """新建集市表时同时准备旧版日期目录分区与存量事实，后续可立即迁移。"""
        partitions, facts = build_legacy_dataset(definition)
        with self.locked() as conn:
            self._insert_table_row(conn, definition)
            for partition in partitions:
                self._insert_partition(conn, partition)
            for fact in facts:
                self._insert_fact(conn, fact)
            self._ensure_version(conn, definition["table_id"], LEGACY_VERSION, LEGACY_FORMAT, "当前")
        table = self.get_table(int(definition["table_id"]))
        assert table is not None
        return table

    def update_table_fields(self, table_id: int, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields)
        with self.locked() as conn:
            conn.execute(
                f"UPDATE mart_table SET {assignments}, updated_at = ? WHERE table_id = ?",
                (*fields.values(), utc_now(), table_id),
            )

    # ------------------------------------------------------------------ 版本
    def list_versions(self, table_id: int) -> list[dict[str, Any]]:
        return self.read(
            "SELECT * FROM mart_version WHERE table_id = ? ORDER BY id", (table_id,)
        )

    def _ensure_version(
        self, conn: sqlite3.Connection, table_id: int, version: str, fmt: str, status: str
    ) -> None:
        # 已有版本行不覆盖状态：迁移后 legacy 应为“历史”，重复种子不能把它翻回“当前”。
        conn.execute(
            """
            INSERT OR IGNORE INTO mart_version(table_id, version, format, status, published_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (table_id, version, fmt, status, utc_now()),
        )

    def next_version_name(self, table_id: int) -> str:
        row = self.read("SELECT version_seq FROM mart_table WHERE table_id = ?", (table_id,))
        seq = int(row[0]["version_seq"]) if row else 0
        return f"v{seq + 1}"

    # ------------------------------------------------------------- 分区/事实
    def list_partitions(
        self, table_id: int, version: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM mart_partition WHERE table_id = ?"
        params: list[Any] = [table_id]
        if version:
            sql += " AND version = ?"
            params.append(version)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY partition_date, id"
        return self.read(sql, tuple(params))

    def list_facts(
        self, table_id: int, version: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM mart_fact WHERE table_id = ?"
        params: list[Any] = [table_id]
        if version:
            sql += " AND version = ?"
            params.append(version)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY grain_key, id"
        return self.read(sql, tuple(params))

    # ------------------------------------------------------------------ 迁移
    def get_migration(self, migration_id: int) -> dict[str, Any] | None:
        rows = self.read("SELECT * FROM mart_migration WHERE id = ?", (migration_id,))
        return self._decorate_migration(rows[0]) if rows else None

    def get_migration_raw(self, migration_id: int) -> dict[str, Any] | None:
        rows = self.read("SELECT * FROM mart_migration WHERE id = ?", (migration_id,))
        return rows[0] if rows else None

    def find_migration(self, table_id: int, target_version: str) -> dict[str, Any] | None:
        rows = self.read(
            "SELECT * FROM mart_migration WHERE table_id = ? AND target_version = ?",
            (table_id, target_version),
        )
        return self._decorate_migration(rows[0]) if rows else None

    def find_open_migration(self, table_id: int) -> dict[str, Any] | None:
        rows = self.read(
            """
            SELECT * FROM mart_migration
            WHERE table_id = ? AND status IN ('预览', '执行中', '已中断')
            ORDER BY id DESC LIMIT 1
            """,
            (table_id,),
        )
        return self._decorate_migration(rows[0]) if rows else None

    def list_migrations(
        self, table_id: int | None = None, version: str | None = None, status: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM mart_migration WHERE 1 = 1"
        params: list[Any] = []
        if table_id is not None:
            sql += " AND table_id = ?"
            params.append(table_id)
        if version:
            sql += " AND target_version = ?"
            params.append(version)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY table_id, id"
        return [self._decorate_migration(row) for row in self.read(sql, tuple(params))]

    def _decorate_migration(self, row: dict[str, Any]) -> dict[str, Any]:
        plan = row.get("plan") or "[]"
        try:
            row["plan"] = json.loads(plan)
        except json.JSONDecodeError:
            row["plan"] = []
        row["has_checkpoint"] = bool(row.get("checkpoint") and row.get("checkpoint") != "{}")
        row.pop("checkpoint", None)
        row["batches"] = row.get("plan") if isinstance(row.get("plan"), list) else []
        row["version_order"] = int(row.get("batch_no") or 0)
        return row

    def insert_migration(self, conn: sqlite3.Connection, fields: dict[str, Any]) -> int:
        payload = {field: fields.get(field) for field in MIGRATION_FIELDS}
        payload["created_at"] = payload.get("created_at") or utc_now()
        payload["status"] = payload.get("status") or "预览"
        for field in ("total_partitions", "migrated_partitions", "total_facts", "migrated_facts", "batch_no"):
            payload[field] = int(payload.get(field) or 0)
        for field in ("plan", "checkpoint"):
            value = payload.get(field)
            payload[field] = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        payload["plan"] = payload["plan"] or "[]"
        payload["checkpoint"] = payload["checkpoint"] or "{}"
        for field in ("grain_signature", "dim_signature", "measure_signature", "source_signature",
                      "requested_by", "started_at", "finished_at", "rolled_back_at",
                      "confirm_token", "reconciled_at", "reconcile_status",
                      "reconcile_signature", "error"):
            payload[field] = payload[field] or ""
        payload["source"] = payload.get("source") or "旧版集市分区"
        payload["rollback_scope"] = payload.get("rollback_scope") or "目标版本分区"
        columns = ", ".join(MIGRATION_FIELDS)
        placeholders = ", ".join("?" for _ in MIGRATION_FIELDS)
        cursor = conn.execute(
            f"INSERT INTO mart_migration({columns}) VALUES ({placeholders})",
            tuple(payload[field] for field in MIGRATION_FIELDS),
        )
        return int(cursor.lastrowid)

    def update_migration(self, conn: sqlite3.Connection, migration_id: int, **fields: Any) -> None:
        if not fields:
            return
        for name in ("plan", "checkpoint"):
            if name in fields and not isinstance(fields[name], str):
                fields[name] = json.dumps(fields[name], ensure_ascii=False)
        assignments = ", ".join(f"{key} = ?" for key in fields)
        conn.execute(
            f"UPDATE mart_migration SET {assignments} WHERE id = ?",
            (*fields.values(), migration_id),
        )


_repository: MartRepository | None = None


def get_repository(db_path: str | None = None) -> MartRepository:
    """进程内共享仓库；测试可传自己的路径拿到独立实例。"""
    global _repository
    if db_path is not None:
        return MartRepository(db_path)
    if _repository is None:
        _repository = MartRepository()
    return _repository


def reset_repository_for_tests(db_path: str | None = None) -> MartRepository:
    global _repository
    if _repository is not None:
        _repository.conn.close()
    _repository = MartRepository(db_path)
    return _repository
