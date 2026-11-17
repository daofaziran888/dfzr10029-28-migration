"""数据集市业务规则：表登记 / 状态流转 / 分区迁移 / 版本发布 / 核对。

关键约束：
- 旧版按日期目录（dt=YYYY-MM-DD）存放，新版是版本化分区（version=<v>/dt=...）；
- 迁移必须保留事实粒度、关联维度和度量结果，核对以源版本为基准；
- 迁移和表版本指针在同一事务里切换，看板只按当前版本读，不会混读新旧目录；
- 影子分区切换在回填期间保持旧目录对外可见，完成后一次性切换；原位批量迁移按批
  让已迁移分区逐个可见——两种模式的可见版本与回滚范围不同，都记录在迁移记录上；
- 迁移中断后可从 checkpoint 续跑，也可按模式回滚；
- 核对结果持久化在迁移记录上，重启后按同一版本回查。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from app.mart_seed import (
    LEGACY_FORMAT,
    LEGACY_VERSION,
    VERSIONED_FORMAT,
    make_table_definition,
    versioned_partition_path,
)
from app.services.mart_repository import MartRepository, get_repository, utc_now
from app.store import store

MODULE = "data_mart"
REQUIRED_FIELDS = ["表编号", "表名称", "主题域"]
STATUS_ORDER = ["草稿", "已建模", "已发布", "已下线"]
ACTION_RULES = {"提交建模": "已建模", "发布集市": "已发布", "下线集市": "已下线"}
NEGATIVE_ACTIONS = ["下线集市"]

MIGRATION_MODES = {
    "影子分区切换": {
        "rollback_scope": "目标版本全量分区（影子切换回退）",
        "description": "旧日期目录继续对外服务，新版本分区先写影子目录，全部回填后一次性切换版本指针。",
        "visible_during": "旧版日期目录（新分区影子回填，不对外开放）",
    },
    "原位批量迁移": {
        "rollback_scope": "已迁移批次分区（原位回退）",
        "description": "按批次把旧日期目录分区原位改写成版本化分区，已迁移批次立即对新查询可见。",
        "visible_during": "新旧分区逐批交替可见（每个日期只命中一个版本）",
    },
}
MIGRATION_SOURCES = ["旧版集市分区", "存量事实数据"]
VISIBLE_PARTITION_STATUSES = ("已发布", "已回填", "已切换", "已生效")


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def _json_load(value: Any, default: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value) if value else default
    except (TypeError, json.JSONDecodeError):
        return default


def _signature(payload: Any) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def fact_signature(facts: list[dict[str, Any]]) -> dict[str, Any]:
    """从事实行提取粒度、维度、度量三类指纹，用于迁移前后核对。"""
    grains = sorted(str(fact.get("grain_key", "")) for fact in facts)
    dims = sorted(
        json.dumps(_json_load(fact.get("dims"), {}), ensure_ascii=False, sort_keys=True)
        for fact in facts
    )
    measures = sorted(
        json.dumps(_json_load(fact.get("measures"), {}), ensure_ascii=False, sort_keys=True)
        for fact in facts
    )
    totals: dict[str, float] = {}
    for fact in facts:
        for key, value in _json_load(fact.get("measures"), {}).items():
            if isinstance(value, (int, float)):
                totals[str(key)] = round(totals.get(str(key), 0) + float(value), 4)
    return {
        "grain_count": len(grains),
        "grain_signature": _signature(grains),
        "dim_signature": _signature(dims),
        "measure_signature": _signature(measures),
        "measure_totals": dict(sorted(totals.items())),
        "signature": _signature({"grain": grains, "dims": dims, "measures": measures}),
    }


class DataMartService:
    def __init__(self, repository: MartRepository | None = None) -> None:
        self.repo = repository or get_repository()
        self._sync_store_rows()

    # ------------------------------------------------------------ 旧有图接口
    def _sync_store_rows(self) -> None:
        """把持久化的集市表镜像到业务模块行，旧页面/概览/动作链保持可用。"""
        tables = self.repo.list_tables()
        existing = store.rows(MODULE)
        by_id = {int(row.get("id", 0)): row for row in existing}
        by_code = {str(row.get("表编号", "")): row for row in existing}
        for table in tables:
            table_id = int(table["table_id"])
            row = by_id.get(table_id) or by_code.get(table["table_code"])
            if row is None:
                row = {"id": table_id, "pending": True, "abnormal": False, "status": "草稿"}
                existing.append(row)
            row.setdefault("status", "草稿")
            row["id"] = table_id
            row["表编号"] = table["table_code"]
            row["表名称"] = table["table_name"]
            row["主题域"] = table["subject"]
            row["事实粒度"] = table["fact_grain"]
            row["关联维度"] = "，".join(table.get("dimensions") or [])
            row["度量字段"] = "，".join(table.get("measures") or [])
            row["刷新周期"] = table["refresh_cycle"]
            row["表状态"] = row.get("status", "草稿")
            row["分区版本"] = table["current_version"]
            row["分区格式"] = (
                VERSIONED_FORMAT if table["current_version"] != LEGACY_VERSION else LEGACY_FORMAT
            )
            row["版本序号"] = table["version_seq"]
            row["分区数量"] = table["partition_count"]
            row["事实行数"] = table["fact_count"]
            row["_table"] = True

    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            if row.get("_table"):
                row["表状态"] = row.get("status", "草稿")
                row["分区版本"] = row.get("分区版本") or LEGACY_VERSION
                row["分区格式"] = (
                    VERSIONED_FORMAT if row.get("分区版本") != LEGACY_VERSION else LEGACY_FORMAT
                )
            nodes.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("表名称") or row.get("name") or ""),
                "status": str(row.get("status", STATUS_ORDER[0])),
                **{k: v for k, v in row.items()
                   if k not in ("id", "name", "status", "pending", "abnormal", "_table")},
            })
        for i in range(len(nodes) - 1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i + 1]["id"], "label": "依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        table = self.repo.get_table(entry_id)
        if entry is None and table is None:
            return None
        if entry is None:
            self._sync_store_rows()
            entry = store.find(MODULE, entry_id)
        if entry is not None and table is not None:
            entry["分区版本"] = table["current_version"]
            entry["分区格式"] = (
                VERSIONED_FORMAT if table["current_version"] != LEGACY_VERSION else LEGACY_FORMAT
            )
            entry["版本序号"] = table["version_seq"]
            entry["分区数量"] = table["partition_count"]
            entry["事实行数"] = table["fact_count"]
            entry["_table"] = True
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        existing_ids = [int(row.get("id", 0)) for row in store.rows(MODULE)]
        repo_ids = [int(table["table_id"]) for table in self.repo.list_tables()]
        next_id = max(existing_ids + repo_ids + [0]) + 1
        entry = {"id": next_id}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        for optional in ("事实粒度", "关联维度", "度量字段", "刷新周期"):
            if values.get(optional) is not None:
                entry[optional] = values.get(optional)
        entry["status"] = STATUS_ORDER[0]
        entry["表状态"] = entry["status"]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["_table"] = True

        table_code = str(entry.get("表编号") or f"DATA-{next_id:04d}").strip()
        if self.repo.find_table_by_code(table_code):
            table_code = f"{table_code}-{next_id}"
            entry["表编号"] = table_code
        definition = make_table_definition(
            table_id=next_id,
            table_code=table_code,
            table_name=str(entry.get("表名称") or ""),
            subject=str(entry.get("主题域") or ""),
            fact_grain=entry.get("事实粒度"),
            dimensions=entry.get("关联维度"),
            measures=entry.get("度量字段"),
            refresh_cycle=entry.get("刷新周期"),
        )
        table = self.repo.create_table_with_legacy(definition)
        entry.update({
            "分区版本": table["current_version"],
            "分区格式": LEGACY_FORMAT,
            "版本序号": table["version_seq"],
            "分区数量": table["partition_count"],
            "事实行数": table["fact_count"],
        })
        store.rows(MODULE).append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"集市表 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据集市可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["表状态"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"集市表已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # -------------------------------------------------------------- 表与版本
    def list_tables(self) -> list[dict[str, Any]]:
        self._sync_store_rows()
        return [self._table_view(table) for table in self.repo.list_tables()]

    def _table_view(self, table: dict[str, Any]) -> dict[str, Any]:
        view = dict(table)
        view["partition_format"] = (
            VERSIONED_FORMAT if table["current_version"] != LEGACY_VERSION else LEGACY_FORMAT
        )
        view["current_version_label"] = (
            "旧版日期目录" if table["current_version"] == LEGACY_VERSION else "版本化分区"
        )
        view["versions"] = self.repo.list_versions(int(table["table_id"]))
        view["open_migration"] = self.repo.find_open_migration(int(table["table_id"]))
        view["latest_migration"] = self._latest_migration(int(table["table_id"]))
        return view

    def get_table(self, table_id: int) -> dict[str, Any] | None:
        table = self.repo.get_table(table_id)
        return self._table_view(table) if table else None

    def list_versions(self, table_id: int) -> list[dict[str, Any]] | None:
        if self.repo.get_table(table_id) is None:
            return None
        return self.repo.list_versions(table_id)

    def _latest_migration(self, table_id: int, version: str | None = None) -> dict[str, Any] | None:
        records = self.repo.list_migrations(table_id=table_id, version=version)
        return records[-1] if records else None

    def publish_version(self, table_id: int, version: str, remark: str = "") -> tuple[dict[str, Any] | None, str]:
        """切换看板可见版本；版本指针与分区状态同步，避免同时读到旧目录和新分区。"""
        table = self.repo.get_table(table_id)
        if table is None:
            return None, f"集市表 {table_id} 不存在"
        versions = {row["version"]: row for row in self.repo.list_versions(table_id)}
        if version not in versions:
            return None, f"版本「{version}」尚未在集市表中建立，无法发布"
        if table["current_version"] == version:
            return self._table_view(table), f"版本「{version}」已是当前可见版本"
        open_migration = self.repo.find_open_migration(table_id)
        if open_migration is not None:
            return None, (
                f"迁移记录 #{open_migration['id']} 正在进行，版本切换会与分区回填冲突，请先结束迁移"
            )
        partitions = self.repo.list_partitions(table_id, version=version)
        if not partitions:
            return None, f"版本「{version}」没有可切换的分区，请先完成迁移回填"
        # 历史版本的完整性由“是否还有未回填分区”判断；已归档的旧目录可以直接切回。
        pending = [
            p for p in partitions
            if p.get("status") not in VISIBLE_PARTITION_STATUSES + ("历史",)
        ]
        if pending:
            return None, f"版本「{version}」尚有 {len(pending)} 个分区未回填完成，不能切换"
        migration = self._latest_migration(table_id, version)
        if migration is not None and migration.get("status") not in ("已执行", "已回滚", None):
            return None, f"版本「{version}」的迁移记录状态为「{migration.get('status')}」，不能发布"
        previous = table["current_version"]
        with self.repo.locked() as conn:
            # 同一事务内把版本指针和分区可见性一起切换：看板不会读到半步状态。
            for name in versions:
                conn.execute(
                    "UPDATE mart_version SET status = ? WHERE table_id = ? AND version = ?",
                    ("当前" if name == version else "历史", table_id, name),
                )
            conn.execute(
                "UPDATE mart_version SET status = '当前', published_at = ? WHERE table_id = ? AND version = ?",
                (utc_now(), table_id, version),
            )
            conn.execute(
                "UPDATE mart_table SET current_version = ?, updated_at = ? WHERE table_id = ?",
                (version, utc_now(), table_id),
            )
            for partition in self.repo.list_partitions(table_id):
                if partition["version"] == version:
                    conn.execute(
                        "UPDATE mart_partition SET status = '已发布' WHERE id = ?",
                        (int(partition["id"]),),
                    )
                elif partition.get("status") == "已发布":
                    conn.execute(
                        "UPDATE mart_partition SET status = '历史' WHERE id = ?",
                        (int(partition["id"]),),
                    )
        self._sync_store_rows()
        view = self.repo.get_table(table_id)
        assert view is not None
        label = "旧版日期目录" if version == LEGACY_VERSION else "版本化分区"
        return self._table_view(view), (
            f"集市表已从「{previous}」切换到「{version}」（{label}），看板只读取该版本分区。"
            f"{('备注：' + remark) if remark else ''}"
        )

    # ------------------------------------------------------------------ 分区
    def list_partitions(self, table_id: int, version: str | None = None) -> list[dict[str, Any]] | None:
        if self.repo.get_table(table_id) is None:
            return None
        rows = self.repo.list_partitions(table_id, version=version)
        for row in rows:
            row["visible"] = row.get("status") in VISIBLE_PARTITION_STATUSES
            row["legacy"] = row.get("format") == LEGACY_FORMAT
        return rows

    def list_facts(
        self, table_id: int, version: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]] | None:
        if self.repo.get_table(table_id) is None:
            return None
        rows = self.repo.list_facts(table_id, version=version)
        for row in rows:
            row["dims"] = _json_load(row.get("dims"), {})
            row["measures"] = _json_load(row.get("measures"), {})
        return rows[:limit]

    def visible_view(self, table_id: int) -> dict[str, Any] | None:
        """看板读取口径：按可见分区逐日取事实，同一日期只命中一个版本。"""
        table = self.repo.get_table(table_id)
        if table is None:
            return None
        partitions = [
            partition for partition in self.repo.list_partitions(table_id)
            if partition.get("status") in VISIBLE_PARTITION_STATUSES
        ]
        facts: list[dict[str, Any]] = []
        for partition in partitions:
            rows = self.repo.read(
                """
                SELECT * FROM mart_fact
                WHERE table_id = ? AND version = ? AND partition_date = ? AND status = '有效'
                ORDER BY grain_key, id
                """,
                (table_id, partition["version"], partition["partition_date"]),
            )
            facts.extend(rows)
        return {
            "table_id": table_id,
            "table_code": table["table_code"],
            "current_version": table["current_version"],
            "visible_versions": sorted({partition["version"] for partition in partitions}),
            # 原位迁移中途会同时可见旧目录与已回填分区；同一日期只命中一个版本。
            "mixed_versions": len({partition["version"] for partition in partitions}) > 1,
            "partition_count": len(partitions),
            "fact_count": len(facts),
            "partitions": partitions,
            "facts": [
                {**fact, "dims": _json_load(fact.get("dims"), {}),
                 "measures": _json_load(fact.get("measures"), {})}
                for fact in facts
            ],
        }

    # ------------------------------------------------------------------ 迁移
    def preview_migration(
        self,
        table_id: int,
        *,
        mode: str = "影子分区切换",
        source: str = "旧版集市分区",
        source_version: str | None = None,
        target_version: str | None = None,
        batch_size: int = 2,
        requested_by: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        table = self.repo.get_table(table_id)
        if table is None:
            return None, f"集市表 {table_id} 不存在"
        if mode not in MIGRATION_MODES:
            return None, f"迁移模式「{mode}」不受支持，可选：{'、'.join(MIGRATION_MODES)}"
        if source not in MIGRATION_SOURCES:
            return None, f"迁移来源「{source}」不受支持，可选：{'、'.join(MIGRATION_SOURCES)}"
        if table["current_version"] != LEGACY_VERSION:
            return None, f"集市表当前已是版本化分区版本「{table['current_version']}」，无需再次迁移"
        with self.repo.locked() as conn:
            existing = self.repo.find_open_migration(table_id)
            if existing is not None:
                return existing, f"集市表已有进行中的迁移记录 #{existing['id']}，沿用原批次"
            versions = {row["version"] for row in self.repo.list_versions(table_id)}
            # 版本边界：历史迁移批次占用的目标版本也不复用，回滚过也不能原地重来。
            used_versions = {row["target_version"] for row in self.repo.list_migrations(table_id=table_id)}
            source_version = source_version or LEGACY_VERSION
            if source_version != LEGACY_VERSION:
                return None, "存量集市表的事实数据入口只有旧版日期目录分区"
            desired = (target_version or self.repo.next_version_name(table_id)).strip()
            version_conflict = False
            if desired == LEGACY_VERSION:
                version_conflict = True
                desired = self.repo.next_version_name(table_id)
            if desired in versions or desired in used_versions:
                version_conflict = True
                seq = max([
                    int(table["version_seq"]),
                    *[self._version_seq(name) for name in used_versions],
                ])
                while True:
                    seq += 1
                    desired = f"v{seq}"
                    if desired not in versions and desired not in used_versions:
                        break
            source_partitions = self.repo.list_partitions(table_id, version=source_version)
            if not source_partitions:
                return None, f"来源版本「{source_version}」没有可迁移的日期目录分区"
            source_facts = self.repo.list_facts(table_id, version=source_version, status="有效")
            signature = fact_signature(source_facts)
            batches = self._build_batches(source_partitions, max(1, int(batch_size or 1)))
            batch_no = max(
                [int(row.get("batch_no") or 0) for row in self.repo.list_migrations(table_id=table_id)] + [0]
            ) + 1
            fields = {
                "table_id": table_id,
                "source_version": source_version,
                "target_version": desired,
                "mode": mode,
                "status": "预览",
                "batch_no": batch_no,
                "total_partitions": len(source_partitions),
                "migrated_partitions": 0,
                "total_facts": len(source_facts),
                "migrated_facts": 0,
                "grain_signature": signature["grain_signature"],
                "dim_signature": signature["dim_signature"],
                "measure_signature": signature["measure_signature"],
                "source_signature": signature["signature"],
                "plan": batches,
                "checkpoint": self._new_checkpoint(mode, desired, source_version),
                "requested_by": requested_by,
                "source": source,
                "rollback_scope": MIGRATION_MODES[mode]["rollback_scope"],
                "confirm_token": _signature(
                    {"table_id": table_id, "target": desired, "batch": batch_no,
                     "source": signature["signature"]}
                ),
            }
            migration_id = self.repo.insert_migration(conn, fields)
        record = self.get_migration(migration_id)
        assert record is not None
        record["version_conflict"] = version_conflict
        record["version_boundary"] = {
            "source_version": source_version,
            "target_version": desired,
            "batch_no": batch_no,
            "reserved": version_conflict,
        }
        # 预览结果只在本次响应里带上解析出的源指纹，便于页面直接比较。
        record["preview"]["source_signature"] = signature
        return record, (
            f"迁移批次已预览：{len(batches)} 批 / {len(source_partitions)} 个分区"
            f" / {len(source_facts)} 行事实；目标版本「{desired}」"
        )

    @staticmethod
    def _build_batches(partitions: list[dict[str, Any]], batch_size: int) -> list[dict[str, Any]]:
        batches: list[dict[str, Any]] = []
        for index in range(0, len(partitions), batch_size):
            chunk = partitions[index:index + batch_size]
            batches.append({
                "batch": len(batches) + 1,
                "dates": [part["partition_date"] for part in chunk],
                "partition_ids": [int(part["id"]) for part in chunk],
                "partition_count": len(chunk),
                "row_count": sum(int(part.get("row_count") or 0) for part in chunk),
                "format": VERSIONED_FORMAT,
                "status": "待迁移",
            })
        return batches

    @staticmethod
    def _new_checkpoint(mode: str, target_version: str, source_version: str) -> dict[str, Any]:
        return {
            "completed_batches": [],
            "current_batch": 0,
            "in_place": [],
            "mode": mode,
            "target_version": target_version,
            "source_version": source_version,
        }

    def get_migration(self, migration_id: int) -> dict[str, Any] | None:
        record = self.repo.get_migration(migration_id)
        if record is None:
            return None
        record["checkpoint"] = self._checkpoint_raw(migration_id)
        record["source_options"] = list(MIGRATION_SOURCES)
        record["mode_options"] = list(MIGRATION_MODES)
        record["table"] = self.repo.get_table(int(record["table_id"]))
        record["reconcile"] = self._persisted_reconcile(
            int(record["table_id"]), record["target_version"])
        if not record.get("preview"):
            record["preview"] = {
                "mode": record.get("mode"),
                "mode_description": MIGRATION_MODES.get(record.get("mode"), {}).get("description", ""),
                "visible_during": MIGRATION_MODES.get(record.get("mode"), {}).get("visible_during", ""),
                "rollback_scope": record.get("rollback_scope") or MIGRATION_MODES.get(
                    record.get("mode"), {}).get("rollback_scope", ""),
                "source": record.get("source"),
                "source_options": list(MIGRATION_SOURCES),
                "batches": record.get("plan") or [],
            }
        return record

    def _checkpoint_raw(self, migration_id: int) -> dict[str, Any]:
        raw = self.repo.get_migration_raw(migration_id)
        if raw is None:
            return {}
        return _json_load(raw.get("checkpoint"), {}) or {}

    def list_migrations(
        self,
        table_id: int | None = None,
        version: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        records = self.repo.list_migrations(table_id=table_id, version=version, status=status)
        for record in records:
            record["rollback_scope"] = record.get("rollback_scope") or "目标版本分区"
        return records

    def find_migration_by_version(
        self, version: str, table_id: int | None = None
    ) -> dict[str, Any] | None:
        """跨模块按版本回查：看板、导出只需给出版本名。"""
        records = self.repo.list_migrations(table_id=table_id, version=version)
        if not records:
            return None
        return self.get_migration(int(records[-1]["id"]))

    def execute_migration(
        self,
        migration_id: int,
        *,
        confirm_token: str = "",
        max_batches: int | None = None,
        fail_at_batch: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """开始或继续迁移；并发提交只会有一个请求推进状态。"""
        record = self.repo.get_migration_raw(migration_id)
        if record is None:
            return None, f"迁移记录 {migration_id} 不存在"
        table = self.repo.get_table(int(record["table_id"]))
        if table is None:
            return None, f"集市表 {record['table_id']} 不存在"
        with self.repo.locked() as conn:
            current = self.repo.get_migration_raw(migration_id)
            assert current is not None
            if current["status"] == "执行中":
                return self.repo.get_migration(migration_id), "迁移正在执行，本次提交视为重复确认"
            if current["status"] not in ("预览", "已中断"):
                return None, f"迁移记录状态为「{current['status']}」，不能重复执行"
            if current.get("confirm_token") and confirm_token and confirm_token != current["confirm_token"]:
                return None, "确认令牌与迁移批次不一致，已拒绝本次提交"
            open_records = [
                row for row in self.repo.list_migrations(table_id=int(record["table_id"]))
                if row["status"] in ("预览", "执行中", "已中断") and int(row["id"]) != migration_id
            ]
            if open_records:
                return None, f"迁移记录 #{open_records[0]['id']} 尚未结束，请按版本次序先处理"
            cursor = conn.execute(
                """
                UPDATE mart_migration SET status = '执行中',
                    started_at = CASE WHEN started_at = '' THEN ? ELSE started_at END,
                    error = ''
                WHERE id = ? AND status IN ('预览', '已中断')
                """,
                (utc_now(), migration_id),
            )
            if cursor.rowcount != 1:
                return None, "迁移已被另一个请求确认，本次未能生效"
            checkpoint = _json_load(current.get("checkpoint"), {}) or self._new_checkpoint(
                current["mode"], current["target_version"], current["source_version"])
            target_version = current["target_version"]
            source_version = current["source_version"]
            plan = _json_load(current.get("plan"), []) or []
            completed = list(checkpoint.get("completed_batches") or [])
            remaining = [batch for batch in plan if batch["batch"] not in completed]
            limit = len(remaining) if max_batches is None else max(0, int(max_batches))
            stop_batch = int(fail_at_batch) if fail_at_batch else None
            migrated_partitions = int(current["migrated_partitions"])
            migrated_facts = int(current["migrated_facts"])
            interrupted = False
            error = ""
            for batch in remaining[:limit]:
                if stop_batch is not None and batch["batch"] >= stop_batch:
                    interrupted = True
                    error = f"模拟在第 {batch['batch']} 批中断，可从断点续跑"
                    checkpoint["current_batch"] = batch["batch"]
                    break
                try:
                    partition_count, fact_count = self._migrate_batch(
                        conn, migration_id, int(record["table_id"]), batch,
                        source_version, target_version, current["mode"], checkpoint,
                    )
                except Exception as exc:  # noqa: BLE001 记录中断原因，方便回滚/续跑
                    interrupted = True
                    error = str(exc)
                    checkpoint["current_batch"] = batch["batch"]
                    break
                migrated_partitions += partition_count
                migrated_facts += fact_count
                completed.append(batch["batch"])
                batch["status"] = "已迁移"
                checkpoint["completed_batches"] = completed
                checkpoint["current_batch"] = batch["batch"]
                self.repo.update_migration(
                    conn, migration_id,
                    migrated_partitions=migrated_partitions,
                    migrated_facts=migrated_facts,
                    plan=plan,
                    checkpoint=checkpoint,
                )
            all_done = len(completed) >= len(plan) and not interrupted
            if all_done:
                self._finalize_migration(
                    conn, migration_id, int(record["table_id"]), current["mode"],
                    source_version, target_version, plan, checkpoint,
                )
                message = (
                    f"迁移完成并已同步切换集市表版本到「{target_version}」，"
                    f"共 {sum(int(batch.get('partition_count') or 0) for batch in plan)} 个分区/"
                    f"{self._sum_facts(int(record['table_id']), target_version)} 行事实"
                )
            else:
                remaining_count = len(plan) - len(completed)
                self.repo.update_migration(
                    conn, migration_id,
                    status="已中断",
                    migrated_partitions=migrated_partitions,
                    migrated_facts=migrated_facts,
                    plan=plan,
                    checkpoint=checkpoint,
                    error=error or "迁移未完成，可从断点续跑",
                )
                message = (
                    f"迁移已中断：完成 {len(completed)} 批，剩余 {remaining_count} 批，"
                    f"可从断点续跑或回滚"
                )
                self._sync_store_rows()
                return self.repo.get_migration(migration_id), message
        self._sync_store_rows()
        return self.repo.get_migration(migration_id), message

    def _migrate_batch(
        self,
        conn: Any,
        migration_id: int,
        table_id: int,
        batch: dict[str, Any],
        source_version: str,
        target_version: str,
        mode: str,
        checkpoint: dict[str, Any],
    ) -> tuple[int, int]:
        migrated_partitions = 0
        migrated_facts = 0
        table = self.repo.get_table(table_id)
        assert table is not None
        for partition_id in batch.get("partition_ids") or []:
            rows = self.repo.read("SELECT * FROM mart_partition WHERE id = ?", (partition_id,))
            if not rows:
                continue
            partition = rows[0]
            partition_date = partition["partition_date"]
            target_path = versioned_partition_path(
                table["table_code"], target_version, partition_date)
            if mode == "影子分区切换":
                # 影子回填：分区先落库但不进入可见集合，切换版本指针时一起转正。
                conn.execute(
                    """
                    INSERT INTO mart_partition(
                        table_id, partition_date, version, format, path, row_count,
                        status, migrated_from, migration_id)
                    VALUES (?, ?, ?, ?, ?, ?, '影子回填', ?, ?)
                    ON CONFLICT(table_id, version, partition_date) DO UPDATE SET
                        format = excluded.format, path = excluded.path,
                        row_count = excluded.row_count, status = '影子回填',
                        migrated_from = excluded.migrated_from,
                        migration_id = excluded.migration_id
                    """,
                    (table_id, partition_date, target_version, VERSIONED_FORMAT, target_path,
                     int(partition.get("row_count") or 0), source_version, migration_id),
                )
            else:
                # 原位批量迁移：直接改写原分区行，另存批次快照用于按批回滚。
                checkpoint.setdefault("in_place", []).append({
                    "partition_id": int(partition["id"]),
                    "partition_date": partition_date,
                    "version": partition["version"],
                    "format": partition["format"],
                    "path": partition["path"],
                    "status": partition["status"],
                    "migration_id": partition.get("migration_id"),
                })
                conn.execute(
                    """
                    UPDATE mart_partition
                    SET version = ?, format = ?, path = ?, status = '已回填',
                        migrated_from = ?, migration_id = ?
                    WHERE id = ?
                    """,
                    (target_version, VERSIONED_FORMAT, target_path, source_version,
                     migration_id, int(partition["id"])),
                )
            # 两种模式都从来源分区复制事实行：粒度、维度、度量原样带入目标版本。
            source_facts = self.repo.read(
                "SELECT * FROM mart_fact WHERE table_id = ? AND version = ? AND partition_date = ?",
                (table_id, source_version, partition_date),
            )
            for fact in source_facts:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO mart_fact(
                        table_id, partition_date, version, grain_key, dims, measures,
                        status, source_path)
                    VALUES (?, ?, ?, ?, ?, ?, '有效', ?)
                    """,
                    (table_id, fact["partition_date"], target_version, fact["grain_key"],
                     fact["dims"], fact["measures"], target_path),
                )
                if cursor.rowcount:
                    migrated_facts += 1
            migrated_partitions += 1
        return migrated_partitions, migrated_facts

    def _finalize_migration(
        self,
        conn: Any,
        migration_id: int,
        table_id: int,
        mode: str,
        source_version: str,
        target_version: str,
        plan: list[dict[str, Any]],
        checkpoint: dict[str, Any],
    ) -> None:
        conn.execute(
            """
            INSERT INTO mart_version(table_id, version, format, status, published_at)
            VALUES (?, ?, ?, '当前', ?)
            ON CONFLICT(table_id, version) DO UPDATE SET
                format = excluded.format, status = '当前', published_at = excluded.published_at
            """,
            (table_id, target_version, VERSIONED_FORMAT, utc_now()),
        )
        conn.execute(
            "UPDATE mart_version SET status = '历史' WHERE table_id = ? AND version = ?",
            (table_id, source_version),
        )
        if mode == "影子分区切换":
            # 影子分区转正、旧目录归档，与版本指针在同一事务里，避免看板混读。
            conn.execute(
                "UPDATE mart_partition SET status = '已发布' WHERE table_id = ? AND version = ?",
                (table_id, target_version),
            )
            conn.execute(
                "UPDATE mart_partition SET status = '历史' WHERE table_id = ? AND version = ? AND status = '已发布'",
                (table_id, source_version),
            )
        else:
            conn.execute(
                "UPDATE mart_partition SET status = '已发布' WHERE table_id = ? AND version = ?",
                (table_id, target_version),
            )
        conn.execute(
            "UPDATE mart_table SET current_version = ?, version_seq = ?, updated_at = ? WHERE table_id = ?",
            (target_version, self._version_seq(target_version), utc_now(), table_id),
        )
        self.repo.update_migration(
            conn, migration_id,
            status="已执行",
            migrated_partitions=sum(int(batch.get("partition_count") or 0) for batch in plan),
            migrated_facts=self._sum_facts(table_id, target_version),
            plan=[{**batch, "status": "已迁移"} for batch in plan],
            checkpoint=checkpoint,
            finished_at=utc_now(),
            error="",
        )

    @staticmethod
    def _version_seq(version: str) -> int:
        try:
            return int(str(version).lstrip("vV"))
        except ValueError:
            return 0

    def _sum_facts(self, table_id: int, version: str) -> int:
        rows = self.repo.read(
            "SELECT COUNT(*) AS c FROM mart_fact WHERE table_id = ? AND version = ? AND status = '有效'",
            (table_id, version),
        )
        return int(rows[0]["c"]) if rows else 0

    def rollback_migration(self, migration_id: int) -> tuple[dict[str, Any] | None, str]:
        """按迁移模式回滚：影子切换撤掉整个目标版本，原位迁移只退已迁移批次。"""
        record = self.repo.get_migration_raw(migration_id)
        if record is None:
            return None, f"迁移记录 {migration_id} 不存在"
        table_id = int(record["table_id"])
        with self.repo.locked() as conn:
            current = self.repo.get_migration_raw(migration_id)
            assert current is not None
            if current["status"] == "预览":
                # 只预览未执行：取消批次，不动任何分区，释放该表的迁移锁。
                self.repo.update_migration(
                    conn, migration_id, status="已取消", rolled_back_at=utc_now(),
                    plan=self._reset_plan_status(_json_load(current.get("plan"), []) or []),
                    error="",
                )
                self._sync_store_rows()
                return self.repo.get_migration(migration_id), "迁移批次已取消（未执行，分区未变更）"
            if current["status"] not in ("已执行", "已中断", "执行中"):
                return None, f"迁移记录状态为「{current['status']}」，不能回滚"
            checkpoint = _json_load(current.get("checkpoint"), {}) or {}
            target_version = current["target_version"]
            source_version = current["source_version"]
            mode = current["mode"]
            table = self.repo.get_table(table_id)
            assert table is not None
            restored = 0
            if mode == "影子分区切换":
                target_partitions = self.repo.list_partitions(table_id, version=target_version)
                conn.execute(
                    "DELETE FROM mart_partition WHERE table_id = ? AND version = ?",
                    (table_id, target_version),
                )
                conn.execute(
                    "DELETE FROM mart_fact WHERE table_id = ? AND version = ?",
                    (table_id, target_version),
                )
                restored = len(target_partitions)
                conn.execute(
                    "UPDATE mart_version SET status = '已回滚' WHERE table_id = ? AND version = ?",
                    (table_id, target_version),
                )
                if table["current_version"] == target_version:
                    conn.execute(
                        "UPDATE mart_version SET status = '当前' WHERE table_id = ? AND version = ?",
                        (table_id, source_version),
                    )
                    conn.execute(
                        "UPDATE mart_table SET current_version = ?, updated_at = ? WHERE table_id = ?",
                        (source_version, utc_now(), table_id),
                    )
                # 回退后旧目录必须重新对外可见，否则看板会读到空分区。
                conn.execute(
                    "UPDATE mart_partition SET status = '已发布' "
                    "WHERE table_id = ? AND version = ? AND status = '历史'",
                    (table_id, source_version),
                )
            else:
                # 原位：只回退 checkpoint 记录下来的分区，未迁移部分保持原样。
                for saved in reversed(checkpoint.get("in_place") or []):
                    conn.execute(
                        """
                        UPDATE mart_partition
                        SET version = ?, format = ?, path = ?, status = ?,
                            migrated_from = NULL, migration_id = ?
                        WHERE id = ?
                        """,
                        (saved["version"], saved["format"], saved["path"], saved["status"],
                         saved.get("migration_id"), int(saved["partition_id"])),
                    )
                    conn.execute(
                        "DELETE FROM mart_fact WHERE table_id = ? AND version = ? AND partition_date = ?",
                        (table_id, target_version, saved["partition_date"]),
                    )
                    restored += 1
                checkpoint["in_place"] = []
                conn.execute(
                    "UPDATE mart_version SET status = '已回滚' WHERE table_id = ? AND version = ?",
                    (table_id, target_version),
                )
                if table["current_version"] == target_version:
                    conn.execute(
                        "UPDATE mart_table SET current_version = ?, updated_at = ? WHERE table_id = ?",
                        (source_version, utc_now(), table_id),
                    )
                    conn.execute(
                        "UPDATE mart_version SET status = '当前' WHERE table_id = ? AND version = ?",
                        (table_id, source_version),
                    )
                remaining = conn.execute(
                    "SELECT COUNT(*) AS c FROM mart_partition WHERE table_id = ? AND version = ?",
                    (table_id, target_version),
                ).fetchone()["c"]
                if not remaining:
                    conn.execute(
                        "DELETE FROM mart_version WHERE table_id = ? AND version = ?",
                        (table_id, target_version),
                    )
                # 原位回退后旧目录重新可见，看板不会同时读到新旧分区。
                conn.execute(
                    "UPDATE mart_partition SET status = '已发布' "
                    "WHERE table_id = ? AND version = ? AND status = '历史'",
                    (table_id, source_version),
                )
            self.repo.update_migration(
                conn, migration_id,
                status="已回滚",
                checkpoint=checkpoint,
                rolled_back_at=utc_now(),
                plan=self._reset_plan_status(_json_load(current.get("plan"), []) or []),
                error="",
            )
        self._sync_store_rows()
        scope = MIGRATION_MODES.get(mode, {}).get("rollback_scope", "目标版本分区")
        return self.repo.get_migration(migration_id), (
            f"迁移已回滚，范围：{scope}，恢复 {restored} 个分区"
        )

    @staticmethod
    def _reset_plan_status(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {**batch, "status": "已回滚" if batch.get("status") == "已迁移" else batch.get("status")}
            for batch in plan
        ]

    # ------------------------------------------------------------------ 核对
    def reconcile(
        self, table_id: int, version: str | None = None, *, persist: bool = True
    ) -> tuple[dict[str, Any] | None, str]:
        table = self.repo.get_table(table_id)
        if table is None:
            return None, f"集市表 {table_id} 不存在"
        version = version or table["current_version"]
        records = [
            record for record in self.repo.list_migrations(table_id=table_id, version=version)
            if record["status"] in ("已执行", "已回滚", "已中断")
        ]
        if not records:
            return None, f"版本「{version}」没有可核对的迁移记录"
        record = records[-1]
        source_version = record["source_version"]
        target_version = record["target_version"]
        source_facts = self.repo.list_facts(table_id, version=source_version, status="有效")
        target_facts = self.repo.list_facts(table_id, version=target_version, status="有效")
        source_sig = fact_signature(source_facts)
        target_sig = fact_signature(target_facts)
        grain_ok = source_sig["grain_count"] == target_sig["grain_count"]
        dims_ok = source_sig["dim_signature"] == target_sig["dim_signature"]
        measures_ok = source_sig["measure_signature"] == target_sig["measure_signature"]
        matched = grain_ok and dims_ok and measures_ok
        mismatches = [
            name for name, passed in (
                ("事实粒度", grain_ok), ("关联维度", dims_ok), ("度量结果", measures_ok),
            ) if not passed
        ]
        result = {
            "ok": matched,
            "table_id": table_id,
            "version": version,
            "source_version": source_version,
            "target_version": target_version,
            "migration_id": int(record["id"]),
            "grain_ok": grain_ok,
            "dimensions_ok": dims_ok,
            "measures_ok": measures_ok,
            "source": source_sig,
            "target": target_sig,
            "mismatches": mismatches,
            "checked_at": utc_now(),
        }
        if persist:
            with self.repo.locked() as conn:
                # 同一版本重复核对只保留最近一次结果，按 (table, version) 回查。
                self.repo.update_migration(
                    conn, int(record["id"]),
                    reconciled_at=result["checked_at"],
                    reconcile_status="一致" if matched else "不一致",
                    reconcile_signature=target_sig["signature"],
                )
            result["persisted"] = True
            result["reconcile_status"] = "一致" if matched else "不一致"
        return result, (
            "核对通过：事实粒度、关联维度与度量结果一致" if matched
            else "核对未通过：存在" + "、".join(mismatches) + "差异"
        )

    def _persisted_reconcile(self, table_id: int, version: str) -> dict[str, Any] | None:
        records = self.repo.list_migrations(table_id=table_id, version=version)
        if not records:
            return None
        record = records[-1]
        if not record.get("reconciled_at"):
            return None
        return {
            "ok": record.get("reconcile_status") == "一致",
            "table_id": table_id,
            "version": version,
            "migration_id": int(record["id"]),
            "reconcile_status": record.get("reconcile_status"),
            "reconcile_signature": record.get("reconcile_signature"),
            "reconciled_at": record.get("reconciled_at"),
            "source_version": record.get("source_version"),
            "target_version": record.get("target_version"),
            "persisted": True,
        }

    def get_persisted_reconcile(self, table_id: int, version: str) -> dict[str, Any] | None:
        return self._persisted_reconcile(table_id, version)
