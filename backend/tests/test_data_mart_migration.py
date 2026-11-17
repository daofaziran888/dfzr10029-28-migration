"""数据集市分区迁移的验收测试：直接用 ASGI 调用，不依赖 httpx。

覆盖：
- 存量集市表在新分区格式下保持事实粒度与度量结果（核对）
- 影子分区切换 / 原位批量迁移的可见版本与回滚范围
- 迁移中断后可续跑、可回滚
- 版本号冲突时保留版本边界
- 并发确认只产生唯一结果
- 核对结果按版本持久化，跨进程重开库仍可回查
- 旧版 v1 库原地升级后历史数据不丢
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any

from app.services.data_mart import DataMartService
from app.services.mart_repository import MartRepository


def call(app: Any, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    """极简 ASGI 调用：避开 Windows/干净环境缺 httpx 的问题。"""
    raw_path, _, query = path.partition("?")
    payload = json.dumps(body).encode() if body is not None else b""
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": method, "scheme": "http", "path": raw_path, "raw_path": raw_path.encode(),
        "query_string": query.encode(), "headers": [(b"content-type", b"application/json")],
        "client": ("127.0.0.1", 12345), "server": ("127.0.0.1", 8000),
        "root_path": "",
    }
    messages = [
        {"type": "http.request", "body": payload, "more_body": False},
    ]
    cursor = 0
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        nonlocal cursor
        if cursor < len(messages):
            message = messages[cursor]
            cursor += 1
            return message
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    asyncio.run(app(scope, receive, send))
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    try:
        return status, json.loads(raw.decode())
    except json.JSONDecodeError:
        return status, raw.decode()


class MartMigrationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "mart.sqlite3")
        self.repo = MartRepository(self.db_path)
        self.svc = DataMartService(self.repo)

    def tearDown(self) -> None:
        self.repo.conn.close()
        self.tmp.cleanup()

    def _run(self, table_id: int, mode: str, **kwargs: Any) -> dict[str, Any]:
        record, _ = self.svc.preview_migration(table_id, mode=mode, **kwargs)
        assert record is not None
        done, _ = self.svc.execute_migration(record["id"])
        assert done is not None
        return done

    def test_shadow_switch_keeps_visibility_and_rollback_scope(self) -> None:
        source_facts = {
            fact["grain_key"]: fact["measures"]
            for fact in self.svc.repo.list_facts(1, version="legacy")
        }
        record, message = self.svc.preview_migration(1, mode="影子分区切换", batch_size=2)
        self.assertIn("已预览", message)
        self.assertEqual(record["status"], "预览")
        self.assertEqual(record["source_version"], "legacy")
        self.assertEqual(record["target_version"], "v1")

        # 仅执行一批：影子分区不可见，看板仍只读旧目录。
        partial, _ = self.svc.execute_migration(record["id"], max_batches=1)
        self.assertEqual(partial["status"], "已中断")
        visible = self.svc.visible_view(1)
        self.assertEqual({p["version"] for p in visible["partitions"]}, {"legacy"})
        self.assertEqual(len(visible["facts"]), len(source_facts))

        done, _ = self.svc.execute_migration(record["id"])
        self.assertEqual(done["status"], "已执行")
        table = self.svc.get_table(1)
        self.assertEqual(table["current_version"], "v1")
        self.assertEqual(table["partition_format"], "版本化分区")
        visible = self.svc.visible_view(1)
        self.assertEqual({p["version"] for p in visible["partitions"]}, {"v1"})

        migrated = {
            fact["grain_key"]: fact["measures"]
            for fact in self.svc.repo.list_facts(1, version="v1")
        }
        self.assertEqual(migrated, source_facts, "迁移后事实粒度与度量结果必须一致")

        self.assertIn("目标版本全量分区", done["rollback_scope"])
        rolled, message = self.svc.rollback_migration(record["id"])
        self.assertIsNotNone(rolled)
        self.assertIn("恢复 3 个分区", message)
        self.assertEqual(self.svc.get_table(1)["current_version"], "legacy")
        self.assertEqual(self.svc.repo.list_facts(1, version="v1"), [])

    def test_in_place_migration_rolls_back_only_migrated_batches(self) -> None:
        record, _ = self.svc.preview_migration(
            2, mode="原位批量迁移", source="存量事实数据", batch_size=1)
        interrupted, message = self.svc.execute_migration(
            record["id"], max_batches=1, fail_at_batch=2)
        self.assertIn("中断", message)
        self.assertEqual(interrupted["status"], "已中断")
        visible = self.svc.visible_view(2)
        # 已迁移 1 个日期，其余日期仍是旧目录；同一日期只命中一个版本。
        self.assertEqual(len(visible["partitions"]), 3)
        versions = sorted(p["version"] for p in visible["partitions"])
        self.assertEqual(versions, ["legacy", "legacy", "v1"])

        resumed, _ = self.svc.execute_migration(record["id"])
        self.assertEqual(resumed["status"], "已执行")
        self.assertEqual(self.svc.get_table(2)["current_version"], "v1")
        self.assertIn("已迁移批次分区", resumed["rollback_scope"])

        self.svc.rollback_migration(record["id"])
        self.assertEqual(self.svc.get_table(2)["current_version"], "legacy")
        partitions = self.svc.list_partitions(2)
        self.assertEqual({p["version"] for p in partitions}, {"legacy"})
        self.assertEqual(len(self.svc.repo.list_facts(2, version="legacy")), 54)

    def test_reconcile_persists_and_survives_reopen(self) -> None:
        record = self._run(3, "影子分区切换", batch_size=3)
        result, message = self.svc.reconcile(3, "v1")
        self.assertTrue(result["ok"], message)
        self.assertTrue(result["grain_ok"])
        self.assertTrue(result["dimensions_ok"])
        self.assertTrue(result["measures_ok"])

        self.repo.conn.close()
        reopened = MartRepository(self.db_path)
        try:
            svc = DataMartService(reopened)
            persisted = svc.get_persisted_reconcile(3, "v1")
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted["migration_id"], record["id"])
            self.assertEqual(persisted["reconcile_status"], "一致")
            via_version = svc.find_migration_by_version("v1", table_id=3)
            self.assertIsNotNone(via_version)
            self.assertEqual(via_version["reconcile"]["reconcile_status"], "一致")
        finally:
            reopened.conn.close()

    def test_reconcile_detects_measure_drift(self) -> None:
        record = self._run(1, "影子分区切换", batch_size=3)
        with self.repo.locked() as conn:
            conn.execute(
                "UPDATE mart_fact SET measures = ? WHERE table_id = 1 AND version = 'v1' LIMIT 1",
                (json.dumps({"销售金额": -1, "销售数量": -1, "订单数": -1}, ensure_ascii=False),),
            )
        result, message = self.svc.reconcile(1, "v1")
        self.assertFalse(result["ok"])
        self.assertIn("度量结果", message)
        persisted = self.svc.get_persisted_reconcile(1, "v1")
        self.assertEqual(persisted["reconcile_status"], "不一致")
        self.assertEqual(record["target_version"], "v1")

    def test_version_conflict_is_reserved_and_batched_forward(self) -> None:
        first, _ = self.svc.preview_migration(1, target_version="v1")
        self.svc.execute_migration(first["id"])
        # 手工造一张仍是旧目录的探针表，模拟另一处来源再次请求同一版本。
        self.assertEqual(self.svc.get_table(1)["current_version"], "v1")
        second, message = self.svc.preview_migration(1, target_version="v9")
        self.assertIsNone(second)
        self.assertIn("无需再次迁移", message)

        # 新建的表若目标版本被占用，必须顺延并留下版本边界。
        fresh, _ = self.svc.create_entry({
            "表编号": "DATA-0101", "表名称": "版本边界测试", "主题域": "测试",
            "事实粒度": "按日", "关联维度": "日期", "度量字段": "金额",
        })
        assert fresh is not None
        with self.repo.locked() as conn:
            conn.execute(
                "INSERT INTO mart_version(table_id, version, format, status, published_at) "
                "VALUES (?, 'v1', '版本化分区', '历史', '')", (fresh["id"],))
        record, message = self.svc.preview_migration(int(fresh["id"]), target_version="v1")
        assert record is not None
        # 版本边界：v1 已被占用，目标版本顺延为 v2，不复用已发布版本。
        self.assertTrue(record["version_conflict"])
        self.assertEqual(record["target_version"], "v2")
        self.assertEqual(record["version_boundary"]["reserved"], True)

    def test_concurrent_confirmation_yields_single_result(self) -> None:
        record, _ = self.svc.preview_migration(2, mode="影子分区切换", batch_size=1)
        results: list[tuple[str | None, str]] = []

        def worker() -> None:
            current, message = self.svc.execute_migration(record["id"])
            results.append((current["status"] if current else None, message))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        success = [item for item in results if item[0] == "已执行"]
        self.assertEqual(len(success), 1, results)
        self.assertEqual(self.svc.get_migration(record["id"])["status"], "已执行")
        self.assertEqual(self.svc.get_table(2)["current_version"], "v1")

    def test_new_table_starts_legacy_and_can_publish_previous_version(self) -> None:
        table = self.svc.get_table(2)
        migration = self._run(2, "影子分区切换", batch_size=2)
        versioned, _ = self.svc.publish_version(2, "v1", "看板切换")
        self.assertIsNotNone(versioned)
        self.assertEqual(versioned["current_version"], "v1")
        # 回滚后再切回旧目录，看板读取口径同步。
        self.svc.rollback_migration(migration["id"])
        legacy, message = self.svc.publish_version(2, "legacy", "回退看板")
        self.assertIsNotNone(legacy, message)
        self.assertEqual(legacy["current_version"], "legacy")
        self.assertEqual(
            {p["version"] for p in self.svc.visible_view(2)["partitions"]}, {"legacy"}
        )

    def test_cancel_preview_keeps_partitions_untouched(self) -> None:
        record, _ = self.svc.preview_migration(1, batch_size=1)
        assert record is not None
        cancelled, message = self.svc.rollback_migration(record["id"])
        self.assertEqual(cancelled["status"], "已取消")
        self.assertIn("分区未变更", message)
        self.assertEqual(self.svc.get_table(1)["current_version"], "legacy")
        self.assertEqual(len(self.svc.repo.list_facts(1, version="legacy")), 12)
        # 取消后按版本次序重新预览，不复用已占用的版本。
        again, _ = self.svc.preview_migration(1)
        self.assertEqual(again["target_version"], "v2")

    def test_schema_v1_upgrade_preserves_history(self) -> None:
        legacy_path = str(Path(self.tmp.name) / "legacy_v1.sqlite3")
        conn = sqlite3.connect(legacy_path)
        conn.executescript(
            """
            CREATE TABLE mart_table (table_id INTEGER PRIMARY KEY, table_code TEXT NOT NULL UNIQUE,
                table_name TEXT NOT NULL, subject TEXT NOT NULL DEFAULT '',
                legacy_version TEXT NOT NULL DEFAULT 'legacy');
            CREATE TABLE mart_partition (id INTEGER PRIMARY KEY AUTOINCREMENT, table_id INTEGER NOT NULL,
                partition_date TEXT NOT NULL, version TEXT NOT NULL, format TEXT NOT NULL,
                path TEXT NOT NULL, row_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL,
                UNIQUE(table_id, version, partition_date));
            CREATE TABLE mart_fact (id INTEGER PRIMARY KEY AUTOINCREMENT, table_id INTEGER NOT NULL,
                partition_date TEXT NOT NULL, version TEXT NOT NULL, grain_key TEXT NOT NULL,
                dims TEXT NOT NULL, measures TEXT NOT NULL, status TEXT NOT NULL,
                UNIQUE(table_id, version, grain_key));
            CREATE TABLE mart_migration (id INTEGER PRIMARY KEY AUTOINCREMENT, table_id INTEGER NOT NULL,
                source_version TEXT NOT NULL, target_version TEXT NOT NULL, mode TEXT NOT NULL,
                status TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(table_id, target_version));
            CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta VALUES('schema_version','1');
            """
        )
        conn.execute(
            "INSERT INTO mart_table(table_id, table_code, table_name, subject) "
            "VALUES(9, 'DATA-0009', '历史集市', '历史')")
        conn.execute(
            "INSERT INTO mart_partition(table_id, partition_date, version, format, path, row_count, status) "
            "VALUES(9, '2026-01-01', 'legacy', '日期目录', '/warehouse/data-0009/dt=2026-01-01', 2, '已发布')")
        conn.execute(
            "INSERT INTO mart_fact(table_id, partition_date, version, grain_key, dims, measures, status) "
            "VALUES(9, '2026-01-01', 'legacy', 'k1', '{\"日期\":\"2026-01-01\"}', '{\"金额\":10}', '有效')")
        conn.execute(
            "INSERT INTO mart_migration(table_id, source_version, target_version, mode, status, created_at) "
            "VALUES(9, 'legacy', 'v1', '原位批量迁移', '已执行', '2026-01-02')")
        conn.commit()
        conn.close()

        upgraded = MartRepository(legacy_path)
        try:
            self.assertEqual(upgraded.schema_version(), 3)
            tables = upgraded.list_tables()
            self.assertEqual([table["table_code"] for table in tables], ["DATA-0009"])
            facts = upgraded.list_facts(9, version="legacy")
            self.assertEqual(len(facts), 1)
            self.assertEqual(json.loads(facts[0]["measures"])["金额"], 10)
            migrations = upgraded.list_migrations(table_id=9)
            self.assertEqual(migrations[0]["status"], "已执行")
        finally:
            upgraded.conn.close()


class MartApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        import os
        os.environ["MART_DB_PATH"] = str(Path(self.tmp.name) / "api.sqlite3")
        from app.config import Settings, settings
        object.__setattr__(settings, "mart_db_path", os.environ["MART_DB_PATH"])
        from app.services import mart_repository
        mart_repository.reset_repository_for_tests(os.environ["MART_DB_PATH"])
        from app import main as main_module
        from app.routers import data_mart as data_mart_router
        data_mart_router.service = DataMartService(mart_repository.get_repository())
        self.app = main_module.app

    def tearDown(self) -> None:
        from app.services import mart_repository
        if mart_repository._repository is not None:
            mart_repository._repository.conn.close()
        self.tmp.cleanup()

    def test_migration_workflow_over_api(self) -> None:
        status, payload = call(self.app, "POST", "/api/data_mart/migrations", {
            "table_id": 1, "mode": "影子分区切换", "source": "旧版集市分区", "batch_size": 2,
        })
        self.assertEqual(status, 200, payload)
        self.assertTrue(payload["ok"], payload)
        record = payload["entry"]
        migration_id = record["id"]

        status, detail = call(self.app, "GET", f"/api/data_mart/migrations/{migration_id}")
        self.assertEqual(status, 200)
        self.assertEqual(detail["source"], "旧版集市分区")
        self.assertEqual(detail["preview"]["mode"], "影子分区切换")
        self.assertIn("旧版集市分区", detail["source_options"])

        status, payload = call(
            self.app, "POST", f"/api/data_mart/migrations/{migration_id}/execute", {})
        self.assertEqual(status, 200, payload)
        self.assertTrue(payload["ok"], payload)

        status, reconcile = call(
            self.app, "POST", f"/api/data_mart/migrations/{migration_id}/reconcile")
        self.assertEqual(status, 200, reconcile)
        self.assertTrue(reconcile["entry"]["ok"], reconcile)

        status, persisted = call(
            self.app, "GET", "/api/data_mart/reconcile?table_id=1&version=v1")
        self.assertEqual(status, 200, persisted)
        self.assertEqual(persisted["migration_id"], migration_id)
        self.assertEqual(persisted["reconcile_status"], "一致")

        status, versions = call(
            self.app, "GET", "/api/data_mart/versions?table_id=1&version=v1")
        self.assertEqual(status, 200)
        self.assertEqual(versions["total"], 1)

        status, visible = call(self.app, "GET", "/api/data_mart/visible?table_id=1")
        self.assertEqual(status, 200)
        self.assertEqual(visible["current_version"], "v1")
        self.assertEqual({p["version"] for p in visible["partitions"]}, {"v1"})
        self.assertTrue(visible["facts"])
        self.assertIn("dims", visible["facts"][0])
        self.assertIn("measures", visible["facts"][0])

        status, table = call(self.app, "GET", "/api/data_mart/tables/1")
        self.assertEqual(status, 200)
        self.assertEqual(table["current_version"], "v1")
        self.assertEqual(table["partition_format"], "版本化分区")

    def test_preview_rejects_unknown_mode_and_source(self) -> None:
        status, payload = call(self.app, "POST", "/api/data_mart/migrations", {
            "table_id": 1, "mode": "不存在的模式"})
        self.assertEqual(status, 200)
        self.assertFalse(payload["ok"])
        self.assertIn("不受支持", payload["message"])

        status, payload = call(self.app, "POST", "/api/data_mart/migrations", {
            "table_id": 1, "source": "外部数仓"})
        self.assertFalse(payload["ok"])
        self.assertIn("不受支持", payload["message"])

    def test_legacy_endpoints_still_work(self) -> None:
        status, graph = call(self.app, "GET", "/api/data_mart")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(graph["nodes"]), 3)
        node = graph["nodes"][0]
        self.assertIn("分区版本", node)
        self.assertIn("度量字段", node)

        status, action = call(
            self.app, "POST", f"/api/data_mart/{node['id']}/actions", {"action": "提交建模"})
        self.assertEqual(status, 200)
        self.assertTrue(action["ok"], action)
        self.assertEqual(action["entry"]["status"], "已建模")

        status, created = call(self.app, "POST", "/api/data_mart", {
            "values": {"表编号": "DATA-0200", "表名称": "接口新建集市", "主题域": "测试",
                       "事实粒度": "按订单", "关联维度": "日期,渠道", "度量字段": "金额,数量"}})
        self.assertEqual(status, 200)
        self.assertTrue(created["ok"], created)
        self.assertEqual(created["entry"]["分区版本"], "legacy")

        status, graph = call(self.app, "GET", "/api/data_mart")
        self.assertEqual(len(graph["nodes"]), 4)

    def test_overview_module_count_unchanged(self) -> None:
        status, overview = call(self.app, "GET", "/api/overview")
        self.assertEqual(status, 200)
        self.assertEqual(len(overview["modules"]), 18)


if __name__ == "__main__":
    unittest.main()
