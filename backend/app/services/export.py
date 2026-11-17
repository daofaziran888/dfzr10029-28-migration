"""数据导出 时间线服务：生成作业时间序列和示例数据。

导出作业如果指向集市表，会沿用集市表的当前分区版本；作业快照里记录版本与
事实粒度，导出文件因此能对应到确定的集市版本，而不是浮动的当前状态。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.services.mart_repository import get_repository
from app.store import store

MODULE = "export"
REQUIRED_FIELDS = ["任务编号", "导出对象", "导出格式"]
STATUS_ORDER = ["待导出", "导出中", "已完成", "导出失败"]
ACTION_RULES = {"开始导出": "导出中", "重新导出": "待导出", "标记失败": "导出失败"}
NEGATIVE_ACTIONS = []


def _mart_partition_meta(name: str) -> dict[str, Any] | None:
    """按导出对象（表编号/表名称）解析集市分区版本，读不到就返回 None。"""
    try:
        repo = get_repository()
    except Exception:  # noqa: BLE001 集市库不可用时导出仍要能列出作业
        return None
    table = repo.find_table_by_code(name) or next(
        (row for row in repo.list_tables() if row["table_name"] == name), None)
    if table is None:
        return None
    return {
        "mart_table_id": int(table["table_id"]),
        "partition_version": table["current_version"],
        "fact_grain": table["fact_grain"],
        "partition_format": (
            "版本化分区" if table["current_version"] != table["legacy_version"] else "日期目录"
        ),
    }


class ExportService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            item: dict[str, Any] = {
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            }
            partition_meta = _mart_partition_meta(str(row.get("导出对象") or ""))
            if partition_meta:
                item.update(partition_meta)
                item["logs"] += (
                    f"\n[分区版本] {partition_meta['partition_version']}"
                    f"（{partition_meta['partition_format']}，{partition_meta['fact_grain']}）"
                )
            items.append(item)
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        partition_meta = _mart_partition_meta(str(entry.get("导出对象") or ""))
        if partition_meta:
            entry["分区版本"] = partition_meta["partition_version"]
            entry["分区格式"] = partition_meta["partition_format"]
            entry["事实粒度"] = partition_meta["fact_grain"]
            entry["集市表id"] = partition_meta["mart_table_id"]
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        partition_meta = _mart_partition_meta(str(entry.get("导出对象") or ""))
        if partition_meta:
            # 登记时锁定版本，作业重跑也按这个版本导出。
            entry["分区版本"] = partition_meta["partition_version"]
            entry["分区格式"] = partition_meta["partition_format"]
            entry["事实粒度"] = partition_meta["fact_grain"]
            entry["集市表id"] = partition_meta["mart_table_id"]
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"导出任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据导出可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"导出任务已{action}"
