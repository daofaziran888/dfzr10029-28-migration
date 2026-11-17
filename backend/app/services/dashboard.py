"""数据看板业务规则：状态流转、字段校验与筛选口径都收在这里。

看板读取集市表数据时按集市表的当前版本口径取分区（见 DataMartService.visible_view），
版本切换与分区回填由数据集市模块负责，这里只把可见版本带进看板明细，避免看板
同时命中旧日期目录和新版本分区。
"""
from __future__ import annotations

from typing import Any

from app.services.mart_repository import get_repository
from app.store import store

MODULE = "dashboard"
LIST_FIELDS = ["看板编号", "看板名称", "包含图表", "数据刷新", "可见范围", "创建人", "创建时间", "看板状态"]
REQUIRED_FIELDS = ["看板编号", "看板名称", "包含图表"]
STATUS_ORDER = ["待配置", "已发布", "展示中", "已下线"]
ACTION_RULES = {"完成配置": "已发布", "发布看板": "展示中", "下线看板": "已下线"}
NEGATIVE_ACTIONS = ["下线看板"]


class DashboardService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("看板编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        entry.update(self._mart_read_scope(entry))
        return entry

    @staticmethod
    def _mart_read_scope(entry: dict[str, Any]) -> dict[str, Any]:
        """看板引用的集市表按当前发布版本读取，明细里带上可见版本与分区格式。"""
        try:
            repo = get_repository()
        except Exception:  # noqa: BLE001 集市库不可用不影响看板配置本身
            return {}
        reference = str(entry.get("包含图表") or "")
        table = next(
            (row for row in repo.list_tables()
             if row["table_code"] in reference or row["table_name"] in reference),
            None,
        )
        if table is None:
            return {}
        return {
            "集市表id": int(table["table_id"]),
            "可见分区版本": table["current_version"],
            "分区格式": (
                "版本化分区" if table["current_version"] != table["legacy_version"] else "日期目录"
            ),
        }

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"看板配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据看板可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"看板配置已{action}"
