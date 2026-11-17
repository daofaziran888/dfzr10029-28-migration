"""数据集市示例数据：旧版日期目录分区与存量事实行。

事实行按集市表自己的事实粒度和度量字段生成，迁移前后可以直接比对：
- grain_key 代表事实粒度；
- dims 保留关联维度；
- measures 保留度量结果。

这里的表定义同时用于 SQLite 初始化和新建集市表时的旧版数据准备。
"""
from __future__ import annotations

import itertools
import json
from typing import Any

MART_DATES = ["2026-11-01", "2026-11-02", "2026-11-03"]

MART_TABLE_DEFS: list[dict[str, Any]] = [
    {
        "table_id": 1,
        "table_code": "DATA-0001",
        "table_name": "销售日汇总集市",
        "subject": "销售",
        "fact_grain": "按日",
        "dimensions": ["日期", "区域", "渠道"],
        "measures": ["销售金额", "销售数量", "订单数"],
        "refresh_cycle": "每日",
        "dimension_values": {"区域": ["华东", "华南"], "渠道": ["线上", "门店"]},
        "orders_per_partition": 0,
    },
    {
        "table_id": 2,
        "table_code": "DATA-0002",
        "table_name": "客户订单集市",
        "subject": "客户",
        "fact_grain": "按订单",
        "dimensions": ["日期", "客户等级", "渠道"],
        "measures": ["订单金额", "优惠金额", "订单数"],
        "refresh_cycle": "每小时",
        "dimension_values": {"客户等级": ["钻石", "黄金", "白银"], "渠道": ["线上", "门店"]},
        "orders_per_partition": 3,
    },
    {
        "table_id": 3,
        "table_code": "DATA-0003",
        "table_name": "库存日报集市",
        "subject": "库存",
        "fact_grain": "按日",
        "dimensions": ["日期", "仓库", "品类"],
        "measures": ["库存数量", "周转天数", "缺货次数"],
        "refresh_cycle": "每日",
        "dimension_values": {"仓库": ["北仓", "南仓"], "品类": ["家电", "食品"]},
        "orders_per_partition": 0,
    },
]

LEGACY_FORMAT = "日期目录"
VERSIONED_FORMAT = "版本化分区"
LEGACY_VERSION = "legacy"


def legacy_partition_path(table_code: str, partition_date: str) -> str:
    """旧版按日期目录存放：warehouse/<表编号>/dt=<日期>。"""
    return f"/warehouse/{table_code.lower()}/dt={partition_date}"


def versioned_partition_path(table_code: str, version: str, partition_date: str) -> str:
    """新版版本化分区：warehouse/<表编号>/version=<版本>/dt=<日期>。"""
    return f"/warehouse/{table_code.lower()}/version={version}/dt={partition_date}"


def measure_value(defn: dict[str, Any], date_index: int, combo_index: int, order_index: int, measure_index: int) -> int:
    base = 120 + int(defn["table_id"]) * 10 + date_index * 7 + combo_index * 3 + order_index * 2
    if defn.get("orders_per_partition"):
        if measure_index == 2:
            return 1
        return base * (10 - measure_index * 2)
    return base * (8 - measure_index * 2) + measure_index


def build_legacy_dataset(defn: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按表定义生成旧版日期目录分区和存量事实行。"""
    partitions: list[dict[str, Any]] = []
    facts: list[dict[str, Any]] = []
    dimensions = [dim for dim in defn.get("dimensions", []) if dim != "日期"]
    value_axes = [defn.get("dimension_values", {}).get(dim, ["默认"]) for dim in dimensions]
    combos = list(itertools.product(*value_axes)) if value_axes else [()]
    orders = int(defn.get("orders_per_partition") or 0)
    measures = list(defn.get("measures", ["度量值"]))
    for date_index, partition_date in enumerate(MART_DATES):
        path = legacy_partition_path(str(defn["table_code"]), partition_date)
        row_count = 0
        for combo_index, combo in enumerate(combos):
            dims = {"日期": partition_date}
            dims.update({dim: value for dim, value in zip(dimensions, combo)})
            order_indexes = range(orders) if orders else (0,)
            for order_index in order_indexes:
                grain_parts = [partition_date, *(str(value) for value in combo)]
                if orders:
                    grain_parts.append(f"订单-{order_index + 1:03d}")
                grain_key = "|".join(grain_parts)
                measure_values = {
                    measure: measure_value(defn, date_index, combo_index, order_index, index)
                    for index, measure in enumerate(measures)
                }
                facts.append({
                    "table_id": int(defn["table_id"]),
                    "partition_date": partition_date,
                    "version": LEGACY_VERSION,
                    "grain_key": grain_key,
                    "dims": json.dumps(dims, ensure_ascii=False, sort_keys=True),
                    "measures": json.dumps(measure_values, ensure_ascii=False, sort_keys=True),
                    "status": "有效",
                    "source_path": path,
                })
                row_count += 1
        partitions.append({
            "table_id": int(defn["table_id"]),
            "partition_date": partition_date,
            "version": LEGACY_VERSION,
            "format": LEGACY_FORMAT,
            "path": path,
            "row_count": row_count,
            "status": "已发布",
            "migrated_from": None,
        })
    return partitions, facts


def split_field(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []
    return [item.strip() for item in str(value).replace("，", ",").split(",") if item.strip()]


def make_table_definition(
    *,
    table_id: int,
    table_code: str,
    table_name: str,
    subject: str = "",
    fact_grain: str | None = None,
    dimensions: Any = None,
    measures: Any = None,
    refresh_cycle: str | None = None,
) -> dict[str, Any]:
    """把用户登记的集市表字段转成可生成旧版分区的表定义。"""
    dims = split_field(dimensions) or ["日期"]
    if "日期" not in dims:
        dims = ["日期", *dims]
    measure_fields = split_field(measures) or ["度量值"]
    grain = str(fact_grain or "按日").strip() or "按日"
    return {
        "table_id": int(table_id),
        "table_code": str(table_code or f"DATA-{table_id:04d}"),
        "table_name": str(table_name or f"集市表{table_id}"),
        "subject": str(subject or ""),
        "fact_grain": grain,
        "dimensions": dims,
        "measures": measure_fields,
        "refresh_cycle": str(refresh_cycle or "每日"),
        "dimension_values": {dim: ["默认", "其他"] for dim in dims if dim != "日期"},
        "orders_per_partition": 2 if "单" in grain else 0,
    }
