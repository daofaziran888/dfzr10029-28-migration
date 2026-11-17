"""数据集市接口：DAG 图结构、集市表、版本切换、分区迁移与核对。

路由结构（固定顺序，静态路径必须在 /{entry_id} 之前）：
- GET  /api/data_mart                      DAG 节点与边（既有调用链）
- GET  /api/data_mart/export               关系清单 CSV
- GET  /api/data_mart/tables               集市表 + 当前版本 + 进行中的迁移
- GET  /api/data_mart/versions             按表或按版本号跨表回查迁移记录
- GET  /api/data_mart/partitions           分区列表（旧目录 / 版本化）
- GET  /api/data_mart/facts                事实行（含粒度、维度、度量）
- GET  /api/data_mart/visible              看板可见口径
- POST /api/data_mart/migrations           预览迁移批次（版本输入）
- GET  /api/data_mart/migrations           迁移记录列表
- GET  /api/data_mart/migrations/{id}      迁移记录明细
- POST /api/data_mart/migrations/{id}/execute   执行/续跑
- POST /api/data_mart/migrations/{id}/rollback  按模式回滚
- POST /api/data_mart/migrations/{id}/reconcile 核对并持久化
- GET  /api/data_mart/reconcile            按同一版本回查核对结果
- POST /api/data_mart/tables/{id}/publish  切换看板可见版本
- GET/POST /api/data_mart/{entry_id}...    既有单条明细与动作
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    EntryPayload,
    MartMigrationPayload,
    MartPublishPayload,
)
from app.services.data_mart import DataMartService

router = APIRouter(prefix="/api/data_mart", tags=["数据集市"])
service = DataMartService()
LIST_FIELDS = ["表编号", "表名称", "主题域", "事实粒度", "关联维度", "度量字段", "刷新周期", "表状态"]
STATUSES = ["草稿", "已建模", "已发布", "已下线"]
EXPORT_FIELDS = [
    "table_id", "table_code", "table_name", "fact_grain", "dimensions", "measures",
    "version", "partition_format", "partition_date", "partition_path",
    "grain_key", "dims", "measures_json", "fact_status", "reconcile_status", "reconciled_at",
]


def csv_cell(value: Any) -> str:
    """只做公式注入转义，引号交给 csv.writer 统一处理，避免双重转义。"""
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return text


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/export")
def export_entries(
    table_id: int | None = Query(default=None, description="只导出指定集市表"),
    version: str | None = Query(default=None, description="按分区版本导出，默认当前版本"),
) -> StreamingResponse:
    """导出事实粒度、维度、度量与分区版本的明细，便于迁移前后比对。"""
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(EXPORT_FIELDS)
    tables = service.list_tables()
    selected_version = version
    if table_id is not None:
        tables = [table for table in tables if int(table["table_id"]) == table_id]
        if not tables:
            raise HTTPException(status_code=404, detail=f"集市表 {table_id} 不存在")
        if selected_version:
            # 指定单表未建过的版本时明确报错，而不是导出一份空文件。
            if not any(row["version"] == selected_version for row in tables[0]["versions"]):
                raise HTTPException(
                    status_code=404,
                    detail=f"集市表 {table_id} 没有版本「{selected_version}」",
                )
    elif selected_version:
        tables = [
            table for table in tables
            if any(row["version"] == selected_version for row in table["versions"])
        ]
    for table in tables:
        target_version = selected_version or table["current_version"]
        partitions = service.list_partitions(int(table["table_id"]), version=target_version) or []
        facts = service.list_facts(int(table["table_id"]), version=target_version, limit=10_000) or []
        reconcile = service.get_persisted_reconcile(int(table["table_id"]), target_version) or {}
        partition_by_date = {partition["partition_date"]: partition for partition in partitions}
        for fact in facts:
            partition = partition_by_date.get(fact["partition_date"], {})
            writer.writerow([
                table["table_id"], csv_cell(table["table_code"]), csv_cell(table["table_name"]),
                csv_cell(table["fact_grain"]), csv_cell("，".join(table.get("dimensions") or [])),
                csv_cell("，".join(table.get("measures") or [])),
                target_version, csv_cell(table["partition_format"]),
                fact["partition_date"], csv_cell(partition.get("path", "")),
                csv_cell(fact["grain_key"]),
                csv_cell(",".join(f"{k}={v}" for k, v in fact["dims"].items())),
                csv_cell(",".join(f"{k}={v}" for k, v in fact["measures"].items())),
                fact.get("status", ""), reconcile.get("reconcile_status", ""),
                reconcile.get("reconciled_at", ""),
            ])
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=data_mart.csv"})


@router.get("/tables")
def list_tables() -> dict[str, Any]:
    """集市表清单：当前版本、分区格式、版本历史与进行中的迁移批次。"""
    items = service.list_tables()
    return {"items": items, "total": len(items), "page": 1, "size": len(items)}


@router.get("/versions")
def list_versions(
    table_id: int | None = Query(default=None),
    version: str | None = Query(default=None, description="按目标版本跨表回查迁移记录"),
) -> dict[str, Any]:
    """按表或按版本读取迁移记录，供看板/导出等跨领域模块核对版本来源。"""
    if version:
        records = service.list_migrations(table_id=table_id, version=version)
        return {"items": records, "total": len(records), "version": version}
    if table_id is None:
        records = service.list_migrations()
        return {"items": records, "total": len(records)}
    versions = service.list_versions(table_id)
    if versions is None:
        raise HTTPException(status_code=404, detail=f"集市表 {table_id} 不存在")
    records = service.list_migrations(table_id=table_id)
    return {"items": versions, "migrations": records, "total": len(versions)}


@router.get("/partitions")
def list_partitions(
    table_id: int = Query(..., description="集市表编号"),
    version: str | None = Query(default=None),
) -> dict[str, Any]:
    rows = service.list_partitions(table_id, version=version)
    if rows is None:
        raise HTTPException(status_code=404, detail=f"集市表 {table_id} 不存在")
    return {"items": rows, "total": len(rows)}


@router.get("/facts")
def list_facts(
    table_id: int = Query(..., description="集市表编号"),
    version: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=10_000),
) -> dict[str, Any]:
    rows = service.list_facts(table_id, version=version, limit=limit)
    if rows is None:
        raise HTTPException(status_code=404, detail=f"集市表 {table_id} 不存在")
    return {"items": rows, "total": len(rows)}


@router.get("/visible")
def visible_view(table_id: int = Query(..., description="集市表编号")) -> dict[str, Any]:
    """看板读取口径：只返回当前可见版本的分区与事实。"""
    payload = service.visible_view(table_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"集市表 {table_id} 不存在")
    return payload


@router.get("/partitions/{partition_id}")
def get_partition(partition_id: int) -> dict[str, Any]:
    for table in service.list_tables():
        for partition in service.list_partitions(int(table["table_id"])) or []:
            if int(partition["id"]) == partition_id:
                return partition
    raise HTTPException(status_code=404, detail=f"分区 {partition_id} 不存在")


@router.get("/migrations")
def list_migrations(
    table_id: int | None = Query(default=None),
    version: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    records = service.list_migrations(table_id=table_id, version=version, status=status)
    return {"items": records, "total": len(records)}


@router.post("/migrations")
def preview_migration(payload: MartMigrationPayload) -> ActionResult:
    """版本输入 + 解析现有记录得到迁移批次预览，不落地分区。"""
    record, message = service.preview_migration(
        payload.table_id,
        mode=payload.mode,
        source=payload.source,
        source_version=payload.source_version,
        target_version=payload.target_version,
        batch_size=payload.batch_size,
        requested_by=payload.requested_by,
    )
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)


@router.get("/migrations/{migration_id}")
def get_migration(migration_id: int) -> dict[str, Any]:
    record = service.get_migration(migration_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"迁移记录 {migration_id} 不存在")
    return record


@router.post("/migrations/{migration_id}/execute")
def execute_migration(migration_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    body = payload or {}
    record, message = service.execute_migration(
        migration_id,
        confirm_token=str(body.get("confirm_token") or ""),
        max_batches=body.get("max_batches"),
        fail_at_batch=body.get("fail_at_batch"),
    )
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)


@router.post("/migrations/{migration_id}/rollback")
def rollback_migration(migration_id: int) -> ActionResult:
    record, message = service.rollback_migration(migration_id)
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)


@router.post("/migrations/{migration_id}/reconcile")
def reconcile_migration(migration_id: int) -> ActionResult:
    """核对事实粒度/关联维度/度量结果，并把结果持久化在迁移记录上。"""
    record = service.get_migration(migration_id)
    if record is None:
        return ActionResult(ok=False, message=f"迁移记录 {migration_id} 不存在")
    result, message = service.reconcile(int(record["table_id"]), record["target_version"])
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/reconcile")
def get_reconcile(
    table_id: int = Query(..., description="集市表编号"),
    version: str | None = Query(default=None, description="按版本回查，默认当前版本"),
) -> dict[str, Any]:
    table = service.get_table(table_id)
    if table is None:
        raise HTTPException(status_code=404, detail=f"集市表 {table_id} 不存在")
    target_version = version or table["current_version"]
    result = service.get_persisted_reconcile(table_id, target_version)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"集市表 {table_id} 的版本「{target_version}」还没有持久化的核对结果",
        )
    return result


@router.post("/tables/{table_id}/publish")
def publish_version(table_id: int, payload: MartPublishPayload) -> ActionResult:
    table, message = service.publish_version(table_id, payload.version, payload.remark or "")
    if table is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=table)


@router.get("/tables/{table_id}")
def get_table(table_id: int) -> dict[str, Any]:
    table = service.get_table(table_id)
    if table is None:
        raise HTTPException(status_code=404, detail=f"集市表 {table_id} 不存在")
    return table


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"集市表 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="集市表已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
