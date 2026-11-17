"""运行配置：端口、跨域、运行环境与数据集市本地库位置。

数据集市的分区、事实行和迁移记录要能跨进程重启回查，所以单独用 SQLite 落盘。
默认放在 backend/var 下，可用 MART_DB_PATH 覆盖（Docker 里挂卷指向 /srv/app/var）。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _default_mart_db_path() -> str:
    configured = os.getenv("MART_DB_PATH")
    if configured:
        return configured
    return str(Path(__file__).resolve().parent.parent / "var" / "data_mart.sqlite3")


@dataclass(frozen=True)
class Settings:
    app_name: str = "数据分析实验与数据应用服务平台"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    mart_db_path: str = field(default_factory=_default_mart_db_path)
    # 当前仓库结构版本；升级时由 mart_repository 按版本补列/补表，历史库可直接打开。
    mart_schema_version: int = 2


settings = Settings()
