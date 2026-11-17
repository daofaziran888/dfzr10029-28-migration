#!/usr/bin/env bash
# 启动后端；数据集市库默认落在 backend/var/data_mart.sqlite3（可用 MART_DB_PATH 覆盖）。
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
.venv/bin/pip install -q -r requirements.txt
# 本地库的父目录：MART_DB_PATH 指向别处时也先建好，避免首次打开失败。
mkdir -p "$(dirname "${MART_DB_PATH:-var/data_mart.sqlite3}")"
exec .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
