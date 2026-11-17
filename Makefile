.PHONY: install backend frontend test

install:
	cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
	cd frontend && npm ci

backend:
	cd backend && ./run.sh

frontend:
	cd frontend && npm run dev

# 数据集市分区迁移与核对的验收测试；使用临时 SQLite，不碰本地库。
test:
	cd backend && .venv/bin/python -m unittest discover -s tests -v
