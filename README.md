# 数据分析实验与数据应用服务平台

面向指标口径管理、分析实验设计、OLAP查询、数据看板构建与数据API发布的数据应用平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 指标定义 | `metric_def` | 指标 | 指标编号、指标名称、计算公式 |
| 维度管理 | `dimension` | 分析维度 | 维度编号、维度名称、维度类型 |
| 数据集管理 | `dataset` | 分析数据集 | 数据集编号、数据集名称、来源类型 |
| 分析实验 | `experiment` | 实验方案 | 方案编号、实验名称、分析数据集 |
| OLAP查询 | `query_builder` | 查询模板 | 模板编号、模板名称、查询语句 |
| 数据看板 | `dashboard` | 看板配置 | 看板编号、看板名称、包含图表 |
| 图表配置 | `chart` | 图表 | 图表编号、图表名称、图表类型 |
| 数据API发布 | `data_service` | 数据服务 | 服务编号、服务名称、输出数据集 |
| 智能告警 | `alert` | 告警规则 | 规则编号、告警名称、监控指标 |
| 分析用户组 | `user_group` | 用户组 | 组编号、组名称、组描述 |
| 数据快照 | `snapshot` | 快照记录 | 快照编号、数据集名称、快照时间 |
| 任务调度 | `scheduler` | 调度任务 | 任务编号、任务名称、调度类型 |
| 缓存管理 | `cache` | 缓存配置 | 缓存编号、缓存目标、缓存策略 |
| 分析管道 | `pipeline` | 分析节点 | 节点编号、输入数据集、分析算子 |
| 分析模型 | `model` | 模型配置 | 模型编号、模型名称、模型类型 |
| 数据集市 | `data_mart` | 集市表 | 表编号、表名称、主题域 |
| 数据导出 | `export` | 导出任务 | 任务编号、导出对象、导出格式 |
| 数据审计 | `audit` | 审计记录 | 记录编号、操作类型、操作人 |

## 数据集市分区迁移

存量集市表事实数据原来按日期目录存放（`/warehouse/<表编号>/dt=YYYY-MM-DD`），
新查询要求版本化分区（`/warehouse/<表编号>/version=<v>/dt=YYYY-MM-DD`）。
迁移能力在 `backend/app/services/data_mart.py`，持久化在
`backend/app/services/mart_repository.py`（SQLite，默认 `backend/var/data_mart.sqlite3`，
可用 `MART_DB_PATH` 覆盖）。

- **数据入口**：旧版集市分区与存量事实数据；预览迁移时只接受这两个入口。
- **版本输入**：预览批次时传入 `target_version`；被占用的版本不复用，按既有版本次序
  顺延（`version_boundary` 记录来源/目标/批次，冲突时 `reserved=true`）。
- **处理方式**：解析现有分区与事实 → 生成迁移批次 → 回填版本化分区与事实行
  （事实粒度 `grain_key`、关联维度 `dims`、度量 `measures` 原样保留）→ 落成版本化分区集市表。
- **两种模式**的可见版本与回滚范围不同，记录在迁移记录上：
  - `影子分区切换`：回填期间旧目录继续对外服务，完成后一次性切换；回滚撤掉目标版本全量分区。
  - `原位批量迁移`：已迁移批次逐批可见（同一日期只命中一个版本）；回滚只恢复已迁移批次。
- **同步切换**：`mart_table.current_version` 与分区状态在同一事务内更新，看板按
  `/api/data_mart/visible` 读取，不会同时读到旧目录和新分区。
- **中断回滚**：执行可用 `max_batches` / `fail_at_batch` 制造中断，之后按 checkpoint
  续跑或按模式回滚。
- **核对**：`POST /api/data_mart/migrations/{id}/reconcile` 比对源版本与目标版本的
  事实粒度、关联维度、度量结果，并把结论写回迁移记录；`GET /api/data_mart/reconcile`
  按同一版本回查，重启后仍可读到。
- **并发**：同一表同一批次并发提交时，服务层用互斥事务 + 状态守卫保证只有唯一确认结果。

主要接口：

```text
GET  /api/data_mart/tables                         集市表 + 当前版本 + 进行中的迁移
GET  /api/data_mart/versions?table_id=&version=    版本历史 / 按版本跨表回查迁移记录
GET  /api/data_mart/partitions?table_id=&version=  分区列表（旧目录 / 版本化）
GET  /api/data_mart/facts?table_id=&version=       事实行（粒度、维度、度量）
GET  /api/data_mart/visible?table_id=              看板可见口径
POST /api/data_mart/migrations                     预览迁移批次（版本输入）
POST /api/data_mart/migrations/{id}/execute        执行 / 断点续跑
POST /api/data_mart/migrations/{id}/rollback       按模式回滚
POST /api/data_mart/migrations/{id}/reconcile      核对并持久化
GET  /api/data_mart/reconcile?table_id=&version=   按版本回查核对结果
POST /api/data_mart/tables/{id}/publish            切换看板可见版本
GET  /api/data_mart/export?table_id=&version=      导出（保留分区版本与事实粒度）
```

库结构版本为 3；打开 v1/v2 历史库时会按 `schema_meta.schema_version` 逐级补列，
历史分区、事实和迁移记录原样保留。

## 测试

```bash
# 后端：迁移、回滚、核对、并发、旧库升级
cd backend && .venv/bin/python -m unittest discover -s tests -v

# 前端 E2E（需先启动 backend 与 frontend dev server）
cd frontend && npx playwright test
```

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
