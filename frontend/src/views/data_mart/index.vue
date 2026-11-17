<template>
  <section class="page" data-module="data_mart">
    <header class="page-head">
      <div>
        <h2>数据集市</h2>
        <p class="page-desc">
          维护集市表与分区版本。旧版按日期目录存放，版本化分区迁移后与表版本同步切换，
          看板只读取当前可见版本。
        </p>
      </div>
      <div class="page-actions">
        <label class="filter-item" style="margin-right:6px">
          <span>导出分区版本</span>
          <select v-model="exportVersion" data-testid="export-version">
            <option value="">当前版本</option>
            <option v-for="v in exportVersionOptions" :key="v.version" :value="v.version">{{ v.version }}</option>
          </select>
        </label>
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记集市表</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出事实清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>

    <section class="mart-panel" data-testid="mart-migration-panel">
      <div class="panel-head">
        <h3>集市表与分区版本</h3>
        <span class="panel-hint">迁移批次先预览再执行；中断可续跑或按模式回滚。</span>
      </div>
      <table class="data-table" data-testid="mart-tables">
        <thead>
          <tr><th>表编号</th><th>表名称</th><th>事实粒度</th><th>关联维度</th><th>度量字段</th><th>当前版本</th><th>分区格式</th><th>分区数</th><th>事实行数</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr
            v-for="table in tables"
            :key="table.table_id"
            :data-testid="`mart-table-${table.table_id}`"
            :data-version="table.current_version"
            :data-format="table.partition_format"
          >
            <td>{{ table.table_code }}</td>
            <td>{{ table.table_name }}</td>
            <td>{{ table.fact_grain }}</td>
            <td>{{ (table.dimensions || []).join('，') || '—' }}</td>
            <td>{{ (table.measures || []).join('，') || '—' }}</td>
            <td><strong>{{ table.current_version }}</strong></td>
            <td>{{ table.partition_format }}</td>
            <td>{{ table.partition_count }}</td>
            <td>{{ table.fact_count }}</td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`select-table-${table.table_id}`" @click="selectTable(table.table_id)">选中</button>
              <button class="link" type="button" :data-testid="`open-migration-${table.table_id}`" @click="openMigration(table.table_id)">迁移</button>
            </td>
          </tr>
          <tr v-if="!tables.length"><td colspan="10" class="empty-state">暂无集市表，可先登记</td></tr>
        </tbody>
      </table>

      <div v-if="activeTable" class="migration-form" data-testid="migration-form">
        <h4>
          分区迁移 · {{ activeTable.table_name }}
          <small>（当前 {{ activeTable.current_version }} / {{ activeTable.partition_format }}）</small>
        </h4>
        <div class="filter-bar">
          <label class="filter-item"><span>迁移模式</span>
            <select v-model="form.mode" data-testid="migration-mode">
              <option v-for="(meta, name) in modes" :key="name" :value="name">{{ name }}</option>
            </select>
          </label>
          <label class="filter-item"><span>数据入口</span>
            <select v-model="form.source" data-testid="migration-source">
              <option v-for="source in sources" :key="source" :value="source">{{ source }}</option>
            </select>
          </label>
          <label class="filter-item"><span>目标版本</span>
            <input v-model="form.targetVersion" data-testid="migration-target-version" placeholder="留空按版本次序生成" />
          </label>
          <label class="filter-item"><span>每批分区数</span>
            <input v-model.number="form.batchSize" type="number" min="1" max="10" data-testid="migration-batch-size" />
          </label>
          <button class="btn primary" type="button" data-testid="preview-migration" :disabled="busy" @click="previewMigration">预览迁移批次</button>
        </div>
        <p class="panel-hint" data-testid="migration-mode-hint">
          {{ modes[form.mode]?.description }} 可见范围：{{ modes[form.mode]?.visible_during }}；回滚范围：{{ modes[form.mode]?.rollback_scope }}。
        </p>

        <div v-if="migration" class="migration-card" data-testid="migration-card" :data-status="migration.status">
          <div class="migration-meta">
            <span>迁移记录 #{{ migration.id }}</span>
            <span>状态：<strong data-testid="migration-status">{{ migration.status }}</strong></span>
            <span>版本：{{ migration.source_version }} → <strong data-testid="migration-target">{{ migration.target_version }}</strong></span>
            <span>批次：{{ migration.batch_no }}</span>
            <span>中断/回滚范围：{{ migration.rollback_scope }}</span>
          </div>
          <div class="batch-list" data-testid="migration-batches">
            <div
              v-for="batch in migration.batches || migration.plan || []"
              :key="batch.batch"
              class="batch-chip"
              :class="`batch-${batch.status}`"
              :data-testid="`migration-batch-${batch.batch}`"
              :data-status="batch.status"
            >
              第 {{ batch.batch }} 批 · {{ (batch.dates || []).join('、') }} · {{ batch.partition_count }} 分区 / {{ batch.row_count }} 行
              <em>{{ batch.status }}</em>
            </div>
          </div>
          <div class="page-actions" style="justify-content:flex-start;margin-top:8px">
            <button class="btn primary" type="button" data-testid="execute-migration" :disabled="busy || !canExecute" @click="executeMigration()">
              {{ migration.status === '已中断' ? '断点续跑' : '执行迁移' }}
            </button>
            <button class="btn" type="button" data-testid="execute-one-batch" :disabled="busy || !canExecute" @click="executeMigration(1)">只执行一批</button>
            <button class="btn" type="button" data-testid="rollback-migration" :disabled="busy || !canRollback" @click="rollbackMigration">回滚迁移</button>
            <button class="btn" type="button" data-testid="reconcile-migration" :disabled="busy || !canReconcile" @click="reconcileMigration">核对事实</button>
          </div>
          <div class="publish-line">
            <label class="filter-item"><span>切换发布版本</span>
              <select v-model="publishTarget" data-testid="publish-target">
                <option v-for="v in activeTable?.versions || []" :key="v.version" :value="v.version">
                  {{ v.version }}（{{ v.format }}）
                </option>
              </select>
            </label>
            <button class="btn primary" type="button" data-testid="publish-version" :disabled="busy || !canPublish" @click="publishVersion">切换发布版本</button>
            <span class="panel-hint">切换会同步更新分区可见性与看板读取版本，迁移未结束时不允许切换。</span>
          </div>
          <p v-if="reconcile" class="reconcile-line" :class="reconcile.ok ? 'reconcile-ok' : 'reconcile-bad'" data-testid="reconcile-result">
            核对{{ reconcile.ok ? '通过' : '未通过' }}：事实粒度 {{ reconcile.grain_ok ? '一致' : '不一致' }} ·
            关联维度 {{ reconcile.dimensions_ok ? '一致' : '不一致' }} ·
            度量结果 {{ reconcile.measures_ok ? '一致' : '不一致' }}（版本 {{ reconcile.version }}，已持久化）
          </p>
        </div>

        <div v-if="visible" class="visible-view" data-testid="visible-view">
          <h4>看板可见口径 <small>（当前版本 {{ visible.current_version }}，{{ visible.partition_count }} 个分区 / {{ visible.fact_count }} 行事实）</small></h4>
          <p v-if="visible.mixed_versions" class="panel-hint" data-testid="mixed-versions">
            原位迁移进行中：{{ (visible.visible_versions || []).join(' 与 ') }} 逐日交替可见，
            同一日期只命中一个版本，看板不会重复读。
          </p>
          <table class="data-table">
            <thead><tr><th>分区日期</th><th>分区版本</th><th>分区格式</th><th>路径</th><th>行数</th><th>状态</th></tr></thead>
            <tbody>
              <tr v-for="p in visible.partitions" :key="p.id" :data-testid="`visible-partition-${p.id}`">
                <td>{{ p.partition_date }}</td>
                <td>{{ p.version }}</td>
                <td>{{ p.format }}</td>
                <td style="font-family:monospace">{{ p.path }}</td>
                <td>{{ p.row_count }}</td>
                <td>{{ p.status }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="migration-records" data-testid="migration-records">
          <h4>迁移记录 <small>（按版本回查）</small></h4>
          <table class="data-table">
            <thead><tr><th>#</th><th>版本</th><th>模式</th><th>数据入口</th><th>状态</th><th>分区</th><th>事实</th><th>核对</th><th>回滚范围</th></tr></thead>
            <tbody>
              <tr v-for="record in migrations" :key="record.id" :data-testid="`migration-record-${record.id}`" :data-version="record.target_version">
                <td>{{ record.id }}</td>
                <td>{{ record.source_version }} → {{ record.target_version }}</td>
                <td>{{ record.mode }}</td>
                <td>{{ record.source }}</td>
                <td>{{ record.status }}</td>
                <td>{{ record.migrated_partitions }}/{{ record.total_partitions }}</td>
                <td>{{ record.migrated_facts }}/{{ record.total_facts }}</td>
                <td>{{ record.reconcile_status || '—' }}</td>
                <td>{{ record.rollback_scope }}</td>
              </tr>
              <tr v-if="!migrations.length"><td colspan="9" class="empty-state">暂无迁移记录</td></tr>
            </tbody>
          </table>
        </div>
      </div>
      <p v-else class="panel-hint">选择一张集市表后可预览迁移批次、执行、回滚与核对。</p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记集市表</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <label class="create-field"><span>关联维度（逗号分隔）</span><input v-model="createValues['关联维度']" placeholder="日期,区域" /></label>
        <label class="create-field"><span>度量字段（逗号分隔）</span><input v-model="createValues['度量字段']" placeholder="金额,数量" /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot">
      <span>{{ graph.nodes.length }} 个节点 · {{ tables.length }} 张集市表</span>
      <span v-if="message" data-testid="mart-message">{{ message }}</span>
      <span v-if="errorMessage" class="error-text" data-testid="mart-error">{{ errorMessage }}</span>
    </footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'

type AnyRow = Record<string, any>

const ENDPOINT = '/api/data_mart'
const columns = ["表编号", "表名称", "主题域", "事实粒度", "关联维度", "度量字段", "刷新周期", "表状态"]
const actions = ["提交建模", "发布集市", "下线集市"]
const statuses = ["草稿", "已建模", "已发布", "已下线"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','已建模':'#dbeafe','已发布':'#d1fae5','已下线':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const modes: Record<string, { description: string; visible_during: string; rollback_scope: string }> = {
  '影子分区切换': {
    description: '旧日期目录继续对外服务，新版本分区先写影子目录。',
    visible_during: '仅旧版日期目录',
    rollback_scope: '目标版本全量分区',
  },
  '原位批量迁移': {
    description: '按批次把旧日期目录分区原位改写为版本化分区。',
    visible_during: '新旧分区逐批交替',
    rollback_scope: '已迁移批次分区',
  },
}
const sources = ["旧版集市分区", "存量事实数据"]

const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const message = ref('')
const errorMessage = ref('')
const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const busy = ref(false)

const tables = ref<AnyRow[]>([])
const activeTableId = ref<number | null>(null)
const migration = ref<AnyRow | null>(null)
const migrations = ref<AnyRow[]>([])
const reconcile = ref<AnyRow | null>(null)
const visible = ref<AnyRow | null>(null)
const exportVersion = ref('')
const form = reactive({ mode: '影子分区切换', source: '旧版集市分区', targetVersion: '', batchSize: 2 })
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)

const activeTable = computed(() => tables.value.find((table) => table.table_id === activeTableId.value) || null)
const stats = computed(() => [
  { label: '集市表', value: tables.value.length },
  { label: '版本化分区表', value: tables.value.filter((t) => t.partition_format === '版本化分区').length },
  { label: '进行中迁移', value: tables.value.filter((t) => t.open_migration).length },
  { label: '分区总数', value: tables.value.reduce((sum, t) => sum + (t.partition_count || 0), 0) },
])
const exportVersionOptions = computed(() => {
  const versions = new Set<string>()
  tables.value.forEach((table) => (table.versions || []).forEach((v: AnyRow) => versions.add(v.version)))
  return [...versions].map((version) => ({ version }))
})
const canExecute = computed(() => !!migration.value && ['预览', '已中断'].includes(migration.value.status))
const canRollback = computed(() => !!migration.value && ['已执行', '已中断', '执行中'].includes(migration.value.status))
const canReconcile = computed(() => !!migration.value && ['已执行', '已中断'].includes(migration.value.status))
const publishTarget = ref('')
const canPublish = computed(() => {
  if (!activeTable.value || !publishTarget.value) return false
  if (activeTable.value.current_version === publishTarget.value) return false
  // 迁移进行中不允许切版本，避免分区回填和看板读取错位。
  return !activeTable.value.open_migration
})

function layout(nodes: any[]) {
  svgW.value = 800; svgH.value = Math.max(400, nodes.length * 80)
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) {
  dragNode = node
  const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }
  const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }
  document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp)
}

async function readJson(path: string, init?: RequestInit): Promise<AnyRow> {
  const res = await request(path, init)
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error((data as AnyRow).detail || `接口返回 ${res.status}`)
  return data as AnyRow
}

async function reload() {
  try {
    const [graphData, tableData] = await Promise.all([
      readJson(ENDPOINT), readJson(`${ENDPOINT}/tables`),
    ])
    graph.nodes = graphData.nodes || []
    graph.edges = graphData.edges || []
    layout(graph.nodes)
    tables.value = tableData.items || []
    if (activeTableId.value && !tables.value.some((t) => t.table_id === activeTableId.value)) activeTableId.value = null
  } catch (e: any) {
    errorMessage.value = e.message || '加载失败'
  }
}

async function runAction(action: string, node: any) {
  message.value = ''; errorMessage.value = ''
  try {
    const data = await readJson(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) })
    message.value = data.message || '操作完成'
    await reload()
  } catch (e: any) { errorMessage.value = e.message || '操作失败' }
}

function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  message.value = ''; errorMessage.value = ''
  try {
    const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) })
    const data = await res.json()
    if (!res.ok || !data.ok) throw new Error(data.message || '创建失败')
    showCreate.value = false
    await reload()
  } catch (e: any) { errorMessage.value = e.message }
}

async function exportRows() {
  const params = new URLSearchParams()
  if (activeTableId.value) params.set('table_id', String(activeTableId.value))
  if (exportVersion.value) params.set('version', exportVersion.value)
  try {
    const res = await request(`${ENDPOINT}/export?${params.toString()}`)
    if (!res.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await res.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'data_mart.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (e: any) { errorMessage.value = e.message || '导出失败' }
}

async function selectTable(tableId: number) {
  activeTableId.value = tableId
  migration.value = null; reconcile.value = null
  const table = tables.value.find((t) => t.table_id === tableId)
  publishTarget.value = table?.current_version || ''
  await Promise.all([loadMigrations(), loadVisible()])
}

async function openMigration(tableId: number) {
  await selectTable(tableId)
  const table = tables.value.find((t) => t.table_id === tableId)
  const open = table?.open_migration
  if (open) {
    await loadMigration(open.id)
  }
}

async function loadMigration(migrationId: number) {
  try {
    migration.value = await readJson(`${ENDPOINT}/migrations/${migrationId}`)
    reconcile.value = migration.value.reconcile || null
    if (migration.value.status === '已执行') publishTarget.value = migration.value.target_version
  } catch (e: any) { errorMessage.value = e.message }
}

async function loadMigrations() {
  if (!activeTableId.value) return
  try {
    const data = await readJson(`${ENDPOINT}/migrations?table_id=${activeTableId.value}`)
    migrations.value = data.items || []
    const latest = migrations.value[migrations.value.length - 1]
    if (latest && (!migration.value || migration.value.table_id === activeTableId.value)) {
      await loadMigration(latest.id)
    }
    if (!publishTarget.value) publishTarget.value = activeTable.value?.current_version || ''
  } catch (e: any) { errorMessage.value = e.message }
}

async function loadVisible() {
  if (!activeTableId.value) { visible.value = null; return }
  try { visible.value = await readJson(`${ENDPOINT}/visible?table_id=${activeTableId.value}`) } catch(e) { visible.value = null }
}

async function previewMigration() {
  if (!activeTableId.value) return
  busy.value = true; message.value = ''; errorMessage.value = ''
  try {
    const body: AnyRow = {
      table_id: activeTableId.value,
      mode: form.mode,
      source: form.source,
      batch_size: Number(form.batchSize) || 1,
    }
    if (form.targetVersion.trim()) body.target_version = form.targetVersion.trim()
    const data = await readJson(`${ENDPOINT}/migrations`, { method: 'POST', body: JSON.stringify(body) })
    if (!data.ok) throw new Error(data.message)
    migration.value = data.entry
    message.value = data.message
    await reload()
    await loadMigrations()
  } catch (e: any) { errorMessage.value = e.message || '预览失败' } finally { busy.value = false }
}

async function executeMigration(maxBatches?: number) {
  if (!migration.value) return
  busy.value = true; message.value = ''; errorMessage.value = ''
  try {
    const body: AnyRow = { confirm_token: migration.value.confirm_token || '' }
    if (maxBatches) body.max_batches = maxBatches
    const data = await readJson(`${ENDPOINT}/migrations/${migration.value.id}/execute`, {
      method: 'POST', body: JSON.stringify(body),
    })
    if (!data.ok) throw new Error(data.message)
    message.value = data.message
    await loadMigration(migration.value.id)
    await reload(); await loadVisible(); await loadMigrations()
  } catch (e: any) { errorMessage.value = e.message || '执行失败' } finally { busy.value = false }
}

async function rollbackMigration() {
  if (!migration.value) return
  busy.value = true; message.value = ''; errorMessage.value = ''
  try {
    const data = await readJson(`${ENDPOINT}/migrations/${migration.value.id}/rollback`, { method: 'POST' })
    if (!data.ok) throw new Error(data.message)
    message.value = data.message
    reconcile.value = null
    await loadMigration(migration.value.id)
    await reload(); await loadVisible(); await loadMigrations()
  } catch (e: any) { errorMessage.value = e.message || '回滚失败' } finally { busy.value = false }
}

async function reconcileMigration() {
  if (!migration.value) return
  busy.value = true; message.value = ''; errorMessage.value = ''
  try {
    const data = await readJson(`${ENDPOINT}/migrations/${migration.value.id}/reconcile`, { method: 'POST' })
    if (!data.ok) throw new Error(data.message)
    reconcile.value = data.entry
    message.value = data.message
    await loadMigration(migration.value.id)
    await loadMigrations()
  } catch (e: any) { errorMessage.value = e.message || '核对失败' } finally { busy.value = false }
}

async function publishVersion() {
  if (!publishTarget.value || !activeTableId.value) return
  busy.value = true; message.value = ''; errorMessage.value = ''
  try {
    const data = await readJson(`${ENDPOINT}/tables/${activeTableId.value}/publish`, {
      method: 'POST', body: JSON.stringify({ version: publishTarget.value }),
    })
    if (!data.ok) throw new Error(data.message)
    message.value = data.message
    await reload(); await loadVisible(); await loadMigrations()
  } catch (e: any) { errorMessage.value = e.message || '切换失败' } finally { busy.value = false }
}

onMounted(reload)
</script>

<style scoped>
.mart-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: baseline; }
.panel-head h3 { margin: 0 0 8px; font-size: 15px; }
.panel-hint { color: var(--muted); font-size: 12px; margin: 6px 0; }
.migration-form { margin-top: 14px; border-top: 1px dashed var(--border); padding-top: 12px; }
.migration-form h4 { margin: 0 0 8px; font-size: 14px; }
.migration-form small { color: var(--muted); font-weight: normal; }
.migration-card { border: 1px solid var(--border); border-radius: 8px; padding: 10px; background: #fafbfc; }
.migration-meta { display: flex; flex-wrap: wrap; gap: 14px; font-size: 12px; color: var(--muted); }
.batch-list { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
.batch-chip { border: 1px solid var(--border); border-radius: 6px; padding: 4px 8px; font-size: 12px; background: #fff; }
.batch-chip em { font-style: normal; margin-left: 6px; color: var(--muted); }
.batch-chip.batch-已迁移 { border-color: #86efac; background: #f0fdf4; }
.batch-chip.batch-已回滚 { border-color: #fca5a5; background: #fef2f2; }
.reconcile-line { margin: 10px 0 0; font-size: 13px; }
.reconcile-ok { color: #15803d; }
.reconcile-bad { color: #b42318; }
.visible-view, .migration-records { margin-top: 14px; }
.visible-view h4, .migration-records h4 { margin: 0 0 6px; font-size: 14px; }
.visible-view small, .migration-records small { color: var(--muted); font-weight: normal; }
.publish-line { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; margin-top: 10px; }
.publish-line .panel-hint { margin: 0; }
.migration-form select, .migration-form input { border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; }
</style>
