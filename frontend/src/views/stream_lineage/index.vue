<template>
  <section class="page" data-module="stream_lineage">
    <header class="page-head">
      <div><h2>流式血缘</h2><p class="page-desc">维护流式数据从消息到湖表的全链路血缘与延迟追踪；下线转换节点的失效边由血缘边修复记录撤下，查询只保留替代来源。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记血缘记录</button>
        <button class="btn" type="button" data-testid="locate-offline" @click="locateOffline">定位下线节点</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>

    <div class="repair-bar">
      <div class="repair-controls">
        <label class="filter-item"><span>上游主题</span>
          <input v-model="repairForm.topic_code" data-testid="repair-topic" placeholder="如 TOPI-0001" />
        </label>
        <label class="filter-item"><span>目标表</span>
          <input v-model="repairForm.target_table" data-testid="repair-table" placeholder="如 order_agg_hourly" />
        </label>
        <label class="filter-item"><span>撤挂策略</span>
          <select v-model="repairForm.policy" data-testid="repair-policy">
            <option v-for="p in policies" :key="p" :value="p">{{ p }}</option>
          </select>
        </label>
        <label class="filter-item"><span>来源批次</span>
          <input v-model="repairForm.source_batch" data-testid="repair-batch" placeholder="跨模块交接批次" />
        </label>
        <button class="btn primary" type="button" data-testid="generate-repair" :disabled="repairLoading" @click="generateRepairs">
          {{ repairLoading ? '生成中…' : '生成血缘边修复记录' }}
        </button>
      </div>
      <span v-if="repairMessage" class="repair-message" :class="{ 'error-text': !repairOk }" data-testid="repair-message">{{ repairMessage }}</span>
    </div>

    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker>
          <marker id="arrowhead-invalid" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#f87171"/></marker>
        </defs>
        <g v-for="(edge, idx) in graph.edges" :key="`valid-${edge.from}-${edge.to}-${idx}`">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="((nodePos[edge.from]?.x||0)+(nodePos[edge.to]?.x||0))/2" :y="((nodePos[edge.from]?.y||0)+(nodePos[edge.to]?.y||0))/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="(edge, idx) in graph.invalid_edges" :key="`invalid-${edge.invalid_edge_key}-${idx}`">
          <line class="dag-edge dag-edge-invalid" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="((nodePos[edge.from]?.x||0)+(nodePos[edge.to]?.x||0))/2" :y="((nodePos[edge.from]?.y||0)+(nodePos[edge.to]?.y||0)+(nodePos[edge.to]?.y||0))/2 - 6" font-size="10" fill="#b91c1c" text-anchor="middle">失效·{{ edge.invalid_reason }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id, offline: node.status==='已停用'}]"
           :data-testid="`dag-node-${node.id}`" :data-status="node.status"
           @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="(nodePos[node.id]?.x||0) - 60" :y="(nodePos[node.id]?.y||0) - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x||0" :y="(nodePos[node.id]?.y||0) + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>

    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl><dt>节点类型</dt><dd>{{ typeLabel(selectedNode.type) }}</dd></dl>
      <dl><dt>状态</dt><dd>{{ selectedNode.status }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div v-if="selectedNode.type === 'transform'" style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px; flex-wrap:wrap;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>

    <section class="repair-panel" data-testid="repair-panel">
      <h3>血缘边修复记录</h3>
      <p class="page-desc">每条修复项保留失效边快照（旧边版本）与替代边来源；中断批次可续跑补齐，重复失效边键不会重复建项。</p>
      <table class="data-table">
        <thead>
          <tr><th>修复批次</th><th>目标表</th><th>失效边（旧边版本）</th><th>替代来源</th><th>撤挂策略</th><th>数据版本</th><th>来源批次</th><th>状态</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in repairItems" :key="String(item.id)" :data-testid="`repair-row-${item.id}`" :data-status="item.repair_status">
            <td>{{ item.batch_id }}</td>
            <td>{{ item.target_table }}</td>
            <td>
              <div>{{ item.invalid_edge.transform_node }}</div>
              <small class="muted">{{ item.invalid_edge.record_code }} · {{ item.invalid_edge.edge_version }}</small>
            </td>
            <td>
              <div>{{ item.replacement_edge.transform_node }}</div>
              <small class="muted">{{ item.replacement_edge.record_code }} · {{ item.replacement_edge.lineage_version }}</small>
            </td>
            <td>{{ item.policy }}</td>
            <td>{{ item.data_version }}</td>
            <td>{{ item.source_batch ?? '—' }}</td>
            <td><span :class="['status-tag', item.repair_status]">{{ item.repair_status }}</span></td>
            <td>
              <button v-if="item.repair_status !== '已完成'" class="link" type="button"
                      :data-testid="`resume-${item.batch_id}`" @click="resumeRepair(item.batch_id)">续跑补齐</button>
              <span v-else class="muted">已撤下失效边</span>
            </td>
          </tr>
          <tr v-if="!repairItems.length">
            <td colspan="9" class="empty-state">暂无血缘边修复记录，可在上方选择订单主题后生成</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记血缘记录</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot">
      <span>{{ graph.nodes.length }} 个节点 · {{ graph.edges.length }} 条有效边 · {{ graph.invalid_edges.length }} 条失效边</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/stream_lineage'
const columns = ["记录编号", "上游主题", "下游表名", "转换节点", "字段映射", "采集时间", "延迟偏移", "链路版本"]
const actions = ["提交校验", "标记生效", "停用记录"]
const STATUS_COLORS: Record<string,string> = {
  '草稿':'#e2e8f0','待校验':'#fef3c7','已生效':'#d1fae5','已启用':'#d1fae5',
  '已停用':'#fee2e2','已下线':'#fee2e2','已就绪':'#dbeafe','运行中':'#d1fae5','正常':'#d1fae5','未注册':'#f1f5f9',
}
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const typeLabel = (t?: string) => ({ topic: '上游主题', transform: '转换节点', table: '下游表' })[t || ''] || t || '—'
const graph = reactive<{nodes: any[], edges: any[], invalid_edges: any[]}>({ nodes: [], edges: [], invalid_edges: [] })
const nodePos = ref<Record<string,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const policies = ['先撤旧边', '先挂新边']
const repairForm = reactive({ topic_code: 'TOPI-0001', target_table: '', policy: '先撤旧边', source_batch: '' })
const repairItems = ref<any[]>([])
const repairLoading = ref(false)
const repairMessage = ref('')
const repairOk = ref(true)
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)

function layout(nodes: any[]) {
  const w = 800
  const rows: Record<string, any[]> = { topic: [], transform: [], table: [] }
  nodes.forEach(n => { (rows[n.type] || (rows[n.type] = [])).push(n) })
  const h = Math.max(400, Math.max(...Object.values(rows).map(r => r.length)) * 90 + 80)
  svgW.value = w; svgH.value = h
  const pos: Record<string,{x:number;y:number}> = {}
  const colX: Record<string, number> = { topic: 140, transform: 400, table: 660 }
  Object.entries(rows).forEach(([type, list]) => {
    list.forEach((n, i) => { pos[n.id] = { x: colX[type] ?? 120, y: 60 + i * 90 } })
  })
  nodePos.value = pos
}

function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }

function toSvgPoint(event: MouseEvent) {
  const svg = dagRef.value?.querySelector('svg')
  if (!svg) return { x: event.offsetX, y: event.offsetY }
  // 屏幕坐标换算到 viewBox 坐标，避免拖拽时节点跳变、点不动
  const rect = svg.getBoundingClientRect()
  const scaleX = svgW.value / rect.width
  const scaleY = svgH.value / rect.height
  return { x: (event.clientX - rect.left) * scaleX, y: (event.clientY - rect.top) * scaleY }
}

function startDrag(e: MouseEvent, node: any) {
  dragNode = node
  const onMove = (ev: MouseEvent) => {
    if (!dragNode) return
    const p = toSvgPoint(ev)
    nodePos.value[node.id] = { x: p.x, y: p.y }
  }
  const onUp = () => {
    dragNode = null
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', onUp)
  }
  document.addEventListener('mousemove', onMove)
  document.addEventListener('mouseup', onUp)
}

async function reload() {
  try {
    const res = await request(ENDPOINT)
    const data = await res.json()
    graph.nodes = data.nodes || []
    graph.edges = data.edges || []
    graph.invalid_edges = data.invalid_edges || []
    layout(graph.nodes)
    await loadRepairs()
  } catch(e) { errorMessage.value = '加载失败' }
}

async function runAction(action: string, node: any) {
  try {
    await request(`${ENDPOINT}/${node.lineage_id ?? node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) })
    await reload()
  } catch(e) { errorMessage.value = '操作失败' }
}

function locateOffline() {
  const offline = graph.nodes.find(n => n.status === '已停用')
  if (!offline) { errorMessage.value = '当前没有已停用的转换节点'; return }
  selectedNode.value = offline
  errorMessage.value = ''
}

async function loadRepairs() {
  try {
    const res = await request(`${ENDPOINT}/repairs`)
    const data = await res.json()
    repairItems.value = data.items || []
  } catch(e) { repairItems.value = [] }
}

async function generateRepairs() {
  repairLoading.value = true; repairMessage.value = ''
  try {
    const payload: Record<string, string> = { policy: repairForm.policy }
    if (repairForm.topic_code) payload.topic_code = repairForm.topic_code
    if (repairForm.target_table) payload.target_table = repairForm.target_table
    if (repairForm.source_batch) payload.source_batch = repairForm.source_batch
    const res = await request(`${ENDPOINT}/repairs/generate`, { method: 'POST', body: JSON.stringify(payload) })
    const data = await res.json()
    repairOk.value = !!data.ok
    repairMessage.value = data.message
    await reload()
  } catch(e: any) {
    repairOk.value = false
    repairMessage.value = e.message || '生成失败'
  } finally { repairLoading.value = false }
}

async function resumeRepair(batchId: string) {
  try {
    const res = await request(`${ENDPOINT}/repairs/${encodeURIComponent(batchId)}/resume`, { method: 'POST', body: JSON.stringify({}) })
    const data = await res.json()
    repairOk.value = !!data.ok
    repairMessage.value = data.message
    await reload()
  } catch(e: any) {
    repairOk.value = false
    repairMessage.value = e.message || '续跑失败'
  }
}

function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
<style scoped>
.repair-bar { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; background: #fafbfc; }
.repair-controls { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }
.repair-controls .filter-item { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.repair-controls input, .repair-controls select { min-width: 150px; padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; }
.repair-message { display: inline-block; margin-top: 8px; font-size: 12px; }
.dag-edge-invalid { stroke: #f87171; stroke-dasharray: 6 4; marker-end: url(#arrowhead-invalid); }
.dag-node.offline rect { stroke: #dc2626; stroke-width: 2; stroke-dasharray: 5 3; }
.repair-panel { margin-top: 14px; }
.repair-panel h3 { margin: 0 0 6px; font-size: 15px; }
.muted { color: var(--muted); }
.status-tag { padding: 2px 8px; border-radius: 10px; font-size: 12px; }
.status-tag.已完成 { background: #d1fae5; color: #065f46; }
.status-tag.待处理, .status-tag.处理中 { background: #fef3c7; color: #92400e; }
.status-tag.已跳过 { background: #f1f5f9; color: #64748b; }
</style>
