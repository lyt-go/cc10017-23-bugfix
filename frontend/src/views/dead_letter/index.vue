<template>
  <section class="page" data-module="dead_letter">
    <header class="page-head">
      <div>
        <h2>死信处理管理</h2>
        <p class="page-desc">维护反序列化、校验与转换失败的异常消息隔离与回处理，为死信处理提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记死信记录</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出死信处理清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section class="cross-check-panel" data-testid="cross-check-panel">
      <div class="cross-check-head">
        <h3>重放目标核对（血缘边修复记录）</h3>
        <button class="btn" type="button" data-testid="run-cross-check" @click="loadCrossCheck">核对重放目标</button>
      </div>
      <table v-if="crossChecks.length" class="data-table">
        <thead>
          <tr><th>死信记录</th><th>重放目标节点</th><th>来源批次</th><th>目标状态</th><th>修复批次</th><th>替代来源</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in crossChecks" :key="String(item.dead_letter_id)"
              :data-testid="`cross-row-${item.dead_letter_id}`" :data-blocked="String(item.blocked)">
            <td>{{ item.record_code }}</td>
            <td>{{ item.replay_target }}</td>
            <td>{{ item.source_batch ?? '—' }}</td>
            <td>
              <span :class="['status-tag', item.blocked ? 'blocked' : 'valid']">
                {{ item.target_deactivated ? '已停用' : (item.blocked ? '已撤下' : '有效') }}
              </span>
            </td>
            <td>{{ item.repair_batch_id ?? '—' }}</td>
            <td>{{ item.replacement_node ?? '—' }}</td>
            <td>
              <button v-if="item.blocked" class="link" type="button"
                      :data-testid="`redirect-${item.dead_letter_id}`"
                      @click="runRedirect(item.dead_letter_id)">改投重放</button>
              <span v-else class="muted">可正常重放</span>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-else class="page-desc">点击「核对重放目标」读取血缘边修复记录，避免向已停用/已撤下的失效转换节点重放。</p>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无死信处理数据，可先登记死信记录</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记死信记录</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条死信处理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/dead_letter'
const columns = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体", "重试次数", "入库时间", "处置状态"]
const actions = ["加入处置", "标记重放", "标记丢弃"]
const statuses = ["草稿", "待处置", "已重放", "已丢弃"]
const metricNames = ["死信记录总数", "待处置记录", "已重放记录"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
type CrossCheck = {
  dead_letter_id: number
  record_code: string
  replay_target: string
  source_batch: string | null
  target_deactivated: boolean
  blocked: boolean
  repair_batch_id: string | null
  replacement_node: string | null
}
const crossChecks = ref<CrossCheck[]>([])
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'dead_letter.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '死信处理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '死信处理操作失败'
  }
}

async function loadCrossCheck() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/replay-cross-check`)
    if (!response.ok) throw new Error('重放目标核对失败')
    const payload = await response.json()
    crossChecks.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重放目标核对失败'
  }
}

async function runRedirect(deadLetterId: number) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${deadLetterId}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action: '改投重放' }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '改投重放未生效')
    }
    await reload()
    await loadCrossCheck()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '改投重放失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('死信记录列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadCrossCheck()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '死信处理列表读取失败'
  }
}

onMounted(reload)
</script>
<style scoped>
.cross-check-panel { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; background: #fafbfc; }
.cross-check-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
.cross-check-head h3 { margin: 0; font-size: 15px; }
.status-tag { padding: 2px 8px; border-radius: 10px; font-size: 12px; }
.status-tag.blocked { background: #fee2e2; color: #b91c1c; }
.status-tag.valid { background: #d1fae5; color: #065f46; }
.muted { color: var(--muted); font-size: 12px; }
</style>
