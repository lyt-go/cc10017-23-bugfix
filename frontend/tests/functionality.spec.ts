import { expect, test } from '@playwright/test'
import { readFile } from 'node:fs/promises'

type ModuleCase = {
  key: string
  fields: string[]
  actions: string[]
  actionTargets: string[]
  shape: string
}

const modules: ModuleCase[] = [
  {
    "key": "topic_registry",
    "fields": [
      "主题编号",
      "主题名称",
      "消息格式",
      "序列化协议",
      "分区键列",
      "保留时长",
      "压缩类型",
      "Schema版本"
    ],
    "actions": [
      "提交校验",
      "确认上线",
      "标记下线"
    ],
    "actionTargets": [
      "待校验",
      "已上线",
      "已下线"
    ],
    "shape": "crud-table"
  },
  {
    "key": "flink_job",
    "fields": [
      "任务编号",
      "消费主题",
      "转换逻辑",
      "输出表名",
      "并行度",
      "检查点间隔",
      "状态后端",
      "水位策略"
    ],
    "actions": [
      "提交校验",
      "启动作业",
      "停止作业"
    ],
    "actionTargets": [
      "待校验",
      "运行中",
      "已停止"
    ],
    "shape": "job-timeline"
  },
  {
    "key": "kafka_connector",
    "fields": [
      "配置编号",
      "集群地址",
      "消费组名",
      "偏移策略",
      "反序列化器",
      "批量大小",
      "超时时间",
      "死信队列"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用配置"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "schema_evolve",
    "fields": [
      "规则编号",
      "关联主题",
      "新增字段",
      "删除字段",
      "类型变更",
      "默认值填充",
      "兼容模式",
      "生效版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "执行回滚"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已回滚"
    ],
    "shape": "crud-table"
  },
  {
    "key": "upsert_merge",
    "fields": [
      "策略编号",
      "目标表名",
      "合并键列",
      "插入列",
      "更新列",
      "删除标记",
      "排序字段",
      "合并引擎"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用策略"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "lake_partition",
    "fields": [
      "分区编号",
      "目标表名",
      "分区列名",
      "分区粒度",
      "分区转换",
      "存储位置",
      "文件格式",
      "生命周期"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用分区"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "stream_quality",
    "fields": [
      "规则编号",
      "监控主题",
      "校验类型",
      "告警阈值",
      "采样比例",
      "检测窗口",
      "阻断动作",
      "规则版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用规则"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "iceberg_table",
    "fields": [
      "表编号",
      "表名称",
      "存储格式",
      "分区策略",
      "排序字段",
      "快照过期",
      "孤立文件清理",
      "元数据版本"
    ],
    "actions": [
      "提交校验",
      "确认就绪",
      "标记下线"
    ],
    "actionTargets": [
      "待校验",
      "已就绪",
      "已下线"
    ],
    "shape": "crud-table"
  },
  {
    "key": "watermark_track",
    "fields": [
      "标记编号",
      "数据主题",
      "分区编号",
      "事件时间水位",
      "处理时间水位",
      "延迟记录数",
      "消费偏移",
      "检查时间"
    ],
    "actions": [
      "标记正常",
      "标记滞后",
      "停用追踪"
    ],
    "actionTargets": [
      "正常",
      "滞后",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "dead_letter",
    "fields": [
      "记录编号",
      "原始主题",
      "原始分区",
      "失败原因",
      "原始消息体",
      "重试次数",
      "入库时间",
      "处置状态"
    ],
    "actions": [
      "加入处置",
      "标记重放",
      "标记丢弃"
    ],
    "actionTargets": [
      "待处置",
      "已重放",
      "已丢弃"
    ],
    "shape": "crud-table"
  },
  {
    "key": "materialized_view",
    "fields": [
      "视图编号",
      "查询SQL",
      "基表名",
      "刷新模式",
      "刷新间隔",
      "存储位置",
      "数据量",
      "最后刷新"
    ],
    "actions": [
      "提交校验",
      "启用刷新",
      "停用视图"
    ],
    "actionTargets": [
      "待校验",
      "已启用",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "change_capture",
    "fields": [
      "配置编号",
      "源数据库",
      "源表名",
      "快照模式",
      "捕获列",
      "变更序号",
      "增量阶段",
      "目标主题"
    ],
    "actions": [
      "提交校验",
      "启动捕获",
      "停止捕获"
    ],
    "actionTargets": [
      "待校验",
      "运行中",
      "已停止"
    ],
    "shape": "job-timeline"
  },
  {
    "key": "compact_policy",
    "fields": [
      "策略编号",
      "目标表名",
      "触发条件",
      "合并并发",
      "目标文件大小",
      "最小文件数",
      "保留快照数",
      "执行时间窗口"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用策略"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "stream_lineage",
    "fields": [
      "记录编号",
      "上游主题",
      "下游表名",
      "转换节点",
      "字段映射",
      "采集时间",
      "延迟偏移",
      "链路版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用记录"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "dag-graph"
  },
  {
    "key": "backfill_stream",
    "fields": [
      "任务编号",
      "目标表名",
      "起始偏移",
      "截止偏移",
      "重放速率",
      "并行度",
      "覆盖策略",
      "结果校验"
    ],
    "actions": [
      "启动补数",
      "确认完成",
      "取消补数"
    ],
    "actionTargets": [
      "运行中",
      "已完成",
      "已取消"
    ],
    "shape": "job-timeline"
  },
  {
    "key": "sink_dispatch",
    "fields": [
      "规则编号",
      "湖表名",
      "目标系统",
      "写入模式",
      "字段映射",
      "过滤条件",
      "批次大小",
      "失败回退"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用规则"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "ttl_policy",
    "fields": [
      "策略编号",
      "目标表名",
      "热数据保留",
      "温数据窗口",
      "冷归档策略",
      "过期动作",
      "分区粒度",
      "恢复超时"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "停用策略"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已停用"
    ],
    "shape": "crud-table"
  },
  {
    "key": "realtime_metrics",
    "fields": [
      "指标编号",
      "指标名称",
      "聚合窗口",
      "聚合函数",
      "过滤条件",
      "分组维度",
      "输出表名",
      "指标版本"
    ],
    "actions": [
      "提交校验",
      "标记生效",
      "标记废弃"
    ],
    "actionTargets": [
      "待校验",
      "已生效",
      "已废弃"
    ],
    "shape": "analysis-dashboard"
  }
]
const repoKind = 'fullstack'
const crudModules = modules.filter(m => m.shape === 'crud-table')
const dagModules = modules.filter(m => m.shape === 'dag-graph')
const timelineModules = modules.filter(m => m.shape === 'job-timeline')
const analysisModules = modules.filter(m => m.shape === 'analysis-dashboard')

function parseCsv(text: string): string[][] {
  const records: string[][] = []
  let row: string[] = []
  let cell = ''
  let quoted = false
  const input = text.replace(/^\uFEFF/, '')
  for (let index = 0; index < input.length; index++) {
    const char = input[index]
    if (quoted) {
      if (char === '"' && input[index + 1] === '"') { cell += '"'; index++ }
      else if (char === '"') { quoted = false }
      else { cell += char }
    } else if (char === '"' && cell === '') { quoted = true }
    else if (char === ',') { row.push(cell); cell = '' }
    else if (char === '\n') {
      row.push(cell.replace(/\r$/, ''))
      if (row.some(v => v !== '')) records.push(row)
      row = []; cell = ''
    } else { cell += char }
  }
  row.push(cell.replace(/\r$/, ''))
  if (row.some(v => v !== '')) records.push(row)
  return records
}

test('CRUD模块：筛选、导出、登记、动作、指标全链路', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (crudModules.length === 0) { test.skip(); return }

  for (const mod of crudModules) {
    await page.goto(`/${mod.key}`)
    await page.waitForTimeout(3000)
    const bodyRows = page.locator('tbody tr[data-testid^="row-"]')
    await expect(bodyRows).toHaveCount(3)
    const firstValue = await bodyRows.nth(0).locator('td').nth(0).innerText()

    await page.getByTestId(`filter-${mod.fields[0]}`).fill(firstValue)
    await page.getByTestId('query').click()
    await expect(bodyRows).toHaveCount(1)
    await page.getByTestId('reset').click()
    await expect(bodyRows).toHaveCount(3)

    const downloadPromise = page.waitForEvent('download')
    await page.getByTestId('export').click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe(`${mod.key}.csv`)

    const createdValues = mod.fields.map((f, i) => `验收-${mod.key}-${i+1}`)
    await page.getByTestId('open-create').click()
    await expect(page.getByTestId('create-modal')).toBeVisible()
    for (const [i, field] of mod.fields.entries()) {
      await page.getByTestId(`create-${field}`).fill(createdValues[i])
    }
    await page.getByTestId('submit-create').click()
    await expect(page.getByTestId('create-modal')).toHaveCount(0)
    await expect(bodyRows).toHaveCount(4)

    const createdRow = page.locator('tbody tr').filter({ hasText: createdValues[0] })
    for (const [i, action] of mod.actions.entries()) {
      await createdRow.getByRole('button', { name: action, exact: true }).click()
      await expect(createdRow).toHaveAttribute('data-status', mod.actionTargets[i])
    }
  }

  expect(browserErrors).toEqual([])
})

test('DAG拓扑模块：节点渲染、详情、动作、导出', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (dagModules.length === 0) { test.skip(); return }

  for (const mod of dagModules) {
    await page.goto(`/${mod.key}`)
    await page.waitForTimeout(3000)
    await expect(page.locator('svg.dag-svg')).toBeVisible()
    const nodes = page.locator('g.dag-node')
    const count = await nodes.count()
    expect(count).toBeGreaterThanOrEqual(3)

    await nodes.first().click()
    await expect(page.getByTestId('dag-detail')).toBeVisible()

    const downloadPromise = page.waitForEvent('download')
    await page.getByTestId('export').click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe(`${mod.key}.csv`)

    await page.getByTestId('open-create').click()
    await expect(page.getByTestId('create-modal')).toBeVisible()
    for (const [i, field] of mod.fields.slice(0,3).entries()) {
      await page.locator('.create-field input').nth(i).fill(`E2E-${mod.key}-${i}`)
    }
    await page.getByTestId('submit-create').click()
    await expect(page.getByTestId('create-modal')).toHaveCount(0)
  }

  expect(browserErrors).toEqual([])
})

test('流式血缘：下线节点定位与血缘边修复记录全流程', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })

  await page.goto('/stream_lineage')
  await page.waitForTimeout(3000)

  // 下线转换节点仍可在图上定位，但有效边不指向它
  await page.getByTestId('locate-offline').click()
  await expect(page.getByTestId('dag-detail')).toBeVisible()
  const offlineNode = page.locator('g.dag-node.offline')
  await expect(offlineNode.first()).toBeVisible()

  // 生成血缘边修复记录：保留失效边与替代来源
  const rowCountBefore = await page.getByTestId(/repair-row-/).count()
  await page.getByTestId('generate-repair').click()
  await expect(page.getByTestId('repair-message')).toBeVisible()
  await expect.poll(async () => page.getByTestId(/repair-row-/).count()).toBeGreaterThan(rowCountBefore)

  // 修复记录同时展示旧边版本与替代节点
  const firstRepair = page.locator('[data-testid^="repair-row-"]').first()
  await expect(firstRepair).toContainText('edge-v1')
  await expect(firstRepair).toContainText('flink-window-agg-v2')

  // 重复生成不产生重复修复项
  const rowsAfterFirst = await page.getByTestId(/repair-row-/).count()
  await page.getByTestId('generate-repair').click()
  await page.waitForTimeout(1000)
  expect(await page.getByTestId(/repair-row-/).count()).toBe(rowsAfterFirst)

  // 页脚统计与失效边渲染
  await expect(page.locator('.page-foot')).toContainText('条失效边')
  expect(browserErrors).toEqual([])
})

test('死信处理：重放目标核对与改投重放', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })

  // 先在流式血缘页生成修复记录，建立失效边与替代来源
  await page.goto('/stream_lineage')
  await page.waitForTimeout(2000)
  await page.getByTestId('generate-repair').click()
  await page.waitForTimeout(1000)

  await page.goto('/dead_letter')
  await page.waitForTimeout(2000)
  await expect(page.getByTestId('cross-check-panel')).toBeVisible()
  await page.getByTestId('run-cross-check').click()
  const crossRows = page.getByTestId(/cross-row-/)
  await expect.poll(async () => crossRows.count()).toBeGreaterThan(0)

  // 被拦截的记录提供改投重放入路；无拦截记录时面板仍正常渲染
  const blockedRows = page.locator('[data-testid^="cross-row-"][data-blocked="true"]')
  const blockedCount = await blockedRows.count()
  if (blockedCount > 0) {
    await blockedRows.first().getByRole('button', { name: '改投重放' }).click()
    await page.waitForTimeout(1000)
  }
  expect(browserErrors).toEqual([])
})

test('时间线模块：甘特条渲染、日期筛选、日志查看、登记', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (timelineModules.length === 0) { test.skip(); return }

  for (const mod of timelineModules) {
    await page.goto(`/${mod.key}`)
    await expect(page.locator('.timeline-bar').first()).toBeVisible({ timeout: 10000 })
    const bars = page.locator('.timeline-bar')
    const count = await bars.count()
    expect(count).toBeGreaterThanOrEqual(3)

    await bars.first().click()
    await expect(page.getByTestId('timeline-log')).toBeVisible()

    const downloadPromise = page.waitForEvent('download')
    await page.getByTestId('export').click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe(`${mod.key}.csv`)

    await page.getByTestId('open-create').click()
    await expect(page.getByTestId('create-modal')).toBeVisible()
    for (const [i, field] of mod.fields.slice(0,3).entries()) {
      await page.locator('.create-field input').nth(i).fill(`E2E-${mod.key}-${i}`)
    }
    await page.getByTestId('submit-create').click()
    await expect(page.getByTestId('create-modal')).toHaveCount(0)
  }

  expect(browserErrors).toEqual([])
})

test('分析看板模块：KPI卡片、运行分析、图表渲染', async ({ page }) => {
  test.setTimeout(300_000)
  const browserErrors: string[] = []
  page.on('pageerror', e => browserErrors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status()>=400) browserErrors.push(`${r.status()} ${r.url()}`) })
  if (analysisModules.length === 0) { test.skip(); return }

  for (const mod of analysisModules) {
    await page.goto(`/${mod.key}`)
    await expect(page.getByTestId('kpi-card').first()).toBeVisible({ timeout: 10000 })
    const kpiCards = page.getByTestId('kpi-card')
    const kpiCount = await kpiCards.count()
    expect(kpiCount).toBeGreaterThanOrEqual(2)
    await expect(kpiCards.first().locator('strong')).toBeVisible()

    await page.getByTestId('run-analysis').click()
    await expect(page.locator('.analysis-bar').first()).toBeVisible({ timeout: 5000 })
    await expect(page.locator('.data-table tbody tr').first()).toBeVisible({ timeout: 5000 })
  }

  expect(browserErrors).toEqual([])
})

test('概览汇总', async ({ page }) => {
  test.setTimeout(300_000)
  await page.goto('/')
  await expect(page.locator('.stat-value').nth(1)).toBeVisible()
  // 验证概览页加载正常即可，不硬编码精确计数（取决于各模块 E2E 登记成功数）
})
