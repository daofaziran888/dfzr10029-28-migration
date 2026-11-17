import { expect, test } from '@playwright/test'
import { readFile } from 'node:fs/promises'

const API = 'http://127.0.0.1:8000'

async function api(path: string, init?: RequestInit): Promise<any> {
  const response = await fetch(`${API}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  return response.json()
}

let sequence = 0

/** 每个用例新建一张自己的集市表，起手就是旧版日期目录，互不影响、可重复跑。 */
async function createLegacyTable(prefix: string): Promise<any> {
  sequence += 1
  const values = {
    表编号: `E2E-${prefix}-${Date.now()}-${sequence}`,
    表名称: `${prefix}集市`,
    主题域: '验收',
    事实粒度: prefix === '订单' ? '按订单' : '按日',
    关联维度: '日期,区域',
    度量字段: '金额,数量',
    刷新周期: '每日',
  }
  const payload = await api('/api/data_mart', {
    method: 'POST', body: JSON.stringify({ values }),
  })
  expect(payload.ok, JSON.stringify(payload)).toBeTruthy()
  expect(payload.entry['分区版本']).toBe('legacy')
  return payload.entry
}

test.describe('数据集市分区迁移', () => {
  test('页面预览批次、执行迁移、核对并导出分区版本', async ({ page }) => {
    test.setTimeout(120_000)
    const errors: string[] = []
    page.on('pageerror', (error) => errors.push(error.message))
    page.on('response', (response) => {
      if (response.url().includes('/api/') && response.status() >= 400) {
        errors.push(`${response.status()} ${response.url()}`)
      }
    })

    const table = await createLegacyTable('销售')
    const tableId = String(table.id)
    await page.goto('/data_mart')
    await page.waitForTimeout(1500)

    const legacyRow = page.getByTestId(`mart-table-${tableId}`)
    await expect(legacyRow).toHaveAttribute('data-version', 'legacy')
    await expect(legacyRow).toHaveAttribute('data-format', '日期目录')

    await page.getByTestId(`open-migration-${tableId}`).click()
    await expect(page.getByTestId('migration-form')).toBeVisible()

    await page.getByTestId('migration-mode').selectOption('影子分区切换')
    await page.getByTestId('migration-source').selectOption('旧版集市分区')
    await page.getByTestId('migration-target-version').fill('')
    await page.getByTestId('preview-migration').click()
    await expect(page.getByTestId('migration-card')).toBeVisible()
    await expect(page.getByTestId('migration-status')).toHaveText('预览')
    await expect(page.getByTestId('migration-batches')).toBeVisible()
    const batches = page.locator('[data-testid^="migration-batch-"]')
    expect(await batches.count()).toBeGreaterThanOrEqual(1)
    const targetVersion = (await page.getByTestId('migration-target').innerText()).trim()

    await page.getByTestId('execute-one-batch').click()
    await expect(page.getByTestId('migration-status')).toHaveText('已中断')
    // 影子回填期间旧目录仍对外可见，看板不混读。
    await expect(page.locator('[data-testid^="visible-partition-"]').first()).toContainText('legacy')

    await page.getByTestId('execute-migration').click()
    await expect(page.getByTestId('migration-status')).toHaveText('已执行')
    await expect(legacyRow).toHaveAttribute('data-format', '版本化分区')
    await expect(legacyRow).toHaveAttribute('data-version', targetVersion)

    // 核对走同一迁移记录、按目标版本回查，结果落库。
    await page.getByTestId('reconcile-migration').click()
    await expect(page.getByTestId('reconcile-result')).toContainText('核对通过')
    const persisted = await api(
      `/api/data_mart/reconcile?table_id=${tableId}&version=${targetVersion}`)
    expect(persisted.reconcile_status).toBe('一致')
    expect(persisted.source_version).toBe('legacy')

    // 迁移与版本同步切换：可见分区全部是新版本。
    await page.waitForTimeout(500)
    const visibleVersions = await page.locator('[data-testid^="visible-partition-"] td:nth-child(2)')
      .allInnerTexts()
    expect(visibleVersions.length).toBeGreaterThanOrEqual(1)
    expect(new Set(visibleVersions)).toEqual(new Set([targetVersion]))

    // 发布版本选择器允许切回旧目录，看板读取口径同步且不混读。
    await page.getByTestId('publish-target').selectOption('legacy')
    await page.getByTestId('publish-version').click()
    await expect(legacyRow).toHaveAttribute('data-version', 'legacy')
    const legacyVersions = await page.locator('[data-testid^="visible-partition-"] td:nth-child(2)')
      .allInnerTexts()
    expect(new Set(legacyVersions)).toEqual(new Set(['legacy']))
    // 切回目标版本，导出使用该版本。
    await page.getByTestId('publish-target').selectOption(targetVersion)
    await page.getByTestId('publish-version').click()
    await expect(legacyRow).toHaveAttribute('data-version', targetVersion)

    // 导出文件保留分区版本与事实粒度。
    const downloadPromise = page.waitForEvent('download')
    await page.getByTestId('export').click()
    const download = await downloadPromise
    expect(download.suggestedFilename()).toBe('data_mart.csv')
    const text = await readFile((await download.path())!, 'utf8')
    expect(text).toContain('fact_grain')
    expect(text).toContain(targetVersion)
    expect(text).toContain('金额')
    expect(errors).toEqual([])
  })

  test('原位批量迁移中断后可回滚，只恢复已迁移分区', async ({ page }) => {
    test.setTimeout(120_000)
    const table = await createLegacyTable('订单')
    const tableId = String(table.id)
    const record = await api('/api/data_mart/migrations', {
      method: 'POST',
      body: JSON.stringify({
        table_id: table.id, mode: '原位批量迁移', source: '存量事实数据', batch_size: 1,
      }),
    })
    expect(record.ok, JSON.stringify(record)).toBeTruthy()
    const migrationId = record.entry.id
    const targetVersion = record.entry.target_version
    const interrupted = await api(`/api/data_mart/migrations/${migrationId}/execute`, {
      method: 'POST',
      body: JSON.stringify({ max_batches: 1, fail_at_batch: 2 }),
    })
    expect(interrupted.entry.status).toBe('已中断')
    expect(interrupted.entry.rollback_scope).toContain('已迁移批次分区')

    // 原位迁移中途：同一日期只命中一个版本，看板不会同时读到新旧分区。
    const visible = await api(`/api/data_mart/visible?table_id=${table.id}`)
    const versions = visible.partitions.map((partition: any) => partition.version)
    expect(versions).toContain('legacy')
    expect(versions).toContain(targetVersion)

    await page.goto('/data_mart')
    await page.waitForTimeout(1000)
    await page.getByTestId(`select-table-${tableId}`).click()
    await expect(page.getByTestId('migration-card')).toBeVisible()
    await expect(page.getByTestId('migration-status')).toHaveText('已中断')
    await page.getByTestId('rollback-migration').click()
    await expect(page.getByTestId('migration-status')).toHaveText('已回滚')
    await expect(page.getByTestId(`mart-table-${tableId}`)).toHaveAttribute('data-format', '日期目录')

    const rolledBack = await api(`/api/data_mart/partitions?table_id=${table.id}`)
    expect(new Set(rolledBack.items.map((partition: any) => partition.version))).toEqual(
      new Set(['legacy']))
  })

  test('版本号被占用时迁移记录保留版本边界', async () => {
    const table = await createLegacyTable('边界')
    // 先占一个目标版本名，模拟另一处来源已用掉该版本。
    const first = await api('/api/data_mart/migrations', {
      method: 'POST', body: JSON.stringify({ table_id: table.id, target_version: 'v1' }),
    })
    expect(first.ok, JSON.stringify(first)).toBeTruthy()
    await api(`/api/data_mart/migrations/${first.entry.id}/execute`, {
      method: 'POST', body: JSON.stringify({}),
    })

    const second = await api('/api/data_mart/migrations', {
      method: 'POST', body: JSON.stringify({ table_id: table.id, target_version: 'v1' }),
    })
    // 表已经版本化，不再重复迁移，但历史记录仍保留 v1 的版本边界。
    expect(second.ok).toBeFalsy()
    expect(second.message).toContain('无需再次迁移')

    const records = await api(`/api/data_mart/versions?table_id=${table.id}&version=v1`)
    expect(records.total).toBe(1)
    expect(records.items[0].target_version).toBe('v1')
    expect(records.items[0].status).toBe('已执行')
  })
})
