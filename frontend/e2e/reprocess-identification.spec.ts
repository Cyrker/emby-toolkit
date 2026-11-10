import { expect, test } from '@playwright/test'

for (const viewport of [{ name: 'desktop', width: 1440, height: 1000 }, { name: 'mobile', width: 390, height: 844 }]) {
  test(`ambiguous metadata can be selected and retried on ${viewport.name}`, async ({ page }, testInfo) => {
    await page.setViewportSize(viewport)
    const submissions: { body: any; key?: string }[] = []
    const record = { id: '00000000-0000-0000-0000-000000000123', item_id: '123', name: '补齐缺失元数据: 同名作品',
      title: null, source: '手动刮削', path: '', status: 'failed', job_status: 'failed',
      created_at: '2026-11-09T08:00:00Z', completeness_checked: false,
      selected: null, verified: null, message: 'TMDb 候选过多，无法唯一识别', incomplete: [], retry: {},
    }
    await page.route('**/api/**', async route => {
      const request = route.request()
      const path = new URL(request.url()).pathname
      if (!path.startsWith('/api/')) { await route.continue(); return }
      let data: unknown = { success: true, data: [], items: [], total: 0 }
      if (path.endsWith('/auth/check_status')) data = { status: 'logged_in', user: { id: 'fixture-admin', name: '测试管理员', is_admin: true } }
      else if (path.endsWith('/auth/service_status')) data = { authorized: true, url: 'http://emby.invalid' }
      else if (path === '/api/status') data = { is_running: false }
      else if (path === '/api/config') data = { task_chain_sequence: [], task_chain_low_freq_sequence: [] }
      else if (path === '/api/emby/scrape-records') data = { items: [record], total: 1 }
      else if (path.endsWith('/123/identification')) data = {
        requires_selection: true, source_job_id: record.id, snapshot: 'a'.repeat(64), query: '同名作品',
        target: { item_id: '123', root_item_id: '123', name: '同名作品', root_name: '同名作品', media_type: 'movie', year: 2020, path: '' },
        candidates: [
          { tmdb_id: '1', media_type: 'movie', title: '同名作品', original_title: 'First Original Title', year: '2020', overview: '第一个同名候选的简介，描述不同的故事背景。', poster_path: null },
          { tmdb_id: '2', media_type: 'movie', title: '同名作品', original_title: 'Second Original Title', year: '2021', overview: '第二个同名候选的简介，可以通过原名和年份区分正确作品。', poster_path: null },
        ], page: 1, total_pages: 1,
      }
      else if (path.endsWith('/reprocess_item/123')) {
        submissions.push({ body: request.postDataJSON(), key: request.headers()['idempotency-key'] })
        record.status = 'running'
        record.job_status = 'queued'
        data = { job_id: 'retry-job', status_url: '/api/jobs/retry-job', message: '任务已提交' }
      }
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data) })
    })
    await page.goto('/organize-records')
    await expect(page.getByText('尚无媒体缓存', { exact: false })).toBeVisible()
    await expect(page.getByText('任务未保存文件路径')).toHaveCount(1)
    await page.getByRole('button', { name: '补齐资料', exact: true }).click()
    const modal = page.locator('.identity-modal')
    await expect(modal).toBeVisible()
    await expect(modal.getByText('Second Original Title', { exact: true })).toBeVisible()
    await expect(modal.getByRole('button', { name: '确认并重新处理' })).toBeDisabled()
    await modal.getByRole('button', { name: '取消', exact: true }).click()
    expect(submissions).toHaveLength(0)
    await page.getByRole('button', { name: '补齐资料', exact: true }).click()
    await modal.locator('input[type="radio"]').nth(1).check()
    await expect(modal.getByRole('button', { name: '确认并重新处理' })).toBeEnabled()
    await page.screenshot({ path: testInfo.outputPath('identity-modal.png'), fullPage: true, animations: 'disabled' })
    expect(await modal.evaluate(element => {
      const rect = element.getBoundingClientRect()
      return rect.left >= 0 && rect.right <= innerWidth && element.scrollWidth <= element.clientWidth + 1
    })).toBe(true)
    await modal.getByRole('button', { name: '确认并重新处理' }).click()
    await expect(modal).not.toBeVisible()
    await expect.poll(() => submissions.length).toBe(1)
    expect(submissions[0].body).toEqual({ only_missing: true, source_job_id: record.id,
      tmdb_selection: { tmdb_id: '2', media_type: 'movie', snapshot: 'a'.repeat(64) } })
    expect(submissions[0].key).toMatch(/^[a-f0-9]{32}$/)
    await expect(page.getByText('排队 / 处理中', { exact: true }).first()).toBeVisible()
  })
}
