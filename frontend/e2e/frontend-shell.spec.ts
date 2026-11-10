import { expect, test, type Page } from '@playwright/test'

test('public login route remains reachable without an authenticated session', async ({ page }) => {
  await installApiStubs(page, false)
  await page.goto('/login')
  await expect(page).toHaveTitle('embytool')
  await expect(page).toHaveURL(/\/login$/)
  await expect(page.getByRole('heading', { name: 'embytool', exact: true })).toBeVisible()
  await expect(page.getByRole('img', { name: 'embytool', exact: true })).toHaveAttribute('src', /^(?:\/brand-icon\.svg|data:image\/svg\+xml,)/)
})

test('public setup route preserves the existing path', async ({ page }) => {
  await installApiStubs(page)
  await page.goto('/setup')
  await expect(page).toHaveTitle('embytool')
  await expect(page.getByRole('heading', { name: 'embytool', exact: true })).toBeVisible()
})

test('mobile login uses the independent brand icon', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await installApiStubs(page, false)
  await page.goto('/login')
  await expect(page).toHaveTitle('embytool')
  await expect(page.getByRole('img', { name: 'embytool', exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'embytool', exact: true })).toBeVisible()
})

test('web quick task submits through the existing task endpoint', async ({ page }) => {
  const submissions: unknown[] = []
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const url = new URL(request.url())
    if (!url.pathname.startsWith('/api/')) {
      await route.continue()
      return
    }
    if (url.pathname.endsWith('/auth/check_status')) {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ status: 'logged_in', user: { id: 'e2e-user', name: 'E2E User', is_admin: true } }),
      })
      return
    }
    if (url.pathname.endsWith('/auth/service_status')) {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ authorized: true, url: 'http://emby.invalid' }),
      })
      return
    }
    if (url.pathname.endsWith('/config')) {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          tg_menu_tasks: ['populate-metadata'],
          task_chain_sequence: [],
          task_chain_low_freq_sequence: [],
        }),
      })
      return
    }
    if (url.pathname.endsWith('/tasks/available')) {
      const tasks = url.searchParams.get('context') === 'all'
        ? [{ key: 'populate-metadata', name: '同步媒体数据', help: '同步 Emby 媒体数据。' }]
        : []
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(tasks) })
      return
    }
    if (url.pathname.endsWith('/tasks/run')) {
      submissions.push(request.postDataJSON())
      await route.fulfill({
        status: 202,
        contentType: 'application/json',
        body: JSON.stringify({ message: '任务已成功提交。' }),
      })
      return
    }
    if (url.pathname.endsWith('/status')) {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ is_running: false }),
      })
      return
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, data: [] }) })
  })

  await page.goto('/settings/scheduler')
  const quickTaskCard = page.locator('.dashboard-card').filter({ hasText: '网页与 Telegram 快捷任务' })
  await expect(quickTaskCard.getByText('同步媒体数据')).toBeVisible()
  await quickTaskCard.getByRole('button', { name: '立即执行' }).click()
  await page.getByRole('button', { name: '快速模式（增量）' }).click()
  await expect.poll(() => submissions).toEqual([
    { task_name: 'populate-metadata', force_full_update: false },
  ])
})

test('native task logs render in realtime and historical views', async ({ page }) => {
  await installApiStubs(page)
  await page.route('**/api/status', (route) => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({
      is_running: true, current_action: '同步媒体数据', progress: 85,
      logs: [{ timestamp: '2026-09-12T04:00:00+00:00', level: 'info', message: '正在同步媒体元数据 (85/100)' }],
    }),
  }))
  await page.route('**/api/logs/list', (route) => route.fulfill({
    contentType: 'application/json', body: JSON.stringify(['app.log']),
  }))
  await page.route('**/api/logs/view?*', (route) => route.fulfill({
    contentType: 'text/plain', body: '2026-09-12T04:00:00+00:00 [INFO] 同步媒体数据：状态=succeeded',
  }))
  await page.goto('/settings/scheduler')
  await page.getByRole('button', { name: '实时日志', exact: true }).click()
  await expect(page.locator('.log-timeline:visible .log-summary')).toHaveText('正在同步媒体元数据 (85/100)')
  await expect(page.locator('.log-timeline:visible .log-level')).toHaveText('INFO')
  await expect(page.locator('.log-timeline:visible')).not.toContainText('[object Object]')
  await page.keyboard.press('Escape')
  await page.getByRole('button', { name: '历史日志', exact: true }).click()
  await page.locator('.log-timeline:visible .technical-logs > summary').click()
  await expect(page.locator('.log-timeline:visible .log-summary')).toHaveText('同步媒体数据：状态=succeeded')
})

async function installApiStubs(page: Page, authenticated = true) {
  await page.route('**/api/**', async (route) => {
    const pathname = new URL(route.request().url()).pathname
    if (!pathname.startsWith('/api/')) {
      await route.continue()
      return
    }
    if (pathname.endsWith('/auth/check_status')) {
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(authenticated
        ? { status: 'logged_in', user: { id: 'e2e-user', name: 'E2E User', is_admin: true } }
        : { status: 'login_required' }) })
      return
    }
    if (pathname.endsWith('/auth/service_status')) {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ authorized: true, url: 'http://emby.invalid' }),
      })
      return
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, data: [] }) })
  })
}

const authenticatedRoutes = [
  ['/settings/general', 'config'],
  ['/settings/scheduler', 'task'],
  ['/organize-records', 'organize'],
  ['/unified-subscriptions', 'subscription'],
  ['/user-center', 'user center'],
] as const

for (const [path, workflow] of authenticatedRoutes) {
  test(`${workflow} workflow keeps its authenticated route`, async ({ page }) => {
    await installApiStubs(page)
    await page.goto(path)
    await expect(page).toHaveURL(new RegExp(`${path.replaceAll('/', '\\/')}$`))
  })
}
