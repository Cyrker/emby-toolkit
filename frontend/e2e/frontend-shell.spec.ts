import { expect, test, type Page } from '@playwright/test'

test('public login route remains reachable without an authenticated session', async ({ page }) => {
  await page.goto('/login')
  await expect(page).toHaveTitle('Emby Toolkit')
  await expect(page.locator('body')).toContainText(/登录|Login/)
})

test('public setup route preserves the existing path', async ({ page }) => {
  await page.goto('/setup')
  await expect(page).toHaveTitle('Emby Toolkit')
})

async function installApiStubs(page: Page) {
  await page.route('**/api/**', async (route) => {
    const pathname = new URL(route.request().url()).pathname
    if (pathname.endsWith('/auth/check_status')) {
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
        status: 'logged_in',
        user: { id: 'e2e-user', name: 'E2E User', is_admin: true },
      }) })
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
  ['/shared-resources', 'sharing'],
  ['/user-center', 'user center'],
] as const

for (const [path, workflow] of authenticatedRoutes) {
  test(`${workflow} workflow keeps its authenticated route`, async ({ page }) => {
    await installApiStubs(page)
    await page.goto(path)
    await expect(page).toHaveURL(new RegExp(`${path.replaceAll('/', '\\/')}$`))
  })
}
