import { expect, test, type Locator, type Page } from '@playwright/test'

const libraries = [{ Id: 'movies', Name: '电影', value: 'movies', label: '电影' }]
const config = {
  emby_server_url: 'http://emby.invalid', emby_auth_mode: 'user_token', emby_api_key: '********',
  libraries_to_process: ['movies'], proxy_native_view_selection: [],
  monitor_paths: [], monitor_exclude_dirs: [], monitor_extensions: ['.mkv'],
  p115_auth_method: 'web', p115_api_priority: 'openapi', p115_request_interval: 1, p115_max_workers: 5,
  p115_extensions: ['mkv', 'mp4'], p115_save_path_name: '待整理目录', p115_save_path_cid: '1',
  p115_unrecognized_name: '未识别目录', p115_unrecognized_cid: '2',
  p115_media_root_name: '媒体库', p115_media_root_cid: '3', p115_min_video_size: 10,
  etk_server_url_auto_detected: 'http://etk.invalid:5257',
  ai_provider: 'openai', ai_model_name: 'example-model', ai_translation_mode: 'quality',
  network_proxy_enabled: true, telegram_notify_types: [],
  task_chain_enabled: true, task_chain_low_freq_enabled: true,
  task_chain_sequence: ['populate-metadata'], task_chain_low_freq_sequence: [], tg_menu_tasks: ['populate-metadata'],
}
const success = (data: unknown) => ({ success: true, data })

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('isDark', 'true')
    localStorage.setItem('user-theme', 'minimal')
  })
  await page.route('**/api/**', async route => {
    const path = new URL(route.request().url()).pathname
    if (!path.startsWith('/api/')) return route.continue()
    const responses: Record<string, unknown> = {
      '/api/auth/check_status': { status: 'logged_in', user: { id: 'layout-user', name: 'Layout User', is_admin: true } },
      '/api/auth/service_status': { authorized: true, url: 'http://emby.invalid' },
      '/api/config': config, '/api/config/custom_theme': { custom_theme: {} },
      '/api/status': { is_running: false, logs: [] },
      '/api/p115/status': success({ has_token: true, has_cookie: true, nickname: '布局测试账号' }),
      '/api/p115/play_pool': success({ enabled: false, accounts: [] }),
      '/api/p115/sorting_rules': [],
      '/api/p115/washing_priority_config': success({ conflict_mode: 'replace' }),
      '/api/p115/washing_priority_groups': success([{ id: 'group-1', name: '电影与剧集规则组', media_type: 'All', target_cids: [], priorities: [] }]),
      '/api/p115/release_groups': success([]),
      '/api/resubscribe/library_status': [], '/api/resubscribe/rules': [], '/api/resubscribe/libraries': libraries,
      '/api/config/cover_generator/libraries': libraries,
      '/api/config/cover_generator': {
        enabled: true, transfer_monitor: true, show_item_count: true, sort_by: 'Random', max_safe_rating: 'PG-13',
        exclude_libraries: [], badge_style: 'badge', badge_size_ratio: 0.12,
        title_config: '电影: [精选电影, MOVIES]', cover_style: 'single',
      },
      '/api/cleanup/tasks': [],
      '/api/cleanup/settings': { rules: [{ id: 'runtime', enabled: true, priority: 'desc' }, { id: 'date_added', enabled: true, priority: 'asc' }], library_ids: ['movies'] },
      '/api/admin/users': [{ Id: 'layout-user', Name: 'Layout User', Policy: {}, template_name: '布局测试模板' }],
      '/api/admin/user_templates': [{ id: 1, name: '布局测试模板', source_user_id: 'layout-user', source_user_name: 'Layout User' }],
      '/api/admin/invitations': [{ id: 1, status: 'active', template_name: '布局测试模板', expiration_days: 30 }],
      '/api/tasks/available': [{ key: 'populate-metadata', name: '同步媒体数据与扩展信息的较长任务名称', help: '布局测试任务' }],
    }
    if (route.request().method() !== 'GET') {
      if (path === '/api/config/cover_generator/preview') return route.fulfill({ json: { image: '' } })
      throw new Error(`Layout checks must not submit writes: ${path}`)
    }
    const response = responses[path] ?? (path.endsWith('/views') ? { Items: libraries } : path.includes('libraries') ? libraries : path.startsWith('/api/custom_collections/config/') ? [] : success([]))
    await route.fulfill({ json: response })
  })
})

test.afterEach(async ({ page }, testInfo) => {
  await page.screenshot({ path: testInfo.outputPath('layout.png'), animations: 'disabled' })
})

async function settled(page: Page) {
  await expect(page.locator('.n-spin--show')).toHaveCount(0)
  await page.waitForFunction(() => !document.querySelector('[class*="-transition-enter-active"],[class*="-transition-leave-active"]'))
}

async function generalTab(page: Page, name: string) {
  await page.goto('/settings/general')
  await page.locator('.settings-tabs').getByText(name, { exact: true }).click()
  await settled(page)
}

async function readableControls(root: Locator, minimum = 120) {
  await expect.poll(() => root.evaluate((element, minimum) => {
    const bounds = element.getBoundingClientRect()
    return [...element.querySelectorAll('.n-input-number, .n-select, .n-input:not(.n-input-number .n-input)')]
      .filter(control => control.getBoundingClientRect().height > 0)
      .map(control => control.getBoundingClientRect())
      .filter(rect => rect.width < minimum || rect.left < bounds.left - 1 || rect.right > bounds.right + 1)
      .length
  }, minimum)).toBe(0)
}

async function noPageOverflow(page: Page) {
  await expect.poll(() => page.evaluate(() => [document.documentElement, document.body, ...document.querySelectorAll('.app-main-content-wrapper, .settings-page')]
    .filter(element => element.clientWidth > 0 && element.scrollWidth > element.clientWidth + 1).length)).toBe(0)
}

async function cardControlsFit(root: Locator) {
  await expect.poll(() => root.evaluate(element => {
    const problems: string[] = []
    for (const card of element.querySelectorAll('.dashboard-card')) {
      const content = card.querySelector('.n-card__content')!
      const bounds = content.getBoundingClientRect()
      const title = card.querySelector('.card-title')?.textContent?.trim() || 'card'
      for (const control of content.querySelectorAll('.n-input-number, .n-select, .n-input:not(.n-input-number .n-input), .n-input-group, .n-form-item-blank, .n-switch, .n-button')) {
        const rect = control.getBoundingClientRect()
        if (rect.height <= 0) continue
        if (rect.left < bounds.left - 1 || rect.right > bounds.right + 1) problems.push(`${title}: ${control.classList[0]} exceeds card content`)
        if (control.matches('.n-input-number, .n-select, .n-input:not(.n-input-number .n-input)')) {
          const minimum = control.closest('.n-input-group') ? 96 : 120
          if (rect.width < minimum) problems.push(`${title}: ${control.classList[0]} narrower than ${minimum}px`)
        }
      }
      for (const group of content.querySelectorAll('.n-input-group')) {
        const groupBounds = group.getBoundingClientRect()
        if (groupBounds.height <= 0) continue
        for (const child of group.children) {
          const rect = child.getBoundingClientRect()
          if (rect.left < groupBounds.left - 1 || rect.right > groupBounds.right + 1) problems.push(`${title}: input group child exceeds group`)
          if (child.matches('.n-button') && rect.width < 32) problems.push(`${title}: input group button is squeezed`)
        }
      }
    }
    return problems
  })).toEqual([])
}

for (const width of [1366, 390]) {
  test.describe(`${width}px settings`, () => {
    test.use({ viewport: { width, height: 1000 } })

    test('115 fields and folder buttons retain usable width', async ({ page }) => {
      await generalTab(page, '115 网盘')
      const grid = page.locator('.p115-settings-grid')
      await expect.poll(() => grid.evaluate(element => getComputedStyle(element).gridTemplateColumns.split(/\s+/).length)).toBe(width === 390 ? 1 : 2)
      await readableControls(grid)
      await expect.poll(() => grid.locator('.n-input-group').evaluateAll(groups => groups.some(group => {
        const bounds = group.getBoundingClientRect()
        return [...group.children].some(child => child.getBoundingClientRect().right > bounds.right + 1)
      }))).toBe(false)
      await noPageOverflow(page)
    })

    test('cover fields and title delete buttons fit', async ({ page }) => {
      await page.goto('/settings/cover-generator')
      await expect(page.locator('.cover-generator-config .n-select').first()).toBeVisible()
      await settled(page)
      await expect.poll(() => page.getByText('媒体库封面生成', { exact: true }).evaluate(element => element.getBoundingClientRect().height)).toBeLessThan(40)
      await readableControls(page.locator('.cover-generator-config'))
      await page.locator('.n-tabs-tab').getByText('封面标题', { exact: true }).click()
      await settled(page)
      await readableControls(page.locator('.cover-title-row').first())
      const row = page.locator('.cover-title-row').first()
      await expect.poll(() => row.evaluate(element => {
        const bounds = element.getBoundingClientRect()
        const button = element.querySelector('button')!.getBoundingClientRect()
        return button.width >= 32 && button.left >= bounds.left - 1 && button.right <= bounds.right + 1
      })).toBe(true)
      for (const tab of ['单图风格设置', '多图风格设置', '其他设置']) {
        await page.locator('.n-tabs-tab').getByText(tab, { exact: true }).click()
        await settled(page)
        await readableControls(page.locator('.n-tab-pane:visible'))
      }
      await noPageOverflow(page)
    })

    test('washing rule details remain editable', async ({ page }) => {
      await generalTab(page, '115 网盘')
      await page.getByRole('button', { name: '配置洗版规则', exact: true }).click()
      await settled(page)
      const modal = page.locator('.n-modal:visible').last()
      await expect(modal.getByText('电影与剧集规则组', { exact: true })).toBeVisible()
      await readableControls(modal)
      if (width === 390) {
        await expect.poll(() => modal.evaluate(element => element.querySelector('.washing-group-editor')!.getBoundingClientRect().top >= element.querySelector('.washing-group-sidebar')!.getBoundingClientRect().bottom)).toBe(true)
      }
      await modal.getByRole('button', { name: '添加优先级', exact: true }).click()
      await expect(modal.locator('.priority-card .edit-view')).toBeVisible()
      await readableControls(modal)
      await noPageOverflow(page)
    })

    test('sorting toolbar and media type buttons fit', async ({ page }) => {
      await generalTab(page, '115 网盘')
      await page.getByRole('button', { name: '管理分类规则', exact: true }).click()
      await settled(page)
      await readableControls(page.locator('.sorting-filter-toolbar'))
      await page.getByRole('button', { name: '添加新规则', exact: true }).click()
      await settled(page)
      const editor = page.locator('.sorting-rule-editor')
      await readableControls(editor)
      await expect.poll(() => editor.locator('.n-radio-button').evaluateAll(buttons => buttons.every(button => {
        const rect = button.getBoundingClientRect()
        return rect.left >= 0 && rect.right <= innerWidth
      }))).toBe(true)
      await noPageOverflow(page)
    })

    test('resubscribe condition value is not squeezed away', async ({ page }) => {
      await page.goto('/resubscribe')
      await page.getByRole('button', { name: '规则设定', exact: true }).click()
      await page.getByRole('button', { name: '新增规则', exact: true }).click()
      await settled(page)
      const modal = page.locator('.n-modal:visible').last()
      await readableControls(modal, 96)
      await expect(modal.locator('.rule-value .n-select')).toBeVisible()
      await expect.poll(() => modal.locator('.rule-value .n-select').evaluate(element => element.getBoundingClientRect().width)).toBeGreaterThanOrEqual(120)
      await noPageOverflow(page)
    })

    test('mapping inputs retain space for text', async ({ page }) => {
      await generalTab(page, '115 网盘')
      await page.getByRole('button', { name: '管理词表', exact: true }).click()
      await page.getByRole('button', { name: '添加关键词', exact: true }).click()
      await settled(page)
      await readableControls(page.locator('.mapping-manager .list-item'))
      await page.locator('.mapping-manager .n-tabs-tab').getByText('工作室', { exact: true }).click()
      await page.getByRole('button', { name: '添加工作室', exact: true }).click()
      await settled(page)
      await readableControls(page.locator('.mapping-manager .list-item'))
      await noPageOverflow(page)
    })

    test('user tables scroll locally without squeezing headers', async ({ page }) => {
      await page.goto('/user-management')
      await expect(page.locator('.user-table .n-data-table-th').first()).toBeVisible()
      await readableControls(page.locator('.user-filters'))
      for (const name of ['用户列表', '邀请链接', '用户模板']) {
        await page.locator('.n-tabs-tab').getByText(name, { exact: true }).click()
        await settled(page)
        const table = page.locator('.n-tab-pane:visible .n-data-table')
        await expect.poll(() => table.locator('.n-data-table-th').evaluateAll(headers => headers
          .filter(header => header.textContent?.trim()).some(header => header.getBoundingClientRect().width < 80))).toBe(false)
        if (width === 390) {
          await expect.poll(() => table.evaluate(element => [...element.querySelectorAll('.n-scrollbar-container')]
            .some(container => container.scrollWidth > container.clientWidth))).toBe(true)
        }
        await noPageOverflow(page)
      }
    })

    test('cleanup names and actions use separate space on mobile', async ({ page }) => {
      await page.goto('/media-cleanup')
      await page.getByRole('button', { name: '清理规则', exact: true }).click()
      await settled(page)
      const modal = page.locator('.n-modal:visible').last()
      await expect.poll(() => modal.locator('.rule-details').evaluateAll(details => details
        .some(element => element.getBoundingClientRect().width < 120))).toBe(false)
      await noPageOverflow(page)
    })

    test('task labels and execution buttons fit inside cards', async ({ page }) => {
      await page.goto('/settings/scheduler')
      const task = page.locator('.temp-task-item').first()
      await expect(task).toBeVisible()
      await settled(page)
      await expect.poll(() => task.evaluate(element => {
        const bounds = element.getBoundingClientRect()
        return [...element.children].every(child => {
          const rect = child.getBoundingClientRect()
          return rect.width > 0 && rect.left >= bounds.left && rect.right <= bounds.right
        })
      })).toBe(true)
      await noPageOverflow(page)
    })
  })
}

for (const width of [390, 1366]) {
  for (const [tab, title] of [['115 网盘', '账户与连接'], ['智能服务 & 订阅源', 'AI 增强'], ['高级', '网络代理']]) {
    test(`${width === 390 ? 'mobile' : 'desktop'} card heading stays horizontal: ${title}`, async ({ page }) => {
      await page.setViewportSize({ width, height: 1000 })
      await generalTab(page, tab)
      const pane = page.locator('.n-tab-pane:visible')
      const titleElement = pane.locator('.card-title').filter({ hasText: title }).first()
      await expect(titleElement).toBeVisible()
      await expect.poll(() => titleElement.evaluate(element => element.getBoundingClientRect().height)).toBeLessThan(40)
      if (tab === '智能服务 & 订阅源' || tab === '高级') {
        await cardControlsFit(pane)
      }
      await noPageOverflow(page)
    })
  }
}
