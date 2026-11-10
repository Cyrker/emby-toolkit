import { expect, test, type Page } from '@playwright/test'

const monitorPaths = [
  '/mnt/media/电视剧/Very.Long.Series.Title.2026.2160p.WEB-DL.DDP5.1.H.265/Season.01',
  '/mnt/media/电影/Another.Very.Long.Movie.Title.2026.2160p.BluRay.REMUX.DV.HDR',
  '/mnt/media/动漫/Long.Animation.Collection.With.Multiple.Seasons.And.Specials',
]
const excludePaths = [
  '/mnt/media/电视剧/Excluded.Long.Collection.Name.With.Extras.And.Behind.The.Scenes',
  '/mnt/media/电影/Temporary.Downloads.And.Unrecognized.Media.Waiting.For.Review',
]
const extensions = ['.mkv', '.mp4', '.avi', '.mov', '.ts', '.m2ts', '.strm']
const tmdbUrl = 'https://api.themoviedb.org/3'

const viewports = [
  { name: 'desktop', width: 1920, height: 1080, columns: 3 },
  { name: 'three-column threshold', width: 1600, height: 1000, columns: 3 },
  { name: 'narrow desktop', width: 1366, height: 900, columns: 2 },
  { name: 'mobile', width: 390, height: 844, columns: 1 },
] as const

for (const viewport of viewports) {
  test(`general settings keep long form values within cards on ${viewport.name}`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height })
    await page.addInitScript(() => {
      localStorage.setItem('isDark', 'true')
      localStorage.setItem('user-theme', 'minimal')
    })
    await installApiStubs(page)
    await page.goto('/settings/general')
    await page.locator('.settings-tabs').getByText('通用设置', { exact: true }).click()

    const pane = page.locator('.n-tab-pane').filter({ has: page.getByText('基础设置', { exact: true }) })
    const grid = pane.locator('.n-grid').first()
    const cards = grid.locator('.dashboard-card')
    await expect(cards).toHaveCount(3)
    await expect.poll(() => grid.evaluate((element) => getComputedStyle(element).gridTemplateColumns.split(/\s+/).length)).toBe(viewport.columns)
    for (const title of ['基础设置', '实时监控', '数据源与API']) {
      await expect(grid.getByText(title, { exact: true })).toBeVisible()
    }
    for (const path of [...monitorPaths, ...excludePaths]) {
      await expect(grid.getByText(path, { exact: true })).toBeVisible()
    }
    for (const extension of extensions) {
      await expect(grid.getByText(extension, { exact: true })).toBeVisible()
    }
    await expect(grid.getByPlaceholder(tmdbUrl, { exact: true })).toHaveValue(tmdbUrl)
    await expect(grid.locator('.n-input-group')).toHaveCount(3)
    await page.screenshot({ path: testInfo.outputPath('general-settings.png'), fullPage: true, animations: 'disabled' })
    if (viewport.columns === 3) {
      await grid.screenshot({ path: testInfo.outputPath('general-settings-grid.png'), animations: 'disabled' })
    } else {
      for (let index = 0; index < 3; index += 1) {
        await cards.nth(index).evaluate((card) => card.scrollIntoView({ block: 'start' }))
        await page.screenshot({ path: testInfo.outputPath(`general-settings-card-${index + 1}.png`), animations: 'disabled' })
      }
    }

    await expect.poll(() => grid.evaluate((element) => {
      const problems: string[] = []
      const tolerance = 1
      const gridRect = element.getBoundingClientRect()
      const checkBounds = (child: Element, container: DOMRect, description: string) => {
        const rect = child.getBoundingClientRect()
        if (rect.width <= 0 || rect.left < container.left - tolerance || rect.right > container.right + tolerance) {
          problems.push(`${description}: ${Math.round(rect.left)}..${Math.round(rect.right)} outside ${Math.round(container.left)}..${Math.round(container.right)}`)
        }
      }

      for (const card of element.querySelectorAll('.dashboard-card')) {
        const title = card.querySelector('.card-title')?.textContent?.trim() || 'card'
        checkBounds(card, gridRect, title)
        const content = card.querySelector('.n-card__content')!
        const contentRect = content.getBoundingClientRect()
        for (const child of content.querySelectorAll('.n-form-item, .n-form-item-label, .n-form-item-blank, .n-input-group, .n-input, .n-input-number, .n-select')) {
          checkBounds(child, contentRect, `${title} ${child.classList[0]}`)
        }
        for (const control of content.querySelectorAll('.n-input-number, .n-select, .n-input-group > .n-input')) {
          if (control.getBoundingClientRect().width < 96) problems.push(`${title}: control narrower than 96px`)
        }
        for (const group of content.querySelectorAll('.n-input-group')) {
          const groupRect = group.getBoundingClientRect()
          for (const child of group.children) checkBounds(child, groupRect, `${title} input group child`)
          const button = group.querySelector('.n-button')!
          if (button.getBoundingClientRect().width < 32) problems.push(`${title}: folder button is squeezed`)
        }
        if (title === '数据源与API') {
          const items = content.querySelectorAll('.n-form-item')
          const gap = items[1].getBoundingClientRect().top - items[0].getBoundingClientRect().bottom
          if (gap > 32) problems.push(`API form items have an excessive ${Math.round(gap)}px gap`)
        }
      }

      for (const container of [document.documentElement, document.body, ...document.querySelectorAll('.settings-page, .settings-page .n-layout-scroll-container')]) {
        if (container.clientWidth > 0 && container.scrollWidth > container.clientWidth + tolerance) {
          problems.push(`${container.className || container.tagName}: horizontal overflow ${container.scrollWidth - container.clientWidth}px`)
        }
      }
      if (gridRect.left < -tolerance || gridRect.right > window.innerWidth + tolerance) problems.push('settings grid exceeds viewport')
      return problems
    }), { message: 'Cards, form fields and folder buttons should fit without excessive gaps or horizontal scrolling' }).toEqual([])
  })
}

async function installApiStubs(page: Page) {
  await page.route('**/api/**', async (route) => {
    const pathname = new URL(route.request().url()).pathname
    if (!pathname.startsWith('/api/')) {
      await route.continue()
      return
    }
    let response: unknown = { success: true, data: [] }
    if (pathname.endsWith('/auth/check_status')) {
      response = { status: 'logged_in', user: { id: 'e2e-user', name: 'E2E User', is_admin: true } }
    } else if (pathname.endsWith('/auth/service_status')) {
      response = { authorized: true, url: 'http://emby.invalid' }
    } else if (pathname.endsWith('/config')) {
      response = {
        delay_between_items_sec: 0.5,
        max_actors_to_process: 50,
        max_episode_actors_to_process: 5,
        media_image_archive_enabled: true,
        media_image_archive_path: '/config/media_images/Long.Metadata.Image.Archive.Directory',
        monitor_enabled: true,
        monitor_paths: monitorPaths,
        monitor_exclude_dirs: excludePaths,
        monitor_exclude_refresh_delay: 30,
        monitor_scan_max_tasks: 10,
        monitor_scan_batch_size: 2,
        monitor_extensions: extensions,
        tmdb_image_language_preference: 'zh',
        tmdb_api_base_url: tmdbUrl,
        api_douban_default_cooldown_seconds: 1,
        libraries_to_process: [],
        proxy_native_view_selection: [],
        task_chain_sequence: [],
        task_chain_low_freq_sequence: [],
      }
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(response) })
  })
}
