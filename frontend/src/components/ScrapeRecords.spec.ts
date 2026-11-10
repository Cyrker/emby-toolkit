import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { NDataTable, NSelect } from 'naive-ui'
import { h } from 'vue'
import { NConfigProvider } from 'naive-ui'
import apiClient from '@/api/client'
import ScrapeRecords from './ScrapeRecords.vue'

const { confirm } = vi.hoisted(() => ({ confirm: vi.fn() }))
vi.mock('naive-ui', async original => ({
  ...await original<typeof import('naive-ui')>(),
  useMessage: () => ({ success: vi.fn(), error: vi.fn() }),
  useDialog: () => ({ warning: confirm }),
}))
vi.mock('@/api/client', () => ({ default: { get: vi.fn(), post: vi.fn(async () => ({ data: {} })) }, apiPath: (key: string) => key }))
const item = { id: 'job-3', item_id: '3', name: '主动入库: 测试剧 S01E03', title: '第 3 集', source: '本地入库',
  path: '/fixture/S01E03.mp4', status: 'incomplete', job_status: 'succeeded', completeness_checked: false,
  created_at: '2026-09-17T12:13:00Z', selected: 1, verified: 1, message: '历史任务', incomplete: [], retry: {},
}
beforeEach(() => { vi.clearAllMocks(); vi.mocked(apiClient.get).mockResolvedValue({ data: { items: [item], total: 60 } } as any) })

describe('刮削记录', () => {
  it('shows historical incomplete metadata separately from the successful job and pages on the server', async () => {
    const wrapper = mount({ render: () => h(NConfigProvider, {}, { default: () => h(ScrapeRecords) }) })
    await flushPromises()
    expect(wrapper.text()).toContain('资料待补齐')
    expect(wrapper.text()).toContain('本地入库')
    expect(wrapper.text()).toContain('历史任务未校验')
    expect(wrapper.text()).not.toContain('删除记录')
    expect(apiClient.get).toHaveBeenLastCalledWith('api_emby_scrape_records', { params: { page: 1, per_page: 25, search: '', status: '' } })
    ;(wrapper.findComponent(NDataTable).props('pagination') as any).onChange(2)
    await flushPromises()
    expect(vi.mocked(apiClient.get).mock.lastCall?.[1]?.params).toMatchObject({ page: 2, per_page: 25 })
    wrapper.findComponent(NSelect).vm.$emit('update:value', 'incomplete')
    await flushPromises()
    expect(vi.mocked(apiClient.get).mock.lastCall?.[1]?.params).toMatchObject({ page: 1, status: 'incomplete' })
    wrapper.unmount()
  })
  it('requires confirmation and uses missing-only mode for manual completion', async () => {
    const wrapper = mount({ render: () => h(NConfigProvider, {}, { default: () => h(ScrapeRecords) }) })
    await flushPromises()
    const button = wrapper.findAll('button').find(b => b.text() === '补齐资料')!
    await button.trigger('click')
    await flushPromises()
    expect(apiClient.post).not.toHaveBeenCalled()
    await confirm.mock.lastCall?.[0].onPositiveClick()
    expect(apiClient.post).toHaveBeenCalledWith('api_actions_reprocess_item_param_1d7580a7', { only_missing: true })
    wrapper.unmount()
  })
})
