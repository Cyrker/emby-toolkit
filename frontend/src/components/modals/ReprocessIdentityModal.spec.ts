import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { NConfigProvider, NInput, NInputNumber } from 'naive-ui'
import { h } from 'vue'
import apiClient from '@/api/client'
import ReprocessIdentityModal from './ReprocessIdentityModal.vue'

vi.mock('naive-ui', async original => ({
  ...await original<typeof import('naive-ui')>(), useMessage: () => ({ success: vi.fn(), error: vi.fn() }),
}))
vi.mock('@/api/client', () => ({
  default: { get: vi.fn(), post: vi.fn() }, apiPath: (key: string) => key,
}))
const candidate = (id: string) => ({ tmdb_id: id, media_type: 'movie', title: `同名作品 ${id}`, original_title: `Original ${id}`, year: '2020', overview: '作品简介', poster_path: null })
const context = {
  requires_selection: true, source_job_id: 'source-job', snapshot: 'a'.repeat(64),
  target: { item_id: '123', root_item_id: '123', name: '同名作品', root_name: '同名作品', media_type: 'movie', year: 2020, path: '' },
  candidates: [candidate('1'), candidate('2')], page: 1, total_pages: 1,
}
beforeEach(() => {
  vi.clearAllMocks()
  vi.mocked(apiClient.get).mockResolvedValue({ data: context } as any)
  vi.mocked(apiClient.post).mockResolvedValue({ data: { job_id: 'retry' } } as any)
})
function setup() {
  const wrapper = mount({ render: () => h(NConfigProvider, {}, { default: () => h(ReprocessIdentityModal) }) }, { attachTo: document.body })
  const modal = wrapper.findComponent(ReprocessIdentityModal)
  const state = (modal.vm as any).$.setupState
  const destroy = () => { wrapper.unmount(); document.body.innerHTML = '' }
  return { wrapper, modal, state, destroy }
}

describe('重处理选片', () => {
  it('首次搜索失败仍打开窗口，显示明确错误并允许重试', async () => {
    const f = setup()
    try {
      vi.mocked(apiClient.get).mockResolvedValueOnce({ data: context } as any)
        .mockRejectedValueOnce({ response: { data: { error: '首次搜索失败' } } })
      await f.modal.vm.inspect('123', true)
      expect(f.state.show).toBe(true)
      expect(f.state.error).toBe('首次搜索失败')
      expect(f.state.selected).toBeNull()
      await f.state.search()
      expect(f.state.candidates).toHaveLength(2)
      expect(apiClient.post).not.toHaveBeenCalled()
    } finally { f.destroy() }
  })
  it('自动查询但不默认选中；取消不写入也不提交', async () => {
    const f = setup()
    try {
      expect(await f.modal.vm.inspect('123', true, 'source-job')).toBe(true)
      await flushPromises()
      expect(document.body.textContent).toContain('Original 2')
      expect(document.querySelectorAll('input[type="radio"]:checked')).toHaveLength(0)
      const confirm = [...document.querySelectorAll('button')].find(b => b.textContent?.includes('确认并重新处理'))!
      expect(confirm.disabled).toBe(true)
      f.state.show = false
      await flushPromises()
      expect(apiClient.post).not.toHaveBeenCalled()
    } finally { f.destroy() }
  })

  it('明确选择进入同一次重试请求，重复点击只提交一次', async () => {
    const f = setup()
    try {
      await f.modal.vm.inspect('123', true)
      f.state.select(context.candidates[1])
      let finish!: (value: any) => void
      vi.mocked(apiClient.post).mockImplementation(() => new Promise(resolve => { finish = resolve }) as any)
      const pending = f.state.submit()
      await f.state.submit()
      expect(apiClient.post).toHaveBeenCalledTimes(1)
      expect(vi.mocked(apiClient.post).mock.lastCall?.[1]).toEqual({
        only_missing: true, source_job_id: 'source-job',
        tmdb_selection: { tmdb_id: '2', media_type: 'movie', snapshot: context.snapshot },
      })
      expect((vi.mocked(apiClient.post).mock.lastCall?.[2]?.headers as any)['Idempotency-Key']).toHaveLength(32)
      finish({ data: {} })
      await pending
      expect(f.modal.emitted('submitted')).toHaveLength(1)
    } finally { f.destroy() }
  })

  it('提交失败保留选择；网络重试使用相同幂等标识', async () => {
    const f = setup()
    try {
      await f.modal.vm.inspect('123', true)
      f.state.select(context.candidates[1])
      vi.mocked(apiClient.post).mockRejectedValueOnce({ response: { data: { error: '提交失败' } } })
      await f.state.submit()
      expect(f.state.selected.tmdb_id).toBe('2')
      expect(f.state.show).toBe(true)
      const first = vi.mocked(apiClient.post).mock.lastCall?.[2]?.headers
      await f.state.submit()
      expect(vi.mocked(apiClient.post).mock.lastCall?.[2]?.headers).toEqual(first)
    } finally { f.destroy() }
  })

  it('修改关键词立即清空选择，迟到的旧结果不能覆盖新结果', async () => {
    const f = setup()
    try {
      await f.modal.vm.inspect('123', true)
      f.state.select(context.candidates[0])
      let finish!: (value: any) => void
      vi.mocked(apiClient.get).mockImplementationOnce(() => new Promise(resolve => { finish = resolve }) as any)
      const oldSearch = f.state.search()
      f.state.keyword = '新关键词'
      expect(f.state.selected).toBeNull()
      vi.mocked(apiClient.get).mockResolvedValueOnce({ data: { ...context, candidates: [candidate('3')] } } as any)
      await f.state.search()
      finish({ data: context })
      await oldSearch
      expect(f.state.candidates.map((row: any) => row.tmdb_id)).toEqual(['3'])
      expect(f.state.selected).toBeNull()
    } finally { f.destroy() }
  })

  it('空结果和请求失败分开显示，重试成功也不自动选择唯一结果', async () => {
    const f = setup()
    try {
      await f.modal.vm.inspect('123', true)
      vi.mocked(apiClient.get).mockResolvedValueOnce({ data: { ...context, candidates: [] } } as any)
      await f.state.search()
      await flushPromises()
      expect(document.body.textContent).toContain('没有找到匹配作品')
      vi.mocked(apiClient.get).mockRejectedValueOnce({ response: { data: { error: 'TMDb 请求失败' } } })
      await f.state.search()
      expect(f.state.error).toBe('TMDb 请求失败')
      vi.mocked(apiClient.get).mockResolvedValueOnce({ data: { ...context, candidates: [candidate('3')] } } as any)
      await f.state.search()
      expect(f.state.error).toBe('')
      expect(f.state.selected).toBeNull()
      expect(apiClient.post).not.toHaveBeenCalled()
    } finally { f.destroy() }
  })

  it('手动 ID 先查询详情，年份变更清空旧选择，身份变化必须重新确认', async () => {
    const f = setup()
    try {
      await f.modal.vm.inspect('123', true)
      await flushPromises()
      f.state.manualId = '1234'
      await f.state.search(1, true)
      expect(vi.mocked(apiClient.get).mock.lastCall?.[1]?.params).toMatchObject({ manual_id: '1234' })
      f.state.select(context.candidates[0])
      f.wrapper.findComponent(NInputNumber).vm.$emit('update:value', 2021)
      expect(f.state.selected).toBeNull()
      vi.mocked(apiClient.get).mockResolvedValueOnce({ data: { ...context, snapshot: 'b'.repeat(64) } } as any)
      await f.state.search()
      expect(f.state.error).toContain('身份已变化')
      expect(apiClient.post).not.toHaveBeenCalled()
      expect(f.wrapper.findComponent(NInput).exists()).toBe(true)
    } finally { f.destroy() }
  })
})
