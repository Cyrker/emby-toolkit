import { flushPromises, shallowMount } from '@vue/test-utils';
import { describe, expect, it, vi } from 'vitest';
import { NSwitch } from 'naive-ui';
import apiClient from '@/api/client';
import ReviewList from './ReviewList.vue';

vi.mock('vue-router', () => ({ useRouter: () => ({ push: vi.fn() }) }));
vi.mock('naive-ui', async (original) => ({
  ...await original<typeof import('naive-ui')>(),
  useMessage: () => ({ success: vi.fn(), error: vi.fn() }),
}));
vi.mock('@/api/client', () => ({
  default: {
    get: vi.fn(async () => ({ data: { data: [], items: [], total_items: 0 } })),
    post: vi.fn(async () => ({ data: {} })),
  },
  apiPath: (key: string) => key,
}));

describe('手动处理模式', () => {
  it('默认重新处理，开关同时作用于单项和批量提交', async () => {
    const wrapper = shallowMount(ReviewList, {
      props: { taskStatus: { is_running: false } },
      global: { renderStubDefaultSlot: true, stubs: {
        NLayout: { template: '<div><slot /></div>' },
        ReprocessIdentityModal: { template: '<div />', methods: { inspect: async () => false } },
      } },
    });
    try {
      await flushPromises();
      const state = (wrapper.vm as any).$.setupState;
      await state.handleReprocessItem({ item_id: 'fixture', item_name: '测试剧', reason: 'N/A' });
      expect(vi.mocked(apiClient.post).mock.lastCall?.[1]).toMatchObject({ only_missing: false });
      wrapper.findComponent(NSwitch).vm.$emit('update:value', true);
      await flushPromises();
      expect(wrapper.text()).toContain('优先复用完整缓存');
      await state.handleReprocessItem({ item_id: 'fixture', item_name: '测试剧', reason: 'N/A' });
      expect(vi.mocked(apiClient.post).mock.lastCall?.[1]).toMatchObject({ only_missing: true });
      await state.reprocessAllReviewItems();
      expect(vi.mocked(apiClient.post).mock.lastCall?.[1]).toMatchObject({ only_missing: true });
    } finally {
      wrapper.unmount();
    }
  });
});
