import { flushPromises, shallowMount } from '@vue/test-utils';
import { describe, expect, it, vi } from 'vitest';
import apiClient from '@/api/client';
import UserCenterPage from './UserCenterPage.vue';

vi.mock('../stores/auth', () => ({
  useAuthStore: () => ({ username: '测试用户', isAdmin: false }),
}));
vi.mock('naive-ui', async (original) => ({
  ...await original<typeof import('naive-ui')>(),
  useMessage: () => ({ success: vi.fn(), error: vi.fn() }),
}));
vi.mock('@/api/client', () => ({
  default: {
    get: vi.fn(async (key: string) => {
      if (key === 'api_portal_account_info_c9e154df') {
        return { data: { id: 'viewer', name: '测试用户', status: 'active', registration_date: '2026-01-01' } };
      }
      if (key === 'api_portal_subscription_history_3366ba81') {
        return { data: { items: [], total_records: 0 } };
      }
      if (key === 'api_portal_playback_report_days_30_media_type_param_2eab9c1f') {
        return { data: { personal: { history_list: [], total_count: 0, total_minutes: 0 } } };
      }
      return { data: { total: 0, completed: 0, processing: 0, pending: 0, failed: 0 } };
    }),
    post: vi.fn(),
  },
  apiPath: (key: string) => key,
}));

describe('用户中心已退役的播放账号贡献入口', () => {
  it.each([1280, 390])('宽度 %i 下保留正常查询且不请求 Cookie 或奖励接口', async (width) => {
    vi.stubGlobal('innerWidth', width);
    const wrapper = shallowMount(UserCenterPage, {
      global: {
        renderStubDefaultSlot: true,
        stubs: {
          CloudUploadOutline: true,
          NScrollbar: true,
          NTooltip: { template: '<div><slot name="trigger" /><slot /></div>' },
        },
      },
    });
    try {
      await flushPromises();
      expect(vi.mocked(apiClient.get).mock.calls.map(([key]) => key)).toEqual([
        'api_portal_account_info_c9e154df',
        'api_portal_subscription_stats_2836fabf',
        'api_portal_subscription_history_3366ba81',
        'api_portal_playback_report_days_30_media_type_param_2eab9c1f',
      ]);
      expect(wrapper.find('.user-cookie-section').exists()).toBe(false);
      expect(wrapper.text()).not.toMatch(/115 Cookie|五折|二折|累计奖励/);
      expect((wrapper.vm as any).$.setupState.accountInfo.id).toBe('viewer');
      expect(apiClient.post).not.toHaveBeenCalled();
    } finally {
      wrapper.unmount();
      vi.unstubAllGlobals();
    }
  });
});
