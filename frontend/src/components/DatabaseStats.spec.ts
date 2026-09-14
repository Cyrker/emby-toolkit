import { flushPromises, shallowMount } from '@vue/test-utils';
import { afterEach, describe, expect, it, vi } from 'vitest';
import apiClient from '@/api/client';
import VChart from 'vue-echarts';
import DatabaseStats from './DatabaseStats.vue';

vi.mock('@/api/client', () => ({
  default: { get: vi.fn() },
  apiPath: (key: string) => key,
}));

afterEach(() => vi.restoreAllMocks());

describe('媒体库概览', () => {
  async function loadOverview(library: Record<string, unknown>) {
    vi.mocked(apiClient.get).mockImplementation(async (path) => ({
      data: String(path).includes('stats_library')
        ? { status: 'success', data: library }
        : { status: 'skipped' },
    }) as any);
    const wrapper = shallowMount(DatabaseStats, {
      global: { renderStubDefaultSlot: true },
    });
    await flushPromises();
    return wrapper;
  }

  it('按电影、剧集、分集数量展示占比，不使用缺失的分辨率', async () => {
    const wrapper = await loadOverview({
      movies_in_library: 3,
      series_in_library: 2,
      episodes_in_library: 8,
      resolution_stats: [{ resolution: null, count: 11 }],
    });
    try {
      const option = wrapper.findComponent(VChart).props('option') as any;
      expect(option.series[0].name).toBe('媒体类型');
      expect(option.series[0].data).toEqual([
        { name: '电影', value: 3 },
        { name: '剧集', value: 2 },
        { name: '分集', value: 8 },
      ]);
      expect(option.legend.data).toEqual(['电影', '剧集', '分集']);
      expect(option.tooltip.formatter).toBe('{b}: {c} ({d}%)');
    } finally {
      wrapper.unmount();
    }
  });

  it('没有入库媒体时显示无数据，而不是未知或虚假的类型占比', async () => {
    const wrapper = await loadOverview({
      movies_in_library: 0,
      series_in_library: 0,
      episodes_in_library: 0,
      resolution_stats: [{ resolution: null, count: 1 }],
    });
    try {
      const option = wrapper.findComponent(VChart).props('option') as any;
      expect(option.series[0].data).toEqual([{ value: 1, name: '无数据' }]);
    } finally {
      wrapper.unmount();
    }
  });
});
