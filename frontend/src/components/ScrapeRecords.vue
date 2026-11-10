<template>
  <n-card title="刮削记录" size="small" :bordered="false">
    <n-alert type="info" :show-icon="false" style="margin-bottom: 12px">
      展示本地、STRM 入库和手动刮削任务；已入库不等于资料完整。历史任务未做完整性校验的，会单独提示。
    </n-alert>
    <n-space style="margin-bottom: 12px" wrap>
      <n-input v-model:value="search" placeholder="搜索剧名、文件路径或 Emby ID" clearable @keyup.enter="reload" @clear="reload" />
      <n-select v-model:value="status" :options="options" style="width: 150px" @update:value="reload" />
      <n-button :loading="loading" @click="reload">查询 / 刷新</n-button>
    </n-space>
    <n-data-table :columns="columns" :data="records" :loading="loading" :row-key="row => row.id"
      remote :pagination="pagination" size="small" :scroll-x="900" />
    <ReprocessIdentityModal ref="identityModal" @submitted="fetchRecords" />
  </n-card>
</template>

<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import { NAlert, NButton, NCard, NDataTable, NInput, NSelect, NSpace, NTag, useDialog, useMessage, type DataTableColumns } from 'naive-ui'
import apiClient, { apiPath } from '@/api/client'
import ReprocessIdentityModal from './modals/ReprocessIdentityModal.vue'

interface ScrapeRecord {
  id: string; item_id: string | null; name: string; title: string | null; source: string; path: string
  status: string; job_status: string; created_at: string; message: string; completeness_checked: boolean
  selected: number | null; verified: number | null
  incomplete: { item_id: string; name: string; missing: string[] }[]
  failures?: { item_id: string; stage: string; message: string }[]
  retry: { status?: string; message?: string; items?: { available_at: string }[] }
  tmdb_selection?: { tmdb_id: string; media_type: string } | null
}
const records = ref<ScrapeRecord[]>([])
const loading = ref(false)
const search = ref('')
const status = ref('')
const page = ref(1)
const total = ref(0)
const message = useMessage()
const dialog = useDialog()
const identityModal = ref<InstanceType<typeof ReprocessIdentityModal> | null>(null)
const identifying = ref<string | null>(null)
const labels: Record<string, string> = { completed: '刮削完成', incomplete: '资料待补齐', failed: '处理失败', running: '排队 / 处理中', cancelled: '已取消', skipped: '已跳过' }
const options = [{ label: '全部状态', value: '' }, ...Object.entries(labels).map(([value, label]) => ({ value, label }))]
let requestId = 0
const fetchRecords = async () => {
  const current = ++requestId
  loading.value = true
  try {
    const response = await apiClient.get<{ items: ScrapeRecord[]; total: number }>(apiPath('api_emby_scrape_records'), {
      params: { page: page.value, per_page: 25, search: search.value, status: status.value },
    })
    if (current !== requestId) return
    records.value = response.data.items
    total.value = response.data.total
  } catch { if (current === requestId) message.error('获取刮削记录失败') }
  finally { if (current === requestId) loading.value = false }
}
const reload = () => { page.value = 1; void fetchRecords() }
const pagination = computed(() => ({ page: page.value, pageSize: 25, itemCount: total.value,
  onChange: (value: number) => { page.value = value; void fetchRecords() },
}))
const reprocess = async (row: ScrapeRecord) => {
  if (identifying.value) return
  identifying.value = row.id
  try {
    if (await identityModal.value?.inspect(row.item_id!, true, row.id)) return
    dialog.warning({
      title: '补齐资料', content: `补齐“${row.name}”的缺失资料；整剧或单季记录会检查其季集。已有完整缓存会保留，不移动文件，不重建媒体流。`,
      positiveText: '确认', negativeText: '取消',
      onPositiveClick: async () => {
        try {
          await apiClient.post(apiPath('api_actions_reprocess_item_param_1d7580a7', [row.item_id!]), { only_missing: true })
          message.success('已提交重新刮削任务'); await fetchRecords()
        } catch { message.error('提交失败，请确认当前任务已结束') }
      },
    })
  } finally { identifying.value = null }
}
const columns: DataTableColumns<ScrapeRecord> = [
  { title: '状态', key: 'status', width: 130, render: row => h(NTag, {
    size: 'small', bordered: false, type: row.status === 'completed' ? 'success' : row.status === 'failed' ? 'error' : 'warning',
  }, { default: () => labels[row.status] || row.status }) },
  { title: '媒体 / 处理详情', key: 'name', minWidth: 300, render: row => h('div', [
    h('div', { style: 'font-weight: 500; overflow-wrap: anywhere' }, row.name),
    h('small', {}, `${row.title || '尚无媒体缓存'} · ${row.source} · Emby ${row.item_id || '未知'}`),
    !row.completeness_checked ? h('div', { style: 'font-size: 12px; color: #a97925' }, '历史任务未校验资料完整性；待补齐标记参考当前缓存') : null,
    h('details', { style: 'font-size: 12px; margin-top: 4px' }, [
      h('summary', { style: 'cursor: pointer' }, '查看详情'),
      h('div', { style: 'overflow-wrap: anywhere' }, row.path || '任务未保存文件路径'),
      h('div', {}, row.message),
      row.tmdb_selection ? h('div', {}, `本次人工选择：TMDb ${row.tmdb_selection.tmdb_id} (${row.tmdb_selection.media_type})`) : null,
      h('div', {}, `范围 ${row.selected ?? '未知'} 项，Emby 回读确认 ${row.verified ?? '未知'} 项；任务状态：${row.job_status}`),
      ...row.incomplete.map(item => h('div', {}, `${item.name || item.item_id}：缺少 ${item.missing.join('、')}`)),
      ...(row.failures || []).map(item => h('div', {}, `Emby ${item.item_id} · ${item.stage}：${item.message}`)),
      row.retry.status === 'scheduled' ? h('div', {}, `已安排补抓：${row.retry.items?.map(item => new Date(item.available_at).toLocaleString('zh-CN')).join('、')}`) : null,
      row.retry.message ? h('div', {}, row.retry.message) : null,
    ]),
  ]) },
  { title: '开始时间', key: 'created_at', width: 170, render: row => new Date(row.created_at).toLocaleString('zh-CN', { hour12: false }) },
  { title: '操作', key: 'action', width: 110, render: row => h(NButton, {
    size: 'small', loading: identifying.value === row.id,
    disabled: !row.item_id || row.status === 'running' || !!identifying.value, onClick: () => reprocess(row),
  }, { default: () => '补齐资料' }) },
]
onMounted(fetchRecords)
</script>
