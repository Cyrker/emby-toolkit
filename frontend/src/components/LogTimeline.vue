<template>
  <section class="log-timeline" aria-label="可视化日志">
    <div class="log-filters">
      <n-select v-model:value="category" :options="categories" aria-label="日志分类" style="width: 160px" size="small" />
      <n-select v-model:value="level" :options="levels" aria-label="日志级别" style="width: 135px" size="small" />
      <span class="log-count">业务 {{ business.length }} 条 · 异常 {{ errors }} 条 · 已折叠明细 {{ technical.length }} 条 · 每页 {{ pageSize }} 条</span>
    </div>
    <div ref="scroll" class="log-scroll" :style="{ maxHeight: height }">
      <n-empty v-if="!business.length" description="暂无匹配的业务日志；请求明细可在下方展开" size="small" />
      <article v-for="entry in businessPage" :key="entry.id" class="log-event" :class="entry.level.toLowerCase()">
        <header><n-tag size="small" :type="severity(entry.level)" :bordered="false">{{ entry.category }}</n-tag>
          <time>{{ displayTime(entry.timestamp) }}</time><span class="log-level">{{ entry.level }}</span></header>
        <div class="log-summary">{{ entry.summary }}</div>
        <LogDetails :message="entry.message" :source="entry.source" />
      </article>
      <n-pagination v-if="business.length > pageSize" v-model:page="page" :item-count="business.length" :page-size="pageSize" :page-slot="5" size="small" class="log-pagination" />
      <details v-if="technical.length" :open="technicalOpen" @toggle="toggleTechnical" class="technical-logs">
        <summary>HTTP 请求与系统明细（{{ technical.length }} 条，点击展开）</summary>
        <template v-if="technicalOpen">
        <article v-for="entry in technicalPage" :key="entry.id" class="log-event">
          <header><n-tag size="small">{{ entry.category }}</n-tag><time>{{ displayTime(entry.timestamp) }}</time></header>
          <div class="log-summary">{{ entry.summary }}</div>
          <LogDetails :message="entry.message" :source="entry.source" />
        </article>
        <n-pagination v-if="technical.length > pageSize" v-model:page="technicalPageNumber" :item-count="technical.length" :page-size="pageSize" :page-slot="5" size="small" class="log-pagination" />
        </template>
      </details>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { NSelect, NTag, NEmpty, NPagination } from 'naive-ui'
import { parseTaskLogs } from '@/utils/taskLogs'
import LogDetails from './LogDetails.vue'

const props = withDefaults(defineProps<{ logs: unknown; height?: string }>(), { height: '60vh' })
const category = ref('')
const level = ref('')
const scroll = ref<HTMLElement>()
const pageSize = 50
const page = ref(1)
const technicalPageNumber = ref(1)
const technicalOpen = ref(false)
const toggleTechnical = (event: Event) => { technicalOpen.value = (event.target as HTMLDetailsElement).open }
const categories = ['', '整理与入库', '刮削与媒体', '任务', '播放', '异常'].map(value => ({ label: value || '全部业务', value }))
const levels = ['', 'ERROR', 'WARNING', 'INFO', 'DEBUG'].map(value => ({ label: value || '全部级别', value }))
const entries = computed(() => parseTaskLogs(props.logs).map((entry, id) => ({ ...entry, id })))
const filtered = computed(() => entries.value.filter(row => !level.value || row.level === level.value || level.value === 'ERROR' && row.level === 'CRITICAL'))
const business = computed(() => filtered.value.filter(row => !row.technical && (!category.value || row.category === category.value)))
const technical = computed(() => filtered.value.filter(row => row.technical))
const businessPage = computed(() => business.value.slice((page.value - 1) * pageSize, page.value * pageSize))
const technicalPage = computed(() => technical.value.slice((technicalPageNumber.value - 1) * pageSize, technicalPageNumber.value * pageSize))
const errors = computed(() => filtered.value.filter(row => row.category === '异常').length)
const severity = (value: string) => ['ERROR', 'CRITICAL'].includes(value) ? 'error' : value === 'WARNING' ? 'warning' : 'info'
const displayTime = (value: string) => {
  if (!value) return '未标注时间'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString('zh-CN', { hour12: false })
}
watch([category, level], () => {
  page.value = 1
  technicalPageNumber.value = 1
})
// 轮询只更新日志；仅在当前页已不存在时收敛页码，不打断用户阅读。
watch([() => business.value.length, () => technical.value.length], ([businessCount, technicalCount]) => {
  page.value = Math.min(page.value, Math.max(1, Math.ceil(businessCount / pageSize)))
  technicalPageNumber.value = Math.min(technicalPageNumber.value, Math.max(1, Math.ceil(technicalCount / pageSize)))
})
watch(page, () => { if (scroll.value) scroll.value.scrollTop = 0 })
</script>

<style scoped>
.log-timeline { min-width: 0; }
.log-filters { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 14px; }
.log-count, time, .log-level, .log-source { font-size: 12px; opacity: .65; }
.log-scroll { overflow: auto; padding: 2px 8px 8px 2px; }
.log-pagination { margin: 12px 0; }
.log-event { box-sizing: border-box; max-width: 720px; border: 1px solid rgba(128,128,128,.18); border-left: 3px solid #70a9cb; border-radius: 5px; padding: 7px 10px; margin-bottom: 6px; background: rgba(128,128,128,.045); }
.log-event.error, .log-event.critical { border-left-color: #d45c64; }
.log-event.warning { border-left-color: #d2a657; }
header { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.log-summary { margin: 4px 0; font-size: 13px; white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.45; }
summary { cursor: pointer; font-size: 12px; opacity: .8; padding: 5px 0; }
.technical-logs > summary { box-sizing: border-box; max-width: 720px; padding: 8px 10px; border: 1px dashed rgba(128,128,128,.3); border-radius: 5px; margin-bottom: 6px; }
@media (max-width: 600px) { .log-event { padding: 6px 8px; } .log-count { width: 100%; } }
</style>
