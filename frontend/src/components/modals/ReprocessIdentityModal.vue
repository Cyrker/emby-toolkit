<template>
  <n-modal v-model:show="show" preset="card" title="选择 TMDb 作品" class="identity-modal"
    style="width: min(900px, calc(100vw - 32px))" :mask-closable="!submitting" :close-on-esc="!submitting"
    :closable="!submitting" @after-leave="cancelSearch">
    <template v-if="context">
      <n-alert type="info" :show-icon="false">
        确认后将使用所选 TMDb 身份重新刮削，并在 Emby 回读确认后更新任务结果。取消不写入任何内容。
      </n-alert>
      <div class="target-details">
        <strong>{{ context.target.name }}</strong>
        <div>{{ context.target.media_type === 'movie' ? '电影' : '剧集' }} · {{ context.target.year || '年份未知' }} · Emby {{ itemId }}</div>
        <div v-if="context.target.root_item_id !== itemId">所属剧集：{{ context.target.root_name }} · Emby {{ context.target.root_item_id }}</div>
        <div v-if="context.target.path" class="target-path">{{ context.target.path }}</div>
      </div>
      <div class="search-fields">
        <n-input v-model:value="keyword" aria-label="作品搜索关键词" placeholder="作品名称" :disabled="submitting" />
        <n-input-number v-model:value="year" aria-label="搜索年份" placeholder="年份（可选）" clearable
          :min="1800" :max="2200" :show-button="false" :disabled="submitting" />
        <n-button :loading="loading" :disabled="submitting" @click="search(1)">搜索</n-button>
      </div>
      <details class="manual-entry">
        <summary>直接输入 TMDb ID</summary>
        <n-space style="margin-top: 8px">
          <n-input v-model:value="manualId" aria-label="手动 TMDb ID" placeholder="当前媒体类型的 TMDb ID"
            :disabled="submitting" @update:value="invalidate" />
          <n-button :disabled="!manualId.trim() || submitting" :loading="loading" @click="search(1, true)">查询 ID</n-button>
        </n-space>
      </details>
      <n-alert v-if="error" type="error" style="margin: 12px 0" role="alert">{{ error }}</n-alert>
      <div v-if="loading" class="search-state" role="status"><n-spin size="small" /> 正在搜索 TMDb…</div>
      <n-empty v-else-if="!error && !candidates.length" description="没有找到匹配作品，请修改关键词、年份或输入 TMDb ID" />
      <fieldset v-if="!loading && candidates.length" class="candidate-list" :disabled="submitting">
        <legend class="sr-only">选择作品，不会自动选择搜索结果</legend>
        <label v-for="candidate in candidates" :key="candidate.tmdb_id" class="candidate"
          :class="{ selected: selected?.tmdb_id === candidate.tmdb_id }">
          <input type="radio" name="reprocess-tmdb" :value="candidate.tmdb_id"
            :checked="selected?.tmdb_id === candidate.tmdb_id" @change="select(candidate)" />
          <img v-if="candidate.poster_path" :src="`https://image.tmdb.org/t/p/w185${candidate.poster_path}`" alt="" loading="lazy" />
          <div v-else class="poster-placeholder">暂无海报</div>
          <div class="candidate-info">
            <strong>{{ candidate.title }}</strong>
            <div>{{ candidate.original_title }}</div>
            <small>{{ candidate.year || '年份未知' }} · {{ candidate.media_type === 'movie' ? '电影' : '剧集' }} · TMDb {{ candidate.tmdb_id }}</small>
            <p>{{ candidate.overview || '暂无简介' }}</p>
          </div>
        </label>
      </fieldset>
      <n-pagination v-if="totalPages > 1" :page="page" :page-count="totalPages" :page-slot="5"
        :disabled="loading || submitting" style="margin-top: 12px" @update:page="value => search(value)" />
      <n-space justify="end" style="margin-top: 20px">
        <n-button :disabled="submitting" @click="show = false">取消</n-button>
        <n-button type="primary" :disabled="!selected || loading" :loading="submitting" @click="submit">确认并重新处理</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from 'vue'
import { NAlert, NButton, NEmpty, NInput, NInputNumber, NModal, NPagination, NSpace, NSpin, useMessage } from 'naive-ui'
import apiClient, { apiPath } from '@/api/client'

interface Candidate { tmdb_id: string; media_type: 'movie' | 'tv'; title: string; original_title: string; year: string; overview: string; poster_path: string | null }
interface Context {
  requires_selection: boolean; source_job_id: string; snapshot: string
  target: { item_id: string; root_item_id: string; name: string; root_name: string; media_type: 'movie' | 'tv'; year: number | null; path: string }
  candidates: Candidate[]; page: number; total_pages: number; query?: string
}
const emit = defineEmits<{ submitted: [] }>()
const message = useMessage()
const show = ref(false)
const context = ref<Context | null>(null)
const itemId = ref('')
const onlyMissing = ref(true)
const keyword = ref('')
const year = ref<number | null>(null)
const manualId = ref('')
const candidates = ref<Candidate[]>([])
const selected = ref<Candidate | null>(null)
const page = ref(1)
const totalPages = ref(0)
const loading = ref(false)
const submitting = ref(false)
const error = ref('')
let sequence = 0
let controller: AbortController | undefined
let timer: ReturnType<typeof setTimeout> | undefined
let submissionKey = ''
const errorText = (err: any) => err.response?.data?.error || err.response?.data?.message || '查询或提交失败，请重试。'

function cancelSearch() {
  ++sequence
  controller?.abort()
  clearTimeout(timer)
  loading.value = false
}
function invalidate() {
  cancelSearch()
  selected.value = null
  candidates.value = []
  totalPages.value = 0
  error.value = ''
  submissionKey = ''
}
watch([keyword, year], () => {
  if (!show.value) return
  invalidate()
  timer = setTimeout(() => { void search(1) }, 300)
}, { flush: 'sync' })
watch(show, value => { if (!value) cancelSearch() })
onBeforeUnmount(cancelSearch)

async function inspect(id: string, missing: boolean, jobId?: string): Promise<boolean> {
  cancelSearch()
  const current = sequence
  controller = new AbortController()
  try {
    const response = await apiClient.get<Context>(apiPath('api_reprocess_identification', [encodeURIComponent(id)]), {
      params: { job_id: jobId, context_only: true }, signal: controller.signal,
    })
    if (current !== sequence) return true
    if (!response.data.requires_selection) return false
    show.value = false
    itemId.value = id
    onlyMissing.value = missing
    context.value = response.data
    keyword.value = response.data.query ?? response.data.target.root_name
    year.value = response.data.target.year
    manualId.value = ''
    candidates.value = []
    selected.value = null
    page.value = response.data.page
    totalPages.value = response.data.total_pages
    error.value = ''
    submissionKey = ''
    show.value = true
    await search(1)
    return true
  } catch (err: any) {
    if (current === sequence && err.code !== 'ERR_CANCELED') message.error(errorText(err))
    return true
  }
}

async function search(nextPage = 1, manual = false) {
  if (!context.value || submitting.value) return
  invalidate()
  const current = sequence
  loading.value = true
  controller = new AbortController()
  try {
    const response = await apiClient.get<Context>(apiPath('api_reprocess_identification', [encodeURIComponent(itemId.value)]), {
      params: { job_id: context.value.source_job_id, query: keyword.value, year: year.value,
        page: nextPage, manual_id: manual ? manualId.value.trim() : undefined }, signal: controller.signal,
    })
    if (current !== sequence) return
    if (!response.data.requires_selection || response.data.snapshot !== context.value.snapshot) {
      error.value = '目标身份已变化，请关闭窗口后重新确认。'
      return
    }
    candidates.value = response.data.candidates
    page.value = response.data.page
    totalPages.value = response.data.total_pages
  } catch (err: any) {
    if (current === sequence && err.code !== 'ERR_CANCELED') error.value = errorText(err)
  } finally { if (current === sequence) loading.value = false }
}
function select(candidate: Candidate) {
  selected.value = candidate
  submissionKey = Array.from(crypto.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, '0')).join('')
  error.value = ''
}
async function submit() {
  if (!context.value || !selected.value || submitting.value || loading.value) return
  submitting.value = true
  error.value = ''
  try {
    await apiClient.post(apiPath('api_actions_reprocess_item_param_1d7580a7', [encodeURIComponent(itemId.value)]), {
      only_missing: onlyMissing.value, source_job_id: context.value.source_job_id,
      tmdb_selection: { tmdb_id: selected.value.tmdb_id, media_type: selected.value.media_type, snapshot: context.value.snapshot },
    }, { headers: { 'Idempotency-Key': submissionKey } })
    message.success('已提交选片重处理任务；请在处理记录中查看最终结果。')
    show.value = false
    emit('submitted')
  } catch (err) { error.value = errorText(err) }
  finally { submitting.value = false }
}
defineExpose({ inspect })
</script>

<style scoped>
.target-details { margin: 16px 0; overflow-wrap: anywhere; }
.search-fields { display: grid; grid-template-columns: minmax(0, 1fr) 150px auto; gap: 8px; }
.manual-entry { margin: 12px 0; }
.manual-entry summary { cursor: pointer; }
.search-state { padding: 24px; text-align: center; }
.candidate-list { border: 0; padding: 0; margin: 12px 0 0; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; max-height: 48vh; overflow-y: auto; }
.candidate { display: flex; gap: 8px; padding: 12px; border: 1px solid var(--n-border-color); border-radius: 6px; cursor: pointer; align-items: flex-start; }
.candidate.selected { border-color: #18a058; box-shadow: inset 0 0 0 1px #18a058; }
.candidate img, .poster-placeholder { width: 64px; flex: 0 0 64px; min-height: 96px; object-fit: cover; border-radius: 3px; }
.poster-placeholder { display: grid; place-items: center; font-size: 12px; opacity: .65; }
.candidate-info { min-width: 0; overflow-wrap: anywhere; }
.candidate-info p { margin: 8px 0 0; font-size: 12px; display: -webkit-box; -webkit-line-clamp: 5; -webkit-box-orient: vertical; overflow: hidden; }
.sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); }
@media (max-width: 600px) {
  .search-fields { grid-template-columns: minmax(0, 1fr) 110px; }
  .search-fields > :last-child { grid-column: 1 / -1; }
  .candidate-list { grid-template-columns: 1fr; }
}
</style>
