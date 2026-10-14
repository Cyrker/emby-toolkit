<template>
  <details @toggle="onToggle">
    <summary>查看来源与原始详情</summary>
    <template v-if="open">
      <div class="log-source">{{ source || '任务日志' }}</div>
      <n-pagination v-if="pages > 1" v-model:page="page" :page-count="pages" :page-slot="5" size="small">
        <template #prefix>原文分段（每段最多 {{ chunkSize }} 字符）</template>
      </n-pagination>
      <pre>{{ message.slice((page - 1) * chunkSize, page * chunkSize) }}</pre>
    </template>
  </details>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { NPagination } from 'naive-ui'

const props = defineProps<{ message: string; source: string }>()
const chunkSize = 4000
const open = ref(false)
const page = ref(1)
const pages = computed(() => Math.max(1, Math.ceil(props.message.length / chunkSize)))
const onToggle = (event: Event) => {
  open.value = (event.target as HTMLDetailsElement).open
  if (!open.value) page.value = 1
}
</script>

<style scoped>
summary { cursor: pointer; font-size: 11px; opacity: .8; padding: 2px 0; }
.log-source { font-size: 12px; opacity: .65; overflow-wrap: anywhere; }
pre { white-space: pre-wrap; overflow-wrap: anywhere; max-height: 320px; overflow: auto; font-size: 12px; line-height: 1.5; margin: 6px 0; padding: 6px 8px; background: rgba(128,128,128,.08); border-radius: 4px; }
</style>
