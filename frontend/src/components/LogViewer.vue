<!-- src/components/LogViewer.vue -->
<template>
  <n-drawer
    :show="show"
    :width="900"
    @update:show="$emit('update:show', $event)"
    placement="right"
    resizable
  >
    <n-drawer-content
      title="历史日志查看器"
      closable
      :native-scrollbar="false"
      body-content-style="height: 100%; display: flex; flex-direction: column; padding: 0;"
    >
      <div class="drawer-inner-wrapper">

        <!-- 顶部控制区 -->
        <n-space vertical class="header-section">
          <n-input-group>
            <n-input
              v-model:value="searchQuery"
              placeholder="搜索统一应用日志、任务记录和 MoviePilot 通知..."
              clearable
              @keyup.enter="executeSearch"
              :disabled="isLoading"
            />
            <n-button type="primary" @click="executeSearch" :loading="isSearching">
              搜索
            </n-button>
          </n-input-group>

          <n-radio-group v-model:value="searchMode" name="search-mode-radio">
            <n-radio-button value="filter" :disabled="isLoading">
              匹配记录
            </n-radio-button>
            <n-radio-button value="context" :disabled="isLoading">
              记录及上下文
            </n-radio-button>
          </n-radio-group>
        </n-space>

        <n-divider style="margin: 10px 0;" />

        <n-spin :show="isLoading" class="flex-spin">

          <!-- 场景 1: 定位模式 (HTML iframe) -->
          <div v-if="isSearchMode && searchMode === 'context'" class="view-container">
             <div class="toolbar">
               <n-button @click="clearSearch" size="tiny" secondary>
                 <template #icon><n-icon :component="ArrowBackOutline" /></template>
                 返回文件浏览
               </n-button>
               <span class="tip">显示匹配记录及相邻上下文，任务名称和 ID 用于区分并发流程</span>
             </div>

            <div v-if="htmlContent" class="iframe-wrapper">
              <LogTimeline :logs="htmlContent" height="calc(100vh - 300px)" />
            </div>
            <n-empty v-else description="未找到匹配的完整处理流程。" style="margin-top: 50px;" />
          </div>

          <!-- 场景 2: 筛选模式 (JSON) - 保持不变，用于快速查看原始行 -->
          <div v-else-if="isSearchMode && searchMode === 'filter'" class="view-container">
            <n-button @click="clearSearch" size="tiny" secondary style="margin-bottom: 5px;">
              <template #icon><n-icon :component="ArrowBackOutline" /></template>
              返回文件浏览
            </n-button>

            <LogTimeline v-if="hasSearchResults" :logs="searchResults.map(result => result.content)" height="calc(100vh - 300px)" />
            <n-empty v-else description="未找到匹配的日志记录。" style="margin-top: 50px;" />
          </div>

          <!-- ★★★ 场景 3: 文件浏览 (默认) - 改为 HTML iframe ★★★ -->
          <div v-else class="view-container">
            <div class="toolbar">
              <n-select
                v-model:value="selectedFile"
                placeholder="请选择统一应用日志或轮转归档"
                :options="fileOptions"
                :loading="isLoadingFiles"
                @update:value="fetchLogContent"
                size="small"
                style="width: 100%; max-width: 560px;"
              />
              <n-button size="small" secondary :disabled="!selectedFile" @click="fetchLogContent(selectedFile)">刷新日志</n-button>
              <span class="tip">按完整记录倒序，每页 50 条；HTTP 明细按需展开，长错误原文分段查看。</span>
            </div>

            <!-- 这里复用 iframe-wrapper -->
            <div v-if="logContent" class="iframe-wrapper">
              <LogTimeline :logs="logContent" height="calc(100vh - 320px)" />
            </div>
            <n-empty v-else description="无数据" style="margin-top: 50px;" />
          </div>

          <template #description>{{ loadingText }}</template>
        </n-spin>
      </div>
    </n-drawer-content>
  </n-drawer>
</template>

<script setup lang="ts">
// @ts-nocheck -- legacy page logic is transport-migrated; page-family typing follows incrementally.
import { ref, watch, computed } from 'vue';
import apiClient, { apiPath } from '@/api/client'
import LogTimeline from './LogTimeline.vue'
import {
  useMessage, NDrawer, NDrawerContent, NSelect, NSpace, NSpin,
  NInput, NInputGroup, NButton, NDivider, NEmpty, NIcon,
  NRadioGroup, NRadioButton
} from 'naive-ui';
import { ArrowBackOutline } from '@vicons/ionicons5';

const props = defineProps({ show: { type: Boolean, default: false } });
const emit = defineEmits(['update:show']);

const message = useMessage();
const isLoadingFiles = ref(false);
const isLoadingContent = ref(false);
const isSearching = ref(false);
const logFiles = ref([]);
const selectedFile = ref(null);
const logContent = ref('');
const searchQuery = ref('');
const searchResults = ref([]);
const htmlContent = ref(''); // 上下文原始记录，由 LogTimeline 统一渲染
const isSearchMode = ref(false);
const searchMode = ref('context');

const isLoading = computed(() => isLoadingFiles.value || isLoadingContent.value || isSearching.value);
const hasSearchResults = computed(() => searchResults.value.length > 0);
const fileOptions = computed(() => logFiles.value.map(file => ({ label: file, value: file })));
const loadingText = computed(() => {
  if (isLoadingFiles.value) return '正在获取文件列表...';
  if (isLoadingContent.value) return '正在渲染日志视图...';
  if (isSearching.value) return '正在搜索...';
  return '';
});

const fetchLogFiles = async () => {
  isLoadingFiles.value = true;
  try {
    const response = await apiClient.get(apiPath('api_logs_list_99fba90c'));
    logFiles.value = response.data;
    if (!isSearchMode.value && logFiles.value.length > 0 && !selectedFile.value) {
      selectedFile.value = logFiles.value[0];
      await fetchLogContent(selectedFile.value);
    }
  } catch (error) {
    message.error('获取日志文件列表失败！');
  } finally {
    isLoadingFiles.value = false;
  }
};

const fetchLogContent = async (filename) => {
  if (!filename) return;
  isLoadingContent.value = true;
  logContent.value = ''; // 清空旧内容
  try {
    const response = await apiClient.get(apiPath('api_logs_view_109de3a6'), {
      params: {
        filename,
        format: 'text'
      },
      responseType: 'text'
    });
    logContent.value = response.data;
  } catch (error) {
    message.error(`加载日志 ${filename} 失败！`);
  } finally {
    isLoadingContent.value = false;
  }
};

const executeSearch = async () => {
  if (!searchQuery.value.trim()) {
    message.warning('请输入搜索关键词。');
    return;
  }
  isSearching.value = true;
  isSearchMode.value = true;
  searchResults.value = [];
  htmlContent.value = '';

  try {
    if (searchMode.value === 'context') {
      const response = await apiClient.get(apiPath('api_logs_search_context_321408d5'), {
        params: { q: searchQuery.value, format: 'json' },
      });
      if (Array.isArray(response.data) && response.data.length) {
        htmlContent.value = response.data.map(block => block.lines.join('\n')).join('\n');
      } else {
        htmlContent.value = '';
      }
    } else {
      const response = await apiClient.get(apiPath('api_logs_search_b03d5d44'), { params: { q: searchQuery.value } });
      searchResults.value = response.data;
    }
  } catch (error) {
    console.error(error);
    message.error('搜索失败，请检查后台日志。');
  } finally {
    isSearching.value = false;
  }
};

const clearSearch = () => {
  isSearchMode.value = false;
  searchQuery.value = '';
  searchResults.value = [];
  htmlContent.value = '';
  if (selectedFile.value && !logContent.value) {
    fetchLogContent(selectedFile.value);
  }
};

watch(() => props.show, (newVal) => {
  if (newVal) {
    fetchLogFiles();
  } else {
    clearSearch();
    selectedFile.value = null;
    logFiles.value = [];
    logContent.value = '';
  }
});
</script>

<style scoped>
.drawer-inner-wrapper {
  height: 100%;
  display: flex;
  flex-direction: column;
  padding: 15px;
  box-sizing: border-box;
}

.header-section { flex-shrink: 0; }

.flex-spin {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-height: 0;
}

:deep(.n-spin-content) {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.view-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.toolbar {
  flex-shrink: 0;
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
  flex-wrap: wrap;
  gap: 8px;
}

.tip { font-size: 12px; color: #999; }

.iframe-wrapper {
  flex: 1;
  border: 1px solid #333;
  border-radius: 4px;
  overflow: hidden;
  padding: 12px;
  height: 100%;
  display: block;
}

iframe { flex: 1; display: block; }

.log-text-area {
  flex: 1;
  background-color: #282c34;
  font-family: 'Courier New', Courier, monospace;
  font-size: 13px;
  padding: 10px 15px;
  border-radius: 4px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}

.log-line { line-height: 1.6; padding: 1px 0; color: #abb2bf; }
.log-line.info { color: #98c379; }
.log-line.warning { color: #e5c07b; }
.log-line.error, .log-line.critical { color: #e06c75; }
.log-line.debug { color: #56b6c2; }
.log-line.raw { color: #95a5a6; font-style: italic; }
.timestamp { color: #61afef; margin-right: 1em; }
.level { font-weight: bold; margin-right: 1em; text-transform: uppercase; }
.source { display: block; color: #999; font-size: 11px; }
</style>
