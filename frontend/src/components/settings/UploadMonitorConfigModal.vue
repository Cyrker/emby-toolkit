<template>
  <n-modal
    v-model:show="visible"
    preset="card"
    title="115 上传监控"
    style="width: 920px; max-width: 95vw;"
    class="custom-modal glass-modal"
  >
    <n-spin :show="loading">
      <n-space vertical :size="16">
        <n-form label-placement="left" label-width="110">
          <n-form-item label="启用上传监控">
            <n-switch v-model:value="config.enabled">
              <template #checked>开启</template>
              <template #unchecked>关闭</template>
            </n-switch>
          </n-form-item>
        </n-form>

        <div class="mapping-list">
          <div v-for="(item, index) in config.mappings" :key="item.id" class="mapping-row">
            <div class="mapping-header">
              <n-space align="center">
                <n-text strong>目录映射 {{ index + 1 }}</n-text>
                <n-switch v-model:value="item.enabled" size="small" />
              </n-space>
              <n-tooltip>
                <template #trigger>
                  <n-button quaternary circle type="error" @click="removeMapping(index)">
                    <template #icon><n-icon :component="TrashIcon" /></template>
                  </n-button>
                </template>
                删除目录映射
              </n-tooltip>
            </div>

            <n-grid cols="1 m:2" responsive="screen" :x-gap="12" :y-gap="10">
              <n-gi>
                <n-form-item label="本地目录" label-placement="top" :show-feedback="false">
                  <n-input-group>
                    <n-input :value="item.local_dir" readonly placeholder="选择本地目录" />
                    <n-button type="primary" ghost @click="emit('select-local', item.id)">
                      <template #icon><n-icon :component="FolderIcon" /></template>
                    </n-button>
                  </n-input-group>
                </n-form-item>
              </n-gi>
              <n-gi>
                <n-form-item label="115 目录" label-placement="top" :show-feedback="false">
                  <n-input-group>
                    <n-input :value="item.target_name || item.target_cid" readonly placeholder="选择 115 目录" />
                    <n-button type="primary" ghost @click="emit('select-remote', item.id, item.target_cid)">
                      <template #icon><n-icon :component="CloudIcon" /></template>
                    </n-button>
                  </n-input-group>
                </n-form-item>
              </n-gi>
            </n-grid>

            <n-form-item label="监控扩展名" label-placement="top" :show-feedback="false">
              <n-select
                v-model:value="item.extensions"
                multiple
                filterable
                tag
                :options="extensionOptions"
                placeholder="选择或输入扩展名"
              />
            </n-form-item>

            <n-form-item label="上传策略" label-placement="top" :show-feedback="false">
              <n-radio-group v-model:value="item.mode" size="small">
                <n-radio-button value="keep">增量双向同步</n-radio-button>
                <n-radio-button value="delete">上传成功后删除</n-radio-button>
              </n-radio-group>
            </n-form-item>
          </div>
        </div>

        <n-button dashed block type="primary" @click="addMapping">
          <template #icon><n-icon :component="AddIcon" /></template>
          添加监控目录
        </n-button>

        <n-space v-if="status" :size="16">
          <n-tag size="small" type="info">等待 {{ status.pending || 0 }}</n-tag>
          <n-tag size="small" type="warning">上传中 {{ status.uploading || 0 }}</n-tag>
          <n-tag size="small" type="error">失败 {{ status.failed || 0 }}</n-tag>
          <n-tag size="small" type="success">已记录 {{ status.completed || 0 }}</n-tag>
        </n-space>

        <div v-if="status?.recent?.length" class="job-list">
          <div v-for="job in status.recent" :key="job.id" class="job-row">
            <div class="job-header">
              <div class="job-name" :title="job.path">{{ fileName(job.path) }}</div>
              <n-tag size="small" :type="statusType(job.status)" :bordered="false">
                {{ statusText(job.status) }}
              </n-tag>
            </div>
            <n-progress
              type="line"
              :percentage="Number(job.progress || 0)"
              :status="job.status === 'failed' ? 'error' : job.progress >= 100 ? 'success' : 'default'"
              :height="10"
              :border-radius="4"
              indicator-placement="inside"
            />
            <div class="job-meta">
              <span>{{ formatBytes(job.uploaded_bytes || 0) }} / {{ formatBytes(job.total_bytes || job.fingerprint?.size || 0) }}</span>
              <span v-if="job.upload_backend">{{ job.upload_backend === 'cookie' ? 'Cookie' : 'OpenAPI' }}</span>
              <span v-if="job.error" class="job-error" :title="job.error">{{ job.error }}</span>
            </div>
          </div>
        </div>
      </n-space>
    </n-spin>

    <template #footer>
      <n-space justify="end">
        <n-button @click="visible = false">取消</n-button>
        <n-button type="primary" :loading="saving" @click="save">保存</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup lang="ts">
// @ts-nocheck -- legacy page logic is transport-migrated; page-family typing follows incrementally.
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import apiClient, { apiPath } from '@/api/client'
import {
  NButton, NForm, NFormItem, NGi, NGrid, NIcon, NInput, NInputGroup,
  NModal, NProgress, NRadioButton, NRadioGroup, NSelect, NSpace, NSpin, NSwitch, NTag,
  NText, NTooltip, useMessage
} from 'naive-ui';
import {
  AddOutline as AddIcon,
  CloudOutline as CloudIcon,
  FolderOutline as FolderIcon,
  TrashOutline as TrashIcon
} from '@vicons/ionicons5';

const props = defineProps({
  extensions: { type: Array, default: () => [] }
});
const emit = defineEmits(['select-local', 'select-remote']);
const message = useMessage();

const visible = ref(false);
const loading = ref(false);
const saving = ref(false);
const status = ref(null);
const config = ref({ enabled: false, mappings: [] });
let statusTimer = null;

const normalizeExtensionList = values => [...new Set((values || []).map(value => {
  const ext = String(value || '').trim().toLowerCase().replaceAll('*', '');
  return ext && !ext.startsWith('.') ? `.${ext}` : ext;
}).filter(ext => ext && ext !== '.strm'))];

const normalizedExtensions = computed(() => normalizeExtensionList(props.extensions));
const imageExtensions = ['.jpg', '.jpeg', '.png', '.heic', '.heif', '.webp', '.gif', '.bmp', '.tif', '.tiff'];
const extensionOptions = computed(() => normalizeExtensionList([
  ...normalizedExtensions.value,
  ...imageExtensions
]).map(ext => ({ label: ext, value: ext })));

const newMapping = () => ({
  id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  enabled: true,
  local_dir: '',
  target_cid: '',
  target_name: '',
  mode: 'keep',
  extensions: [...normalizedExtensions.value]
});

const addMapping = () => config.value.mappings.push(newMapping());
const removeMapping = index => config.value.mappings.splice(index, 1);

const fileName = path => String(path || '').replaceAll('\\', '/').split('/').pop() || path;
const formatBytes = value => {
  const bytes = Number(value || 0);
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
};
const statusText = value => ({ pending: '等待', uploading: '上传中', failed: '重试中' }[value] || value || '等待');
const statusType = value => ({ pending: 'info', uploading: 'warning', failed: 'error' }[value] || 'default');

const refreshStatus = async () => {
  try {
    const response = await apiClient.get(apiPath('api_p115_upload_monitor_config_b79cb47e'));
    status.value = response.data.status || null;
  } catch (error) {
    console.debug('刷新上传状态失败', error);
  }
};

const stopStatusPolling = () => {
  if (statusTimer) {
    clearInterval(statusTimer);
    statusTimer = null;
  }
};

watch(visible, shown => {
  stopStatusPolling();
  if (shown) statusTimer = setInterval(refreshStatus, 2000);
});

onBeforeUnmount(stopStatusPolling);

const open = async () => {
  visible.value = true;
  loading.value = true;
  try {
    const response = await apiClient.get(apiPath('api_p115_upload_monitor_config_b79cb47e'));
    config.value = response.data.data || { enabled: false, mappings: [] };
    status.value = response.data.status || null;
  } catch (error) {
    message.error(error.response?.data?.message || '加载上传监控配置失败');
  } finally {
    loading.value = false;
  }
};

const updateLocalFolder = (id, path) => {
  const item = config.value.mappings.find(mapping => mapping.id === id);
  if (item) item.local_dir = path;
};

const updateTarget = (id, cid, name) => {
  const item = config.value.mappings.find(mapping => mapping.id === id);
  if (item) {
    item.target_cid = cid;
    item.target_name = name;
  }
};

const save = async () => {
  if (config.value.enabled && !config.value.mappings.length) {
    message.warning('至少添加一组目录映射');
    return;
  }
  const incomplete = config.value.mappings.find(item => !item.local_dir || !item.target_cid);
  if (incomplete) {
    message.warning('请完整选择每组本地目录和 115 目录');
    return;
  }
  config.value.mappings.forEach(item => {
    item.extensions = normalizeExtensionList(item.extensions);
  });
  const missingExtensions = config.value.mappings.find(item => item.enabled && !item.extensions.length);
  if (missingExtensions) {
    message.warning('请为每个已启用目录至少配置一个监控扩展名');
    return;
  }
  saving.value = true;
  try {
    const response = await apiClient.post(apiPath('api_p115_upload_monitor_config_b79cb47e'), config.value);
    config.value = response.data.data;
    status.value = response.data.status || null;
    message.success(response.data.message || '上传监控配置已保存');
    visible.value = false;
  } catch (error) {
    message.error(error.response?.data?.message || '保存上传监控配置失败');
  } finally {
    saving.value = false;
  }
};

defineExpose({ open, updateLocalFolder, updateTarget });
</script>

<style scoped>
.mapping-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
  max-height: 52vh;
  overflow-y: auto;
}

.mapping-row {
  border: 1px solid var(--n-divider-color);
  border-radius: 6px;
  padding: 12px;
}

.mapping-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
}

.job-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 240px;
  overflow-y: auto;
}

.job-row {
  min-height: 76px;
  border-top: 1px solid var(--n-divider-color);
  padding-top: 8px;
}

.job-header,
.job-meta {
  display: flex;
  align-items: center;
  gap: 10px;
}

.job-header {
  justify-content: space-between;
  margin-bottom: 6px;
}

.job-name {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13px;
}

.job-meta {
  min-height: 22px;
  margin-top: 4px;
  color: var(--n-text-color-3);
  font-size: 12px;
}

.job-error {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--n-color-error);
}
</style>
