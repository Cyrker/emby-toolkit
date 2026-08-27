<!-- src/AppContent.vue -->
<template>
  <!-- 1. 如果需要后台布局，显示 MainLayout -->
  <MainLayout 
    v-if="showMainLayout"
    :is-dark="isDarkTheme" 
    :selected-theme="selectedTheme"
    :task-status="backgroundTaskStatus"
    @update:is-dark="handleModeChange"
    @update:selected-theme="handleThemeChange"
    @edit-custom-theme="openThemeEditor"
  />

  <n-modal
    v-model:show="serviceAuthorizationVisible"
    preset="card"
    title="Emby 服务授权"
    :style="{ width: 'min(480px, calc(100vw - 24px))' }"
    :mask-closable="serviceAuthorized"
    :close-on-esc="serviceAuthorized"
    :closable="serviceAuthorized"
  >
    <n-alert type="warning" :show-icon="true" style="margin-bottom: 16px;">
      后台任务需要 Emby 管理员授权。账号密码仅用于本次认证，不会保存。
    </n-alert>
    <n-form ref="serviceAuthorizationFormRef" :model="serviceAuthorizationForm" :rules="serviceAuthorizationRules">
      <n-form-item label="Emby URL" path="url">
        <n-input v-model:value="serviceAuthorizationForm.url" placeholder="http://localhost:8096" />
      </n-form-item>
      <n-form-item label="管理员用户名" path="username">
        <n-input v-model:value="serviceAuthorizationForm.username" autocomplete="username" />
      </n-form-item>
      <n-form-item label="管理员密码" path="password">
        <n-input
          v-model:value="serviceAuthorizationForm.password"
          type="password"
          show-password-on="mousedown"
          autocomplete="current-password"
          placeholder="无密码账号可留空"
          @keydown.enter="submitServiceAuthorization"
        />
      </n-form-item>
    </n-form>
    <template #footer>
      <n-space justify="end">
        <n-button v-if="serviceAuthorized" @click="serviceAuthorizationVisible = false">取消</n-button>
        <n-button type="primary" :loading="serviceAuthorizationSubmitting" @click="submitServiceAuthorization">
          授权
        </n-button>
      </n-space>
    </template>
  </n-modal>
  
  <!-- ★★★ 核心修复点 ★★★ -->
  <!-- 2. 否则 (即公共页面)，用 .fullscreen-container 包裹 router-view -->
  <div v-if="!showMainLayout" class="fullscreen-container">
    <router-view />
  </div>

  <!-- 3. 主题编辑器和加载动画的逻辑保持不变 -->
  <ThemeEditor
    v-if="showThemeEditor"
    :show="showThemeEditor"
    :initial-theme="themeForEditor"
    :is-dark="isDarkTheme"
    @update:show="handleEditorClose"
    @save="handleSaveCustomTheme"
    @update:preview="handlePreviewUpdate"
    @delete-custom-theme="handleDeleteCustomTheme"
  />
  
  <div v-if="!isReady" class="fullscreen-container">
    <n-spin size="large" />
  </div>
</template>

<script setup lang="ts">
// @ts-nocheck -- legacy page logic is transport-migrated; page-family typing follows incrementally.
import { ref, watch, onBeforeUnmount, onMounted, computed, nextTick } from 'vue';
import { useRoute } from 'vue-router';
import {
  useDialog, NSpin, useMessage, NModal, NForm, NFormItem,
  NInput, NButton, NAlert, NSpace
} from 'naive-ui';
import { useAuthStore } from './stores/auth';
import MainLayout from './MainLayout.vue';
import ThemeEditor from './components/ThemeEditor.vue';
import { themes } from './theme.ts';
import apiClient, { apiPath } from '@/api/client'
import { cloneDeep } from 'lodash-es';

// --- 路由和认证 ---
const route = useRoute();
const authStore = useAuthStore();

// --- 布局决策 ---
const showMainLayout = computed(() => {
  return !route.meta.public;
});

// --- 下面的所有 script setup 内容都保持原样 ---
const dialog = useDialog();
const message = useMessage();

const isDarkTheme = ref(localStorage.getItem('isDark') === 'true');
const selectedTheme = ref(localStorage.getItem('user-theme') || 'default');
const userCustomTheme = ref(null);
const showThemeEditor = ref(false);
const previewTheme = ref(null);
const isReady = ref(false);
const newThemeTemplate = ref(null);
const isOpeningEditor = ref(false);

const backgroundTaskStatus = ref({ is_running: false, current_action: '空闲' });
const TASK_STATUS_EVENT = 'etk-task-status';
let statusTimerId = null;
let statusAbortController = null;
let statusPollingActive = false;
let statusFetchInFlight = false;
const pendingSystemUpdateReload = ref(false);
const waitingForBackendRecovery = ref(false);
const serviceAuthorized = ref(true);
const serviceAuthorizationVisible = ref(false);
const serviceAuthorizationSubmitting = ref(false);
const serviceAuthorizationFormRef = ref(null);
const serviceAuthorizationForm = ref({ url: '', username: '', password: '' });
const serviceAuthorizationRules = {
  url: { required: true, message: '请输入 Emby URL', trigger: 'blur' },
  username: { required: true, message: '请输入管理员用户名', trigger: 'blur' },
};

const app = document.getElementById('app');

const applyTheme = (themeKey, isDark) => {
  const root = document.documentElement;
  const themeMode = isDark ? 'dark' : 'light';
  let themeConfig;

  if (themeKey === 'custom') {
    if (!userCustomTheme.value || !userCustomTheme.value[themeMode]) {
      selectedTheme.value = 'default';
      localStorage.setItem('user-theme', 'default');
      themeConfig = themes.default[themeMode];
    } else {
      themeConfig = userCustomTheme.value[themeMode];
    }
  } else {
    if (!themes[themeKey]) {
      selectedTheme.value = 'default';
      localStorage.setItem('user-theme', 'default');
      themeConfig = themes.default[themeMode];
    } else {
      themeConfig = themes[themeKey][themeMode];
    }
  }

  app.dispatchEvent(new CustomEvent('update-naive-theme', { detail: themeConfig.naive }));
  for (const key in themeConfig.custom) {
    root.style.setProperty(key, themeConfig.custom[key]);
  }
  root.classList.remove('dark', 'light');
  root.classList.add(isDark ? 'dark' : 'light');
};

const themeForEditor = computed(() => {
    if (userCustomTheme.value) {
        return userCustomTheme.value;
    }
    return newThemeTemplate.value;
});

const openThemeEditor = () => { 
  if (userCustomTheme.value) {
    newThemeTemplate.value = null;
  }
  showThemeEditor.value = true; 
};

const handleThemeChange = (newTheme) => {
  if (newTheme !== 'custom' || (newTheme === 'custom' && userCustomTheme.value)) {
    selectedTheme.value = newTheme;
    localStorage.setItem('user-theme', newTheme);
    return;
  }

  if (newTheme === 'custom' && !userCustomTheme.value) {
    const currentTemplateSource = themes[selectedTheme.value] || themes.default;
    newThemeTemplate.value = {
        name: '自定义',
        light: cloneDeep(currentTemplateSource.light),
        dark: cloneDeep(currentTemplateSource.dark)
    };

    dialog.info({
      title: '欢迎来到主题设计工坊',
      content: `准备基于你当前的【${currentTemplateSource.name}】主题进行创作吗？`,
      positiveText: '立即设计',
      onPositiveClick: () => {
        isOpeningEditor.value = true;
        selectedTheme.value = 'custom';
        localStorage.setItem('user-theme', 'custom');
        openThemeEditor();
        nextTick(() => {
          isOpeningEditor.value = false;
        });
      }
    });
  }
};

const handleModeChange = (isDark) => {
  isDarkTheme.value = isDark;
  localStorage.setItem('isDark', String(isDark));
  app.dispatchEvent(new CustomEvent('update-dark-mode', { detail: isDark }));
};

const handleSaveCustomTheme = async (newThemeConfigForCurrentMode) => {
  try {
    const fullCustomTheme = cloneDeep(themeForEditor.value);
    if (isDarkTheme.value) {
        fullCustomTheme.dark = newThemeConfigForCurrentMode;
    } else {
        fullCustomTheme.light = newThemeConfigForCurrentMode;
    }
    await apiClient.post(apiPath('api_config_custom_theme_00a0b2df'), fullCustomTheme);
    userCustomTheme.value = fullCustomTheme;
    selectedTheme.value = 'custom';
    localStorage.setItem('user-theme', 'custom');
    showThemeEditor.value = false;
    previewTheme.value = null;
    newThemeTemplate.value = null;
    message.success('专属主题已保存！');
  } catch (error) { 
    message.error('保存自定义主题失败');
    console.error('保存自定义主题失败:', error); 
  }
};

const handleDeleteCustomTheme = async () => {
  try {
    await apiClient.delete(apiPath('api_config_custom_theme_00a0b2df'));
    userCustomTheme.value = null;
    selectedTheme.value = 'default';
    localStorage.setItem('user-theme', 'default');
    showThemeEditor.value = false;
    previewTheme.value = null;
    newThemeTemplate.value = null;
    message.success('自定义主题已删除，已切换回默认主题。');
  } catch (error) {
    message.error('删除自定义主题失败');
    console.error('删除自定义主题失败:', error);
  }
};

const handleEditorClose = (show) => {
    if (!show) {
        showThemeEditor.value = false;
        previewTheme.value = null;
        newThemeTemplate.value = null;
        applyTheme(selectedTheme.value, isDarkTheme.value);
    }
};

const handlePreviewUpdate = (themeConfig) => {
    previewTheme.value = themeConfig;
    app.dispatchEvent(new CustomEvent('update-naive-theme', { detail: themeConfig.naive }));
    const root = document.documentElement;
    for (const key in themeConfig.custom) {
        root.style.setProperty(key, themeConfig.custom[key]);
    }
};

watch([isDarkTheme, selectedTheme], ([isDark, themeKey]) => {
  if (isOpeningEditor.value) return; 
  if (!previewTheme.value) {
      applyTheme(themeKey, isDark);
  }
}, { deep: true });

const emitTaskStatus = (status) => {
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(TASK_STATUS_EVENT, { detail: status }));
  }
};

const stopStatusPolling = () => {
  statusPollingActive = false;
  if (statusTimerId) {
    clearTimeout(statusTimerId);
    statusTimerId = null;
  }
  if (statusAbortController) {
    statusAbortController.abort();
    statusAbortController = null;
  }
  statusFetchInFlight = false;
};

const scheduleStatusPoll = (delay = 2000) => {
  if (!statusPollingActive) return;
  if (statusTimerId) clearTimeout(statusTimerId);
  statusTimerId = setTimeout(fetchStatus, delay);
};

const fetchStatus = async () => {
  if (!statusPollingActive || statusFetchInFlight) return;
  statusFetchInFlight = true;
  statusAbortController = new AbortController();
  try {
    const response = await apiClient.get(apiPath('api_status_7fbc29f0'), {
      timeout: 8000,
      signal: statusAbortController.signal,
    });
    backgroundTaskStatus.value = response.data;
    emitTaskStatus(response.data);
    if (waitingForBackendRecovery.value) {
      waitingForBackendRecovery.value = false;
      if (pendingSystemUpdateReload.value) {
        window.location.reload();
        return;
      }
    }
    const statusMessage = String(response.data?.message || '');
    if (statusMessage.includes('系统正在重启')) {
      pendingSystemUpdateReload.value = true;
    }
  } catch (error) {
    if (error?.code !== 'ERR_CANCELED') {
      console.error('获取状态失败:', error);
    }
  } finally {
    statusFetchInFlight = false;
    statusAbortController = null;
    if (pendingSystemUpdateReload.value) {
      waitingForBackendRecovery.value = true;
    }
    scheduleStatusPoll(pendingSystemUpdateReload.value ? 2000 : 2500);
  }
};

const startStatusPolling = () => {
  if (statusPollingActive) return;
  statusPollingActive = true;
  fetchStatus();
};

const checkServiceAuthorization = async () => {
  if (!authStore.isLoggedIn) return;
  try {
    const response = await apiClient.get(apiPath('api_auth_service_status_460ed5e7'));
    serviceAuthorized.value = !!response.data.authorized;
    serviceAuthorizationForm.value.url = response.data.url || '';
    if (!serviceAuthorized.value) serviceAuthorizationVisible.value = true;
  } catch (error) {
    console.error('检查 Emby 服务授权失败:', error);
  }
};

const openServiceAuthorization = () => {
  serviceAuthorizationForm.value.username = '';
  serviceAuthorizationForm.value.password = '';
  serviceAuthorizationVisible.value = true;
};

const submitServiceAuthorization = async () => {
  try {
    await serviceAuthorizationFormRef.value?.validate();
  } catch {
    return;
  }
  serviceAuthorizationSubmitting.value = true;
  try {
    const response = await apiClient.post(apiPath('api_auth_reauthorize_935831ee'), serviceAuthorizationForm.value);
    const authorization = response.data.authorization || {};
    serviceAuthorized.value = true;
    serviceAuthorizationVisible.value = false;
    serviceAuthorizationForm.value.password = '';
    const pluginInstall = response.data.plugin_install;
    const libraryConfig = response.data.library_config;
    if ((pluginInstall && !pluginInstall.ok) || (libraryConfig && !libraryConfig.ok)) {
      message.warning(response.data.message || 'Emby 服务授权成功，但 ETK 插件自动安装失败');
    } else {
      message.success(response.data.message || 'Emby 服务授权成功');
    }
    window.dispatchEvent(new CustomEvent('etk-emby-authorization-updated', { detail: authorization }));
  } catch (error) {
    message.error(error.response?.data?.message || 'Emby 服务授权失败');
  } finally {
    serviceAuthorizationSubmitting.value = false;
  }
};

watch(() => authStore.isLoggedIn, (isLoggedIn) => {
  if (isLoggedIn) {
    startStatusPolling();
    checkServiceAuthorization();
  } else {
    stopStatusPolling();
    pendingSystemUpdateReload.value = false;
    waitingForBackendRecovery.value = false;
  }
}, { immediate: true });

onMounted(async () => {
  window.addEventListener('etk-open-emby-authorization', openServiceAuthorization);
  try {
    const response = await apiClient.get(apiPath('api_config_3c5fcdb0'));
    if (response.data.custom_theme && Object.keys(response.data.custom_theme).length > 0) {
      userCustomTheme.value = response.data.custom_theme;
    }
  } catch (error) {
    console.error("加载初始配置失败:", error);
  } finally {
    if (selectedTheme.value === 'custom' && !userCustomTheme.value) {
        selectedTheme.value = 'default';
        localStorage.setItem('user-theme', 'default');
    }
    applyTheme(selectedTheme.value, isDarkTheme.value);
    isReady.value = true;
  }
});

onBeforeUnmount(() => {
  stopStatusPolling();
  window.removeEventListener('etk-open-emby-authorization', openServiceAuthorization);
});
</script>
