<template>
  <!-- 1. 使用 n-layout 包裹，适配暗色/亮色主题背景 -->
  <n-layout class="setup-layout">
    <div class="setup-container">
      
      <!-- 2. 加上 dashboard-card 类实现辉光，同时保留 setup-card 用于控制尺寸 -->
      <n-card class="dashboard-card setup-card" :bordered="false" size="large">
        
        <!-- 头部区域：Logo 和 标题 -->
        <div class="setup-header">
          <img src="/brand-icon.svg" alt="embytool" class="setup-logo" />
          <h2 class="setup-title">embytool</h2>
          <p class="setup-subtitle">初始化配置向导</p>
        </div>

        <div class="intro-text">
          <p>请使用 Emby 管理员账号完成服务授权。</p>
          <p class="tip">embytool 只保存授权 Token，不会保存账号密码。</p>
        </div>

        <n-form ref="formRef" :model="formModel" :rules="rules" size="large">
          <n-form-item label="服务器地址 (URL)" path="url">
            <n-input
              v-model:value="formModel.url" 
              placeholder="例如: http://192.168.1.10:8096" 
              @keydown.enter="handleSave"
            />
          </n-form-item>
          
          <n-form-item label="管理员用户名" path="username">
            <n-input
              v-model:value="formModel.username"
              placeholder="Emby 管理员用户名"
              @keydown.enter="handleSave"
            />
          </n-form-item>

          <n-form-item label="管理员密码" path="password">
            <n-input
              v-model:value="formModel.password"
              type="password" 
              show-password-on="mousedown"
              placeholder="无密码账号可留空"
              @keydown.enter="handleSave"
            />
          </n-form-item>

          <n-space vertical style="margin-top: 20px;">
            <n-button type="primary" block @click="handleSave" :loading="loading" size="large" class="setup-btn">
              测试并保存
            </n-button>
          </n-space>
        </n-form>
      </n-card>
    </div>
  </n-layout>
</template>

<script setup lang="ts">
// @ts-nocheck -- legacy page logic is transport-migrated; page-family typing follows incrementally.
import { ref } from 'vue';
import { useRouter } from 'vue-router';
import { NLayout, NCard, NForm, NFormItem, NInput, NButton, NSpace, useMessage } from 'naive-ui';
import apiClient, { apiPath } from '@/api/client'
import { useAuthStore } from '../stores/auth';

const router = useRouter();
const message = useMessage();
const authStore = useAuthStore();
const loading = ref(false);
const formRef = ref(null);

const formModel = ref({
  url: '',
  username: '',
  password: ''
});

const rules = {
  url: { required: true, message: '请输入服务器地址', trigger: 'blur' },
  username: { required: true, message: '请输入管理员用户名', trigger: 'blur' }
};

async function handleSave() {
  try {
    await formRef.value?.validate();
    loading.value = true;
    
    const response = await apiClient.post(apiPath('api_auth_setup_0d7c12f3'), formModel.value);
    
    if (response.data.status === 'ok') {
      await authStore.checkAuthStatus();
      const pluginInstall = response.data.plugin_install;
      const libraryConfig = response.data.library_config;
      if ((pluginInstall && !pluginInstall.ok) || (libraryConfig && !libraryConfig.ok)) {
        message.warning(response.data.message || 'Emby 服务授权成功，但 embytool Bridge 插件自动安装失败');
      } else {
        message.success(response.data.message || 'Emby 服务授权成功');
      }
      router.replace({ name: 'DatabaseStats' });
    }
  } catch (error) {
    const msg = error.response?.data?.message || '授权失败，请检查服务器地址和管理员账号';
    message.error(msg);
  } finally {
    loading.value = false;
  }
}
</script>

<style scoped>
/* 布局容器，撑满全屏 */
.setup-layout {
  height: 100vh;
  width: 100vw;
}

.setup-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  width: 100%;
  padding: 20px;
  box-sizing: border-box;
}

.setup-card {
  width: 100%;
  max-width: 600px; /* 保持较宽的宽度，因为 URL 可能很长 */
  border-radius: 12px;
  
  /* 🔥🔥🔥 关键：强制重置高度，防止 dashboard-card 把它拉成巨人 🔥🔥🔥 */
  height: auto !important;
  min-height: auto !important;
  flex: none !important;
}

/* 头部样式，保持和 Login 一致 */
.setup-header {
  text-align: center;
  margin-bottom: 24px;
}

.setup-logo {
  height: 60px;
  margin-bottom: 10px;
}

.setup-title {
  font-size: 24px;
  font-weight: 600;
  margin: 0 0 8px 0;
}

.setup-subtitle {
  font-size: 14px;
  color: #999;
  margin: 0;
}

.intro-text {
  text-align: center;
  margin-bottom: 30px;
  color: #666;
  font-size: 15px;
}

.tip {
  font-size: 13px;
  color: #999;
  margin-top: 5px;
}

.setup-btn {
  font-weight: bold;
}
</style>
