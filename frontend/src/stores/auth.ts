// src/stores/auth.ts
import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import apiClient, { apiPath } from '@/api/client'
import type { components } from '@/api/generated/schema'

type AuthUser = components['schemas']['AuthUser'] & Record<string, unknown>
type AuthStatus = components['schemas']['AuthCheckResponse']

export const useAuthStore = defineStore('auth', () => {
  const isLoggedIn = ref(false)
  const user = ref<Partial<AuthUser>>({})
  const systemStatus = ref<AuthStatus['status'] | 'unknown'>('unknown')

  const username = computed(() => {
    return String(user.value?.name || user.value?.username || '未登录')
  });

  const isAdmin = computed(() => Boolean(user.value?.is_admin))

  // 为了兼容旧代码，加一个 userType
  const userType = computed(() => String(user.value?.user_type || 'emby_user'))

  async function checkAuthStatus() {
    try {
      const response = await apiClient.get<AuthStatus>(apiPath('api_auth_check_status_0ff38d6c'))
      const status = response.data.status
      systemStatus.value = status

      if (status === 'logged_in') {
        isLoggedIn.value = true;
        user.value = (response.data.user || {}) as Partial<AuthUser>
      } else {
        isLoggedIn.value = false;
        user.value = {}
        // 这里可以抛出特定错误供路由守卫捕获，或者直接返回状态
        if (status === 'setup_required') {
          throw new Error('SETUP_REQUIRED')
        }
      }
      return status
    } catch (error) {
      isLoggedIn.value = false
      throw error
    }
  }

  async function login(credentials: Record<string, unknown>) {
    const response = await apiClient.post<{ user?: AuthUser }>(apiPath('api_auth_login_cb91f294'), credentials)
    isLoggedIn.value = true
    // 确保把后端返回的 user 对象完整存进去
    user.value = (response.data.user || {}) as Partial<AuthUser>
  }

  async function logout() {
    await apiClient.post(apiPath('api_auth_logout_f6fcebb6'))
    isLoggedIn.value = false
    user.value = {}
    systemStatus.value = 'login_required'
  }

  return {
    isLoggedIn,
    user,
    username, // 导出修复后的计算属性
    isAdmin,
    userType,
    systemStatus,
    checkAuthStatus,
    login,
    logout
  };
});
