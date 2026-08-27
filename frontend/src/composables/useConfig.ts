// src/composables/useConfig.ts
import { ref } from 'vue'
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import apiClient, { apiPath, type ApiError } from '@/api/client'
import { cloneConfigSnapshot, configUpdatesSince } from './configDiff.ts'

type ConfigSnapshot = Record<string, any>
const CONFIG_KEY = ['config'] as const

// Keep the legacy singleton surface while Vue Query owns request caching and
// invalidation for every component that consumes configuration.
let hasFetched = false
let lastFetchedConfig: ConfigSnapshot | null = null
const configModel = ref<ConfigSnapshot | null>(null)
const loadingConfig = ref(false)
const savingConfig = ref(false)
const configError = ref<string | null>(null)

export function useConfig() {
  const queryClient = useQueryClient()
  const configQuery = useQuery<ConfigSnapshot>({
    queryKey: CONFIG_KEY,
    queryFn: async () => (await apiClient.get<ConfigSnapshot>(apiPath('api_config_3c5fcdb0'))).data,
    enabled: false,
    staleTime: 30_000,
  })
  const saveMutation = useMutation({
    mutationFn: (payload: ConfigSnapshot) => apiClient.post(apiPath('api_config_3c5fcdb0'), payload),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: CONFIG_KEY }),
  })

  const fetchConfigData = async () => {
    if (loadingConfig.value || hasFetched) return
    loadingConfig.value = true
    configError.value = null
    try {
      const result = await configQuery.refetch()
      if (result.data !== undefined) {
        configModel.value = result.data
        lastFetchedConfig = cloneConfigSnapshot(result.data)
        hasFetched = true
      }
    } catch (error) {
      const apiError = error as ApiError<{ error?: string }>
      configError.value = apiError.response?.data?.error || '无法连接到后端或解析配置。'
      hasFetched = false
    } finally {
      loadingConfig.value = false
    }
  }

  const handleSaveConfig = async (configToSave?: ConfigSnapshot) => {
    const desired = configToSave || configModel.value
    if (!desired) {
      configError.value = '配置数据为空，无法保存。'
      return false
    }
    savingConfig.value = true
    configError.value = null
    try {
      const payload = configUpdatesSince(desired, lastFetchedConfig)
      if (Object.keys(payload).length > 0) await saveMutation.mutateAsync(payload)
      const refreshed = await configQuery.refetch()
      configModel.value = refreshed.data ?? desired
      lastFetchedConfig = cloneConfigSnapshot(configModel.value)
      hasFetched = true
      return true
    } catch (error) {
      const apiError = error as ApiError<{ error?: string }>
      configError.value = apiError.response?.data?.error || (error instanceof Error ? error.message : '配置保存失败。')
      return false
    } finally {
      savingConfig.value = false
    }
  }

  if (!hasFetched && !loadingConfig.value) void fetchConfigData()

  return {
    configModel,
    loadingConfig,
    savingConfig,
    configError,
    fetchConfigData,
    handleSaveConfig,
  }
}
