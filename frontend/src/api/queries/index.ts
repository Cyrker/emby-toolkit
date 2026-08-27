import { useMutation, useQuery, useQueryClient, type UseMutationOptions, type UseQueryOptions } from '@tanstack/vue-query'
import apiClient, { apiPath, unwrap, type ApiRequestConfig, type ApiResponse } from '@/api/client'

export { useAboutInfoQuery } from './system'

/** Shared query wrapper for endpoints that still return the legacy JSON envelope. */
export function useApiQuery<T>(
  key: readonly unknown[],
  path: keyof typeof import('@/api/endpoints').ENDPOINTS,
  config?: ApiRequestConfig,
  options?: Omit<UseQueryOptions<T>, 'queryKey' | 'queryFn'>,
) {
  return useQuery<T>({
    queryKey: key,
    queryFn: async () => unwrap(await apiClient.get<T>(apiPath(path), config)),
    ...options,
  })
}

export function useApiMutation<TData = unknown, TVariables = unknown>(
  path: keyof typeof import('@/api/endpoints').ENDPOINTS,
  method: 'post' | 'put' | 'patch' | 'delete' = 'post',
  options?: Omit<UseMutationOptions<TData, Error, TVariables>, 'mutationFn'>,
) {
  const queryClient = useQueryClient()
  return useMutation<TData, Error, TVariables>({
    mutationFn: async (variables) => {
      const response: ApiResponse<TData> = method === 'delete'
        ? await apiClient.delete<TData>(apiPath(path), { data: variables })
        : await apiClient[method]<TData>(apiPath(path), variables)
      return unwrap(response)
    },
    ...options,
    onSuccess: async (data, variables, context) => {
      const onSuccess = (options as { onSuccess?: (data: TData, variables: TVariables, context: unknown) => unknown } | undefined)?.onSuccess
      await onSuccess?.(data, variables, context)
      await queryClient.invalidateQueries({ queryKey: [path] })
    },
  })
}
