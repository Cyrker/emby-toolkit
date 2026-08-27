import { useQuery } from '@tanstack/vue-query'
import { generatedClient } from '@/api/client'
import type { components } from '@/api/generated/schema'

export type AboutInfo = components['schemas']['AboutInfoResponse']

export function useAboutInfoQuery() {
  return useQuery<AboutInfo>({
    queryKey: ['system', 'about-info'],
    queryFn: async () => {
      const result = await generatedClient.GET('/api/system/about_info')
      if (result.error) throw new Error('Unable to load release information')
      return result.data as AboutInfo
    },
    staleTime: 60_000,
  })
}
