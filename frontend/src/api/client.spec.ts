import { describe, expect, it, vi } from 'vitest'
import apiClient, { ApiError, apiPath } from './client'

describe('API adapter', () => {
  it('resolves endpoint templates without exposing path construction to pages', () => {
    expect(apiPath('api_actor_subscriptions_param_9db6d3cd', ['sub-1'])).toBe('/api/actor-subscriptions/sub-1')
  })

  it('serializes query parameters and preserves the legacy response envelope', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: true, data: { id: 1 } }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    }))
    vi.stubGlobal('fetch', fetchMock)

    const response = await apiClient.get('/api/auth/check_status', { params: { page: 2, tags: ['a', 'b'] } })
    expect(response.data).toEqual({ success: true, data: { id: 1 } })
    expect(fetchMock).toHaveBeenCalledWith(
      expect.objectContaining({ pathname: '/api/auth/check_status', search: '?page=2&tags=a&tags=b' }),
      expect.objectContaining({ credentials: 'include', method: 'GET' }),
    )
  })

  it('maps non-2xx responses to an error with the response payload', async () => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(() => Promise.resolve(new Response(JSON.stringify({ message: 'denied' }), {
      status: 403,
      headers: { 'content-type': 'application/json' },
    }))))

    await expect(apiClient.get('/api/admin/users')).rejects.toBeInstanceOf(ApiError)
    try {
      await apiClient.get('/api/admin/users')
    } catch (error) {
      expect((error as ApiError).response?.status).toBe(403)
      expect((error as ApiError).response?.data).toEqual({ message: 'denied' })
    }
  })

  it('lets the browser assign multipart boundaries for uploads', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: true }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    }))
    vi.stubGlobal('fetch', fetchMock)
    const form = new FormData()
    form.set('file', new Blob(['fixture']), 'fixture.json')

    await apiClient.upload('/api/database/import', form, { headers: { 'Content-Type': 'multipart/form-data' } })
    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(new Headers(init.headers).has('content-type')).toBe(false)
  })
})
