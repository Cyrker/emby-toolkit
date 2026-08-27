import createClient from 'openapi-fetch'
import type { paths } from './generated/schema'
import { ENDPOINTS } from './endpoints'

/**
 * Response shape kept compatible with the legacy UI while all transport
 * details live behind this adapter.  Callers should use `data` or `unwrap`
 * instead of reaching for a concrete HTTP client.
 */
export interface ApiResponse<T = unknown> {
  data: T
  status: number
  headers: Headers
  ok: boolean
  response: Response
}

export interface ApiRequestConfig extends RequestInit {
  params?: Record<string, unknown> | URLSearchParams
  responseType?: 'json' | 'text' | 'blob' | 'arraybuffer'
  data?: unknown
  /** Axios-compatible request timeout retained for legacy callers. */
  timeout?: number
}

export class ApiError<T = unknown> extends Error {
  readonly response?: ApiResponse<T>
  readonly code?: string

  constructor(message: string, response?: ApiResponse<T>, code?: string) {
    super(message)
    this.name = 'ApiError'
    this.response = response
    this.code = code
  }
}

/** Typed OpenAPI client for newly migrated code. Generated types come only
 * from verification/contracts/http/p4-openapi.json. */
export const generatedClient = createClient<paths>({ baseUrl: '', credentials: 'include' })

function appendParams(url: URL, params: ApiRequestConfig['params']): void {
  if (!params) return
  const entries = params instanceof URLSearchParams ? params.entries() : Object.entries(params)
  for (const [key, value] of entries) {
    if (value === undefined || value === null) continue
    if (Array.isArray(value)) {
      for (const item of value) url.searchParams.append(key, String(item))
    } else {
      url.searchParams.set(key, String(value))
    }
  }
}

function isBodyInit(value: unknown): value is BodyInit {
  return (
    typeof value === 'string' ||
    value instanceof Blob ||
    value instanceof FormData ||
    value instanceof URLSearchParams ||
    value instanceof ArrayBuffer ||
    ArrayBuffer.isView(value)
  )
}

function buildRequestBody(config: ApiRequestConfig): BodyInit | undefined {
  const body = config.data ?? config.body
  if (body === undefined || body === null) return undefined
  if (isBodyInit(body)) return body
  return JSON.stringify(body)
}

function normalizeHeaders(config: ApiRequestConfig, body: BodyInit | undefined): Headers {
  const headers = new Headers(config.headers)
  // Browsers add the multipart boundary; an explicit bare header breaks
  // FormData uploads when migrating from Axios.
  if (body instanceof FormData && headers.get('content-type')?.startsWith('multipart/form-data')) {
    headers.delete('content-type')
  }
  if (body && typeof body === 'string' && !headers.has('content-type')) {
    headers.set('content-type', 'application/json')
  }
  return headers
}

async function decode<T>(response: Response, responseType: ApiRequestConfig['responseType']): Promise<T> {
  if (response.status === 204) return undefined as T
  if (responseType === 'blob') return (await response.blob()) as T
  if (responseType === 'arraybuffer') return (await response.arrayBuffer()) as T
  if (responseType === 'text') return (await response.text()) as T
  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('json')) {
    const text = await response.text()
    return (text ? JSON.parse(text) : undefined) as T
  }
  const text = await response.text()
  return (text ? (text as T) : (undefined as T))
}

async function request<T = unknown>(method: string, input: string | URL, config: ApiRequestConfig = {}): Promise<ApiResponse<T>> {
  const origin = typeof window === 'undefined' ? 'http://localhost' : window.location.origin
  const url = new URL(String(input), origin)
  appendParams(url, config.params)
  const body = buildRequestBody(config)
  const timeout = config.timeout && config.timeout > 0 ? config.timeout : undefined
  const controller = timeout !== undefined ? new AbortController() : undefined
  let timedOut = false
  let removeSignalListener: (() => void) | undefined
  if (controller && config.signal) {
    if (config.signal.aborted) {
      controller.abort()
    } else {
      const onAbort = () => controller.abort()
      config.signal.addEventListener('abort', onAbort, { once: true })
      removeSignalListener = () => config.signal?.removeEventListener('abort', onAbort)
    }
  }
  const timeoutId = timeout !== undefined && controller
    ? setTimeout(() => {
        timedOut = true
        controller.abort()
      }, timeout)
    : undefined
  const { params: _params, responseType: _responseType, data: _data, timeout: _timeout, ...requestInit } = config
  let response: Response
  try {
    response = await globalThis.fetch(url, {
      ...requestInit,
      method,
      body,
      signal: controller?.signal ?? config.signal,
      credentials: config.credentials ?? 'include',
      headers: normalizeHeaders(config, body),
    })
  } catch (error) {
    if (isAbortError(error)) {
      throw new ApiError(timedOut ? 'Request timed out' : 'Request aborted', undefined, timedOut ? 'ETIMEDOUT' : 'ERR_CANCELED')
    }
    throw error
  } finally {
    if (timeoutId) clearTimeout(timeoutId)
    removeSignalListener?.()
  }
  let data: T
  try {
    data = await decode<T>(response, config.responseType)
  } catch (error) {
    throw new ApiError(error instanceof Error ? error.message : 'Unable to decode server response', {
      data: undefined as T,
      status: response.status,
      headers: response.headers,
      ok: response.ok,
      response,
    })
  }
  const result: ApiResponse<T> = { data, status: response.status, headers: response.headers, ok: response.ok, response }
  if (!response.ok) {
    const message = typeof data === 'object' && data !== null && 'message' in data ? String((data as { message?: unknown }).message) : `Request failed with ${response.status}`
    throw new ApiError(message, result)
  }
  return result
}

function isAbortError(error: unknown): boolean {
  return Boolean(error && typeof error === 'object' && 'name' in error && (error as { name?: unknown }).name === 'AbortError')
}

/** Resolve a generated endpoint template without leaking URL literals into UI components. */
export function apiPath(key: keyof typeof ENDPOINTS, values: unknown[] = []): string {
  let path: string = ENDPOINTS[key]
  values.forEach((value, index) => {
    path = path.replace(`__ETK_PARAM_${index}__`, String(value ?? ''))
  })
  return path
}

export function unwrap<T>(response: ApiResponse<T>): T {
  return response.data
}

export const apiClient = {
  request,
  get: <T = unknown>(url: string, config?: ApiRequestConfig) => request<T>('GET', url, config),
  post: <T = unknown>(url: string, data?: unknown, config: ApiRequestConfig = {}) => request<T>('POST', url, { ...config, data }),
  put: <T = unknown>(url: string, data?: unknown, config: ApiRequestConfig = {}) => request<T>('PUT', url, { ...config, data }),
  patch: <T = unknown>(url: string, data?: unknown, config: ApiRequestConfig = {}) => request<T>('PATCH', url, { ...config, data }),
  delete: <T = unknown>(url: string, config?: ApiRequestConfig) => request<T>('DELETE', url, config),
  upload: <T = unknown>(url: string, data: BodyInit, config: ApiRequestConfig = {}) => request<T>('POST', url, { ...config, data }),
  download: (url: string, config: ApiRequestConfig = {}) => request<Blob>(config.method || 'GET', url, { ...config, responseType: 'blob' }),
  stream: (url: string, init?: RequestInit) => globalThis.fetch(url, { credentials: 'include', ...init }),
  /** Native Response escape hatch for downloads and streaming adapters. */
  raw: (input: RequestInfo | URL, init?: RequestInit) => globalThis.fetch(input, { credentials: 'include', ...init }),
}

export default apiClient
