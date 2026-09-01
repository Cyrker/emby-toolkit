import apiClient, { apiPath } from './client'

type OperationEnvelope = {
  operation_id?: unknown
  command_id?: unknown
  status?: unknown
  message?: unknown
  success?: unknown
  accepted?: unknown
}

type SharingOperation = {
  status?: unknown
  error?: { message?: unknown; type?: unknown }
}

type DurableRequestState = {
  idempotencyKey: string
  operationId?: string
  commandId?: string
  status?: string
}

const durableRequestMemory = new Map<string, DurableRequestState>()
const durableRequestPrefix = 'etk:durable-request:v1:'

export class SharingOperationError extends Error {
  readonly operationId: string
  readonly operationStatus: string

  constructor(message: string, operationId: string, operationStatus: string) {
    super(`${message}（操作 ID：${operationId}）`)
    this.name = 'SharingOperationError'
    this.operationId = operationId
    this.operationStatus = operationStatus
  }
}

export function newIdempotencyKey(scope: string): string {
  const occurrence = globalThis.crypto?.randomUUID?.()
    ?? `${Date.now()}-${Math.random().toString(36).slice(2)}`
  return `${scope}:${occurrence}`
}

export function selectCenterImportMode(options: {
  virtualImportEnabled: boolean
  logicalSeason: boolean
  validShareChannel: boolean
}): 'rapid' | 'share' {
  return !options.virtualImportEnabled && options.logicalSeason && options.validShareChannel
    ? 'share'
    : 'rapid'
}

export function idempotencyKeyForScope(scope: string): string {
  const current = readDurableRequest(scope)
  if (current?.idempotencyKey) return current.idempotencyKey
  const state = { idempotencyKey: newIdempotencyKey(scope) }
  writeDurableRequest(scope, state)
  return state.idempotencyKey
}

export function rememberDurableRequest(scope: string, payload: unknown): void {
  const current = readDurableRequest(scope)
  if (!current) return
  const value = payload && typeof payload === 'object' ? payload as Record<string, unknown> : {}
  const operationId = String(value.operation_id ?? '').trim()
  const commandId = String(value.command_id ?? '').trim()
  const status = String(value.status ?? '').trim().toLowerCase()
  writeDurableRequest(scope, {
    ...current,
    ...(operationId ? { operationId } : {}),
    ...(commandId ? { commandId } : {}),
    ...(status ? { status } : {}),
  })
}

export function rememberDurableRequestError(scope: string, error: unknown): void {
  const candidate = error as { response?: { data?: unknown } }
  rememberDurableRequest(scope, candidate?.response?.data)
}

export function durableRequestForScope(scope: string): Readonly<DurableRequestState> | null {
  const current = readDurableRequest(scope)
  return current ? { ...current } : null
}

export function releaseDurableRequest(scope: string): void {
  durableRequestMemory.delete(scope)
  try {
    globalThis.localStorage?.removeItem(`${durableRequestPrefix}${scope}`)
  } catch {
    // Memory fallback remains available when persistent browser storage is disabled.
  }
}

export async function opaqueRequestScope(prefix: string, identity: string): Promise<string> {
  const encoded = new TextEncoder().encode(identity)
  if (globalThis.crypto?.subtle) {
    const digest = await globalThis.crypto.subtle.digest('SHA-256', encoded)
    const value = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('')
    return `${prefix}:${value}`
  }
  let hash = 2166136261
  for (const byte of encoded) {
    hash ^= byte
    hash = Math.imul(hash, 16777619)
  }
  return `${prefix}:${(hash >>> 0).toString(16).padStart(8, '0')}`
}

export async function resumeDurableSharingOperation(
  scope: string,
  options: { pollIntervalMs?: number; attempts?: number } = {},
): Promise<OperationEnvelope | SharingOperation | null> {
  const current = readDurableRequest(scope)
  if (!current?.operationId) return null
  return waitForSharingOperation(
    { data: { operation_id: current.operationId, status: current.status ?? 'ready' } },
    { ...options, persistenceScope: scope },
  )
}

export async function resumeDurableTelegramCommand(scope: string): Promise<OperationEnvelope | null> {
  const current = readDurableRequest(scope)
  if (!current?.commandId) return null
  const response = await apiClient.get(
    apiPath('api_subscription_tg_userbot_status_ae9cf3bf'),
    { params: { command_id: current.commandId } },
  )
  const payload = response?.data && typeof response.data === 'object'
    ? response.data as OperationEnvelope
    : {}
  rememberDurableRequest(scope, payload)
  return payload
}

export function shouldRetainIdempotencyKey(error: unknown): boolean {
  const candidate = error as {
    operationStatus?: unknown
    response?: { status?: unknown; data?: { status?: unknown } }
  }
  const status = String(
    candidate?.operationStatus ?? candidate?.response?.data?.status ?? '',
  ).trim().toLowerCase()
  if (['indeterminate', 'pending', 'ready', 'claimed', 'requesting', 'awaiting_readback', 'retryable'].includes(status)) {
    return true
  }
  if (status) return false
  const responseStatus = Number(candidate?.response?.status)
  return !Number.isFinite(responseStatus) || responseStatus >= 500
}

export async function waitForSharingOperation(
  response: { data?: OperationEnvelope },
  options: { pollIntervalMs?: number; attempts?: number; persistenceScope?: string } = {},
): Promise<OperationEnvelope | SharingOperation> {
  const initial = response?.data ?? {}
  if (options.persistenceScope) rememberDurableRequest(options.persistenceScope, initial)
  const operationId = String(initial.operation_id ?? '').trim()
  const initialStatus = String(initial.status ?? '').trim().toLowerCase()
  if (!operationId || initialStatus === 'confirmed') return initial
  assertNonTerminal(initialStatus, operationId, String(initial.message ?? '共享操作未能确认'))

  const pollIntervalMs = options.pollIntervalMs ?? 750
  const attempts = options.attempts ?? 80
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    await new Promise((resolve) => window.setTimeout(resolve, pollIntervalMs))
    const polled = await apiClient.get(
      apiPath('api_shared_resources_operations_param_status_4f7d0b42', [operationId]),
    )
    const payload = polled.data
    const operation = (
      payload && typeof payload === 'object' && 'operation' in payload
        ? payload.operation
        : {}
    ) as SharingOperation
    const status = String(operation.status ?? '').trim().toLowerCase()
    if (options.persistenceScope) {
      rememberDurableRequest(options.persistenceScope, { operation_id: operationId, status })
    }
    if (status === 'confirmed') return operation
    const detail = String(operation.error?.message ?? operation.error?.type ?? '共享操作未能确认')
    assertNonTerminal(status, operationId, detail)
  }
  throw new SharingOperationError('共享操作仍在后台处理中', operationId, 'pending')
}

function assertNonTerminal(status: string, operationId: string, detail: string): void {
  if (['rejected', 'indeterminate', 'cancelled'].includes(status)) {
    throw new SharingOperationError(detail, operationId, status)
  }
}

function readDurableRequest(scope: string): DurableRequestState | null {
  const memory = durableRequestMemory.get(scope)
  if (memory) return memory
  try {
    const raw = globalThis.localStorage?.getItem(`${durableRequestPrefix}${scope}`)
    if (!raw) return null
    const parsed = JSON.parse(raw) as Partial<DurableRequestState>
    const idempotencyKey = String(parsed.idempotencyKey ?? '').trim()
    if (!idempotencyKey) return null
    const state: DurableRequestState = {
      idempotencyKey,
      ...(parsed.operationId ? { operationId: String(parsed.operationId) } : {}),
      ...(parsed.commandId ? { commandId: String(parsed.commandId) } : {}),
      ...(parsed.status ? { status: String(parsed.status) } : {}),
    }
    durableRequestMemory.set(scope, state)
    return state
  } catch {
    return null
  }
}

function writeDurableRequest(scope: string, state: DurableRequestState): void {
  durableRequestMemory.set(scope, state)
  try {
    globalThis.localStorage?.setItem(`${durableRequestPrefix}${scope}`, JSON.stringify(state))
  } catch {
    // The in-memory copy still prevents duplicate submissions in this page lifetime.
  }
}
