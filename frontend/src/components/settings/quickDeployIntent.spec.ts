import { describe, expect, it, vi } from 'vitest'
import {
  QUICK_DEPLOY_INTENT_STORAGE_KEY,
  assertQuickDeployResponse,
  finalizeQuickDeployIntent,
  getQuickDeployIntent,
} from './quickDeployIntent'

const storage = (): Storage => {
  const values = new Map<string, string>()
  return {
    get length() {
      return values.size
    },
    clear: () => values.clear(),
    getItem: (key: string) => values.get(key) ?? null,
    key: (index: number) => [...values.keys()][index] ?? null,
    removeItem: (key: string) => {
      values.delete(key)
    },
    setItem: (key: string, value: string) => {
      values.set(key, value)
    },
  }
}

describe('quick deploy idempotency intent', () => {
  it('reuses the pending key until a definitive terminal result', () => {
    const target = storage()
    const intentId = '00000000-0000-4000-8000-000000000115'
    const createIdMock = vi.fn(() => intentId)
    const createId = createIdMock as typeof globalThis.crypto.randomUUID

    expect(getQuickDeployIntent(target, createId)).toBe(intentId)
    expect(getQuickDeployIntent(target, createId)).toBe(intentId)
    expect(createIdMock).toHaveBeenCalledTimes(1)

    finalizeQuickDeployIntent('indeterminate', target)
    expect(target.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY)).toBe(intentId)

    finalizeQuickDeployIntent('rejected', target)
    expect(target.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY)).toBeNull()
  })

  it.each(['confirmed', 'cancelled', 'rejected'])(
    'clears the key for terminal status %s',
    (status) => {
      const target = storage()
      target.setItem(QUICK_DEPLOY_INTENT_STORAGE_KEY, 'intent-1')

      finalizeQuickDeployIntent(status, target)

      expect(target.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY)).toBeNull()
    },
  )

  it.each([400, 403, 409])('clears the key for deterministic HTTP %s rejection', async (status) => {
    const target = storage()
    target.setItem(QUICK_DEPLOY_INTENT_STORAGE_KEY, 'intent-1')
    const response = new Response(JSON.stringify({ message: 'rejected' }), {
      status,
      headers: { 'content-type': 'application/json' },
    })

    await expect(assertQuickDeployResponse(response, target)).rejects.toThrow('rejected')
    expect(target.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY)).toBeNull()
  })

  it('retains the key when submission outcome is not authoritative', async () => {
    const target = storage()
    target.setItem(QUICK_DEPLOY_INTENT_STORAGE_KEY, 'intent-1')
    const response = new Response(JSON.stringify({ message: 'unavailable' }), {
      status: 503,
      headers: { 'content-type': 'application/json' },
    })

    await expect(assertQuickDeployResponse(response, target)).rejects.toThrow('unavailable')
    expect(target.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY)).toBe('intent-1')
  })
})
