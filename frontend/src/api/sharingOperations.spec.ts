import { describe, expect, it } from 'vitest'

import {
  durableRequestForScope,
  idempotencyKeyForScope,
  releaseDurableRequest,
  rememberDurableRequest,
  selectCenterImportMode,
  shouldRetainIdempotencyKey,
} from './sharingOperations'

describe('sharing operation idempotency lifecycle', () => {
  it.each(['indeterminate', 'pending', 'ready', 'claimed', 'requesting', 'awaiting_readback', 'retryable'])(
    'retains the key for %s outcomes',
    (status) => {
      expect(shouldRetainIdempotencyKey({ operationStatus: status })).toBe(true)
    },
  )

  it('retains the key after transport and server failures', () => {
    expect(shouldRetainIdempotencyKey(new Error('network unavailable'))).toBe(true)
    expect(shouldRetainIdempotencyKey({ response: { status: 503, data: {} } })).toBe(true)
  })

  it('releases the key after deterministic failures', () => {
    expect(shouldRetainIdempotencyKey({ operationStatus: 'rejected' })).toBe(false)
    expect(shouldRetainIdempotencyKey({ response: { status: 400, data: {} } })).toBe(false)
  })

  it('persists only opaque durable request identifiers', () => {
    const scope = 'fixture-durable-request'
    const key = idempotencyKeyForScope(scope)
    rememberDurableRequest(scope, {
      command_id: 'fixture-command',
      status: 'requesting',
      code: '12345',
      resource_url: 'https://fixture.invalid/private',
      provider_secret: 'hidden',
    })

    expect(durableRequestForScope(scope)).toEqual({
      idempotencyKey: key,
      commandId: 'fixture-command',
      status: 'requesting',
    })
    releaseDurableRequest(scope)
  })

  it('selects real share transfer only for an eligible logical season', () => {
    expect(selectCenterImportMode({
      virtualImportEnabled: false,
      logicalSeason: true,
      validShareChannel: true,
    })).toBe('share')
    expect(selectCenterImportMode({
      virtualImportEnabled: false,
      logicalSeason: true,
      validShareChannel: false,
    })).toBe('rapid')
    expect(selectCenterImportMode({
      virtualImportEnabled: true,
      logicalSeason: true,
      validShareChannel: true,
    })).toBe('rapid')
  })
})
