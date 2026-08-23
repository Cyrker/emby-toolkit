import assert from 'node:assert/strict';
import test from 'node:test';

import {
  QUICK_DEPLOY_INTENT_STORAGE_KEY,
  assertQuickDeployResponse,
  getQuickDeployIntent
} from '../../../frontend/src/components/settings/quickDeployIntent.js';

class MemoryStorage {
  constructor() {
    this.values = new Map();
  }

  getItem(key) {
    return this.values.get(key) ?? null;
  }

  setItem(key, value) {
    this.values.set(key, value);
  }

  removeItem(key) {
    this.values.delete(key);
  }
}

test('409 clears the pending intent and the next explicit attempt creates a new UUID', async () => {
  const storage = new MemoryStorage();
  const ids = ['intent-1', 'intent-2'];
  const createId = () => ids.shift();

  assert.equal(getQuickDeployIntent(storage, createId), 'intent-1');
  assert.equal(getQuickDeployIntent(storage, createId), 'intent-1');

  const conflict = new Response(JSON.stringify({ message: '部署目标已变化，请重新确认' }), {
    status: 409,
    headers: { 'Content-Type': 'application/json' }
  });
  await assert.rejects(assertQuickDeployResponse(conflict, storage), /部署目标已变化/);
  assert.equal(storage.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY), null);
  assert.equal(getQuickDeployIntent(storage, createId), 'intent-2');
});

test('503 and interrupted requests retain the pending intent for retry', async () => {
  const storage = new MemoryStorage();
  storage.setItem(QUICK_DEPLOY_INTENT_STORAGE_KEY, 'intent-retry');

  const unavailable = new Response(JSON.stringify({ message: '持久任务服务不可用' }), {
    status: 503,
    headers: { 'Content-Type': 'application/json' }
  });
  await assert.rejects(assertQuickDeployResponse(unavailable, storage), /持久任务服务不可用/);
  assert.equal(storage.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY), 'intent-retry');
  assert.equal(getQuickDeployIntent(storage, () => 'unexpected'), 'intent-retry');
});
