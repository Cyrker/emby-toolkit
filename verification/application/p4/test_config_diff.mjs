import assert from 'node:assert/strict';
import test from 'node:test';

import { cloneConfigSnapshot, configUpdatesSince } from '../../../frontend/src/composables/configDiff.js';

test('a stale full form submits only fields changed by the user', () => {
  const baseline = cloneConfigSnapshot({
    monitor_enabled: false,
    monitor_paths: ['/old'],
    p115_media_root_cid: 'old-cid',
    ai_model_name: 'old-model'
  });
  const form = cloneConfigSnapshot(baseline);
  form.ai_model_name = 'new-model';

  assert.deepEqual(configUpdatesSince(form, baseline), { ai_model_name: 'new-model' });
});

test('array changes are submitted and an unchanged form produces no update', () => {
  const baseline = { monitor_paths: ['/one'], monitor_enabled: true };

  assert.deepEqual(configUpdatesSince(cloneConfigSnapshot(baseline), baseline), {});
  assert.deepEqual(configUpdatesSince({ ...baseline, monitor_paths: ['/one', '/two'] }, baseline), {
    monitor_paths: ['/one', '/two']
  });
});
