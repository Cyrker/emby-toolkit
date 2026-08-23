export const QUICK_DEPLOY_INTENT_STORAGE_KEY = 'etk.quick-deploy.pending-intent';

export const getQuickDeployIntent = (
  storage = globalThis.localStorage,
  createId = () => globalThis.crypto.randomUUID()
) => {
  const pending = storage.getItem(QUICK_DEPLOY_INTENT_STORAGE_KEY);
  if (pending) return pending;
  const created = createId();
  storage.setItem(QUICK_DEPLOY_INTENT_STORAGE_KEY, created);
  return created;
};

export const clearQuickDeployIntent = (storage = globalThis.localStorage) => {
  storage.removeItem(QUICK_DEPLOY_INTENT_STORAGE_KEY);
};

export const assertQuickDeployResponse = async (response, storage = globalThis.localStorage) => {
  if (response.ok && response.body) return;

  let serverMessage = '';
  try {
    const payload = await response.json();
    serverMessage = String(payload?.message || payload?.error || '').trim();
  } catch {
    // An empty or non-JSON failure still gets a deterministic HTTP error below.
  }

  if (response.status === 409) {
    clearQuickDeployIntent(storage);
    throw new Error(serverMessage || '部署目标已变化，请重新确认');
  }
  throw new Error(serverMessage || `请求失败: HTTP ${response.status}`);
};
