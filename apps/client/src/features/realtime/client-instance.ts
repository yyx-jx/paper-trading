export const CLIENT_INSTANCE_STORAGE_KEY = "btc-paper-trading-client-instance-id";

type ClientInstanceStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

let fallbackClientInstanceId: string | undefined;

export function normalizeStoredClientInstanceId(raw: unknown) {
  if (typeof raw !== "string") {
    return undefined;
  }
  const value = raw.trim();
  return /^[A-Za-z0-9_-]{8,80}$/.test(value) ? value : undefined;
}

function randomHex(bytes: number) {
  const cryptoApi = globalThis.crypto;
  if (cryptoApi?.getRandomValues) {
    const values = new Uint8Array(bytes);
    cryptoApi.getRandomValues(values);
    return Array.from(values, (value) => value.toString(16).padStart(2, "0")).join("");
  }
  return `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 18)}`;
}

function createClientInstanceId() {
  const uuid = globalThis.crypto?.randomUUID?.();
  return normalizeStoredClientInstanceId(uuid ? `ci_${uuid}` : `ci_${randomHex(16)}`) ?? `ci_${randomHex(16)}`;
}

function resolveStorage(storage?: ClientInstanceStorage) {
  if (storage) {
    return storage;
  }
  try {
    return globalThis.localStorage;
  } catch {
    return undefined;
  }
}

export function getClientInstanceId(storage?: ClientInstanceStorage) {
  const resolvedStorage = resolveStorage(storage);
  if (!resolvedStorage) {
    fallbackClientInstanceId = fallbackClientInstanceId ?? createClientInstanceId();
    return fallbackClientInstanceId;
  }

  try {
    const existing = normalizeStoredClientInstanceId(resolvedStorage.getItem(CLIENT_INSTANCE_STORAGE_KEY));
    if (existing) {
      return existing;
    }
    resolvedStorage.removeItem(CLIENT_INSTANCE_STORAGE_KEY);
    const next = createClientInstanceId();
    resolvedStorage.setItem(CLIENT_INSTANCE_STORAGE_KEY, next);
    return next;
  } catch {
    fallbackClientInstanceId = fallbackClientInstanceId ?? createClientInstanceId();
    return fallbackClientInstanceId;
  }
}
