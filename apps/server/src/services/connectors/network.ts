import { fetch as undiciFetch, ProxyAgent, type Dispatcher } from "undici";

type HttpsProxyAgentCtor = new (proxyUrl: string) => unknown;

const { HttpsProxyAgent } = require("https-proxy-agent") as {
  HttpsProxyAgent: HttpsProxyAgentCtor;
};

export function createProxyDispatcher(proxyUrl?: string) {
  if (!proxyUrl) {
    return undefined;
  }
  return new ProxyAgent(proxyUrl);
}

export function createProxyWsAgent(proxyUrl?: string) {
  if (!proxyUrl) {
    return undefined;
  }
  return new HttpsProxyAgent(proxyUrl);
}

export async function fetchJsonWithTimeout<T>(url: string, timeoutMs: number, dispatcher?: Dispatcher): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await undiciFetch(url, {
      signal: controller.signal,
      dispatcher
    });
    if (!response.ok) {
      throw new Error(`Request failed with ${response.status} for ${url}.`);
    }
    return (await response.json()) as T;
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchTextWithTimeout(url: string, timeoutMs: number, dispatcher?: Dispatcher): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await undiciFetch(url, {
      signal: controller.signal,
      dispatcher
    });
    if (!response.ok) {
      throw new Error(`Request failed with ${response.status} for ${url}.`);
    }
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}
