const { app, BrowserWindow } = require("electron");
const http = require("node:http");
const path = require("node:path");
const { spawn } = require("node:child_process");
const fs = require("node:fs");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const NODE_EXECUTABLE = process.env.npm_node_execpath || process.env.NODE || process.execPath;
const APP_URL = process.env.APP_URL || "http://127.0.0.1:5173";
const API_BASE_URL = (process.env.TEST_API_BASE_URL || "http://103.179.242.161:10001").replace(/\/+$/, "");
const TARGET_USERS = Math.max(1, Number.parseInt(process.env.TEST_USERS || "10", 10) || 10);
const ORDER_AMOUNT = Number.parseFloat(process.env.TEST_ORDER_AMOUNT || "1") || 1;
const WARMUP_MS = Math.max(1000, Number.parseInt(process.env.TEST_WARMUP_MS || "8000", 10) || 8000);
const SETTLE_MS = Math.max(1000, Number.parseInt(process.env.TEST_SETTLE_MS || "6000", 10) || 6000);
const OUTPUT_PATH = process.env.TEST_OUTPUT_PATH || path.join(PROJECT_ROOT, "deploy", `electron-existing-users-order-${timestampForName()}.json`);
const VITE_PORT = Number.parseInt(process.env.TEST_VITE_PORT || "5173", 10) || 5173;
const SHOW_WINDOWS = process.env.TEST_SHOW_WINDOWS !== "false";

const managedProcesses = [];
const candidateUsers = Array.from({ length: 11 }, (_, index) => ({
  username: `Tester-${index}`,
  password: `${String(index).padStart(2, "0")}A`
}));

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function timestampForName() {
  const now = new Date();
  const parts = [
    now.getFullYear(),
    String(now.getMonth() + 1).padStart(2, "0"),
    String(now.getDate()).padStart(2, "0"),
    String(now.getHours()).padStart(2, "0"),
    String(now.getMinutes()).padStart(2, "0"),
    String(now.getSeconds()).padStart(2, "0")
  ];
  return `${parts[0]}${parts[1]}${parts[2]}_${parts[3]}${parts[4]}${parts[5]}`;
}

function spawnManaged(label, command, args, options = {}) {
  const child = spawn(command, args, {
    cwd: PROJECT_ROOT,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
    ...options,
    env: {
      ...process.env,
      ...(options.env ?? {})
    }
  });
  managedProcesses.push(child);
  child.stdout?.on("data", (chunk) => {
    if (process.env.TEST_VERBOSE === "1") {
      process.stdout.write(`[${label}] ${chunk}`);
    }
  });
  child.stderr?.on("data", (chunk) => {
    if (process.env.TEST_VERBOSE === "1") {
      process.stderr.write(`[${label}] ${chunk}`);
    }
  });
  return child;
}

function stopManagedProcesses() {
  for (const child of managedProcesses.splice(0).reverse()) {
    if (!child.killed && child.exitCode === null) {
      child.kill();
    }
  }
}

function isHttpReady(url) {
  return new Promise((resolve) => {
    const request = http.get(url, (response) => {
      response.resume();
      resolve(Boolean(response.statusCode && response.statusCode < 500));
    });
    request.setTimeout(1000, () => {
      request.destroy();
      resolve(false);
    });
    request.on("error", () => resolve(false));
  });
}

async function waitForHttp(url, label, timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await isHttpReady(url)) {
      return;
    }
    await wait(250);
  }
  throw new Error(`${label} did not become ready at ${url}`);
}

async function ensureRendererServer() {
  if (await isHttpReady(APP_URL)) {
    return;
  }
  spawnManaged(
    "vite",
    NODE_EXECUTABLE,
    [
      path.join(PROJECT_ROOT, "node_modules", "vite", "bin", "vite.js"),
      "--config",
      path.join(PROJECT_ROOT, "apps", "client", "vite.config.ts"),
      "--host",
      "127.0.0.1",
      "--port",
      String(VITE_PORT)
    ],
    {
      env: {
        VITE_DEV_API_PROXY_TARGET: API_BASE_URL
      }
    }
  );
  await waitForHttp(APP_URL, "Vite renderer", 90000);
}

async function requestJson(url, init = {}) {
  const response = await fetch(url, init);
  const text = await response.text();
  const data = text ? JSON.parse(text) : undefined;
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${text}`);
  }
  return data;
}

async function loginCandidate(candidate) {
  const data = await requestJson(`${API_BASE_URL}/api/auth/login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      username: candidate.username,
      password: candidate.password
    })
  });
  return {
    username: candidate.username,
    password: candidate.password,
    token: data.token,
    id: data.id
  };
}

async function pickUsers() {
  const selected = [];
  const failures = [];
  for (const candidate of candidateUsers) {
    if (selected.length >= TARGET_USERS) {
      break;
    }
    try {
      selected.push(await loginCandidate(candidate));
    } catch (error) {
      failures.push({ username: candidate.username, error: error instanceof Error ? error.message : String(error) });
    }
  }
  if (selected.length < TARGET_USERS) {
    throw new Error(`Only ${selected.length}/${TARGET_USERS} existing users could log in. failures=${JSON.stringify(failures)}`);
  }
  return { selected, failures };
}

async function waitForWindowCondition(window, expression, label, timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ok = await window.webContents.executeJavaScript(`Boolean(${expression})`).catch(() => false);
    if (ok) {
      return;
    }
    await wait(250);
  }
  const diagnostic = await window.webContents.executeJavaScript(`
    ({
      href: location.href,
      bodyText: (document.body?.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 800),
      errorBanner: (document.querySelector('.error-banner')?.textContent || '').trim(),
      loginError: (document.querySelector('.terminal-login-error')?.textContent || '').trim(),
      loadingBanner: (document.querySelector('.loading-banner')?.textContent || '').trim()
    })
  `).catch((error) => ({ error: String(error) }));
  throw new Error(`${label} timed out: ${JSON.stringify(diagnostic)}`);
}

async function loginAndBoot(window, account) {
  await window.loadURL(APP_URL);
  await waitForWindowCondition(window, "document.readyState === 'complete'", "initial page load");
  await window.webContents.executeJavaScript(
    `(() => {
      const setNativeValue = (element, value) => {
        const prototype = Object.getPrototypeOf(element);
        const descriptor = Object.getOwnPropertyDescriptor(prototype, 'value');
        if (!descriptor || typeof descriptor.set !== 'function') {
          throw new Error('missing native value setter');
        }
        descriptor.set.call(element, value);
        element.dispatchEvent(new Event('input', { bubbles: true }));
        element.dispatchEvent(new Event('change', { bubbles: true }));
      };
      const usernameInput = document.querySelector('input[autocomplete="username"]');
      const passwordInput = document.querySelector('input[autocomplete="current-password"]');
      const submitButton = document.querySelector('.terminal-sign-button');
      if (!usernameInput || !passwordInput || !submitButton) {
        throw new Error('login controls missing');
      }
      setNativeValue(usernameInput, ${JSON.stringify(account.username)});
      setNativeValue(passwordInput, ${JSON.stringify(account.password)});
      submitButton.click();
    })();`
  );
  await waitForWindowCondition(window, "document.querySelector('.terminal-page')", "trade page");
  await waitForWindowCondition(window, "document.querySelector('.terminal-realtime-state')", "realtime state");
  await window.webContents.executeJavaScript(`
    (() => {
      if (window.__codexRealtimeHistory) {
        return;
      }
      const capture = () => {
        const node = document.querySelector('.terminal-realtime-state');
        if (!node) {
          return;
        }
        const entry = {
          at: Date.now(),
          text: (node.textContent || '').trim(),
          className: node.className
        };
        const history = window.__codexRealtimeHistory = window.__codexRealtimeHistory || [];
        const last = history[history.length - 1];
        if (!last || last.text !== entry.text || last.className !== entry.className) {
          history.push(entry);
        }
      };
      capture();
      const observer = new MutationObserver(capture);
      observer.observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['class'] });
      window.__codexRealtimeObserver = observer;
    })();
  `);
}

async function collectWindowSnapshot(window) {
  return window.webContents.executeJavaScript(`
    (async () => {
      const { useAppStore } = await import('/store/useAppStore.ts');
      const state = useAppStore.getState();
      const realtimeNode = document.querySelector('.terminal-realtime-state');
      return {
        realtimeText: (realtimeNode?.textContent || '').trim(),
        realtimeClassName: realtimeNode?.className || '',
        currentOrdersCount: document.querySelectorAll('.terminal-current-order-card').length,
        lastMarketRecvTs: state.lastMarketRecvTs,
        ordersCount: state.orders.length,
        orderLifecycleCount: state.orderLifecycles.length,
        stateHistory: window.__codexRealtimeHistory || []
      };
    })();
  `);
}

async function placeOrderAndMeasure(window, token, username) {
  const result = await window.webContents.executeJavaScript(`
    (async () => {
      const { useAppStore } = await import('/store/useAppStore.ts');
      const { api } = await import('/utils/api.ts');
      const { latencyForSource } = await import('/features/market/source-latency.ts');
      const snapshotLatency = (state) => {
        const now = Date.now();
        const clientRecvTs = state.lastMarketRecvTs;
        const clientClockOffsetMs = window.__codexClientClockOffsetMs || 0;
        return {
          sampledAt: now,
          lastMarketRecvTs: clientRecvTs,
          clob: latencyForSource(state.snapshot?.sources?.clob, now, clientRecvTs, clientClockOffsetMs),
          binance: latencyForSource(state.snapshot?.sources?.binance, now, clientRecvTs, clientClockOffsetMs),
          coinbase: latencyForSource(state.snapshot?.sources?.coinbase, now, clientRecvTs, clientClockOffsetMs)
        };
      };
      const sampledClockOffsetMs = await api.sampleClockOffset().catch(() => undefined);
      window.__codexClientClockOffsetMs =
        typeof sampledClockOffsetMs === 'number' ? sampledClockOffsetMs : window.__codexClientClockOffsetMs || 0;
      const before = useAppStore.getState();
      const marketLatencyBeforeOrder = snapshotLatency(before);
      const clientOrderId = 'codex_' + Date.now() + '_' + Math.random().toString(36).slice(2);
      const startedAt = Date.now();
      const response = await fetch('/api/orders', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: 'Bearer ' + ${JSON.stringify(token)}
        },
        body: JSON.stringify({
          action: 'buy',
          side: 'UP',
          orderKind: 'market',
          amount: ${JSON.stringify(ORDER_AMOUNT)},
          clientOrderId,
          clientSendTs: startedAt
        })
      });
      const body = await response.json();
      const finishedAt = Date.now();
      if (!response.ok) {
        throw new Error(JSON.stringify(body));
      }
      const orderId = body?.order?.id;
      const waitDeadline = Date.now() + 15000;
      let appearedAt;
      while (Date.now() < waitDeadline) {
        const current = useAppStore.getState();
        if (current.orders.some((order) => order.id === orderId)) {
          appearedAt = Date.now();
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      const after = useAppStore.getState();
      return {
        username: ${JSON.stringify(username)},
        clientOrderId,
        orderId,
        startedAt,
        finishedAt,
        httpMs: finishedAt - startedAt,
        orderAppearedAt: appearedAt,
        clickToStoreMs: typeof appearedAt === 'number' ? appearedAt - startedAt : undefined,
        finishToStoreMs: typeof appearedAt === 'number' ? appearedAt - finishedAt : undefined,
        beforeOrdersCount: before.orders.length,
        afterOrdersCount: after.orders.length,
        lastMarketRecvTs: after.lastMarketRecvTs,
        marketLatencyBeforeOrder
      };
    })();
  `);
  return result;
}

async function main() {
  let windows = [];
  let rendererStartedByScript = false;
  try {
    await ensureRendererServer();
    rendererStartedByScript = !(await isHttpReady(APP_URL)) ? true : false;
    const { selected, failures } = await pickUsers();
    const startedAt = Date.now();
    const perUser = [];

    for (const account of selected) {
      const window = new BrowserWindow({
        show: SHOW_WINDOWS,
        width: 1440,
        height: 960,
        webPreferences: {
          partition: `persist:codex-${account.username}-${Date.now()}`,
          backgroundThrottling: false,
          contextIsolation: true,
          sandbox: false
        }
      });
      windows.push({ window, account });
      await loginAndBoot(window, account);
    }

    await wait(WARMUP_MS);

    const warmupSnapshots = [];
    for (const item of windows) {
      warmupSnapshots.push({
        username: item.account.username,
        ...(await collectWindowSnapshot(item.window))
      });
    }

    const orderResults = await Promise.all(
      windows.map((item) => placeOrderAndMeasure(item.window, item.account.token, item.account.username))
    );

    await wait(SETTLE_MS);

    const finalSnapshots = [];
    for (const item of windows) {
      finalSnapshots.push({
        username: item.account.username,
        ...(await collectWindowSnapshot(item.window))
      });
    }

    const summary = summarize(orderResults, finalSnapshots);
    const output = {
      startedAt: new Date(startedAt).toISOString(),
      finishedAt: new Date().toISOString(),
      appUrl: APP_URL,
      apiBaseUrl: API_BASE_URL,
      targetUsers: TARGET_USERS,
      orderAmount: ORDER_AMOUNT,
      selectedUsers: selected.map(({ username }) => username),
      skippedUsers: failures,
      warmupSnapshots,
      orderResults,
      finalSnapshots,
      summary
    };
    fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
    fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));
    console.log(`electron-existing-users-order-check wrote ${OUTPUT_PATH}`);
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    for (const item of windows) {
      if (!item.window.isDestroyed()) {
        item.window.destroy();
      }
    }
    stopManagedProcesses();
    if (!app.isQuiting) {
      app.quit();
    }
  }
}

function percentile(values, ratio) {
  if (!values.length) {
    return undefined;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index];
}

function summarize(orderResults, finalSnapshots) {
  const clickToStore = orderResults.map((item) => item.clickToStoreMs).filter((value) => typeof value === "number");
  const finishToStore = orderResults.map((item) => item.finishToStoreMs).filter((value) => typeof value === "number");
  const http = orderResults.map((item) => item.httpMs).filter((value) => typeof value === "number");
  const clobMarketAge = orderResults.map((item) => item.marketLatencyBeforeOrder?.clob?.marketUpdateAgeMs).filter((value) => typeof value === "number");
  const clobUpstreamAge = orderResults.map((item) => item.marketLatencyBeforeOrder?.clob?.sourceDataAgeMs).filter((value) => typeof value === "number");
  const clobTransport = orderResults.map((item) => item.marketLatencyBeforeOrder?.clob?.backendToFrontendLatencyMs).filter((value) => typeof value === "number");
  const btcMarketAge = orderResults.map((item) => item.marketLatencyBeforeOrder?.binance?.marketUpdateAgeMs).filter((value) => typeof value === "number");
  const coinbaseMarketAge = orderResults.map((item) => item.marketLatencyBeforeOrder?.coinbase?.marketUpdateAgeMs).filter((value) => typeof value === "number");
  const nonLiveWindows = finalSnapshots.filter((item) => !/live/i.test(item.realtimeText));
  return {
    orders: orderResults.length,
    httpMs: {
      p50: percentile(http, 0.5),
      p95: percentile(http, 0.95),
      max: http.length ? Math.max(...http) : undefined
    },
    clickToStoreMs: {
      p50: percentile(clickToStore, 0.5),
      p95: percentile(clickToStore, 0.95),
      max: clickToStore.length ? Math.max(...clickToStore) : undefined
    },
    finishToStoreMs: {
      p50: percentile(finishToStore, 0.5),
      p95: percentile(finishToStore, 0.95),
      max: finishToStore.length ? Math.max(...finishToStore) : undefined
    },
    clobLatencyAtOrderMs: {
      marketAgeP50: percentile(clobMarketAge, 0.5),
      marketAgeP95: percentile(clobMarketAge, 0.95),
      upstreamAgeP50: percentile(clobUpstreamAge, 0.5),
      upstreamAgeP95: percentile(clobUpstreamAge, 0.95),
      transportP50: percentile(clobTransport, 0.5),
      transportP95: percentile(clobTransport, 0.95)
    },
    binanceMarketAgeAtOrderMs: {
      p50: percentile(btcMarketAge, 0.5),
      p95: percentile(btcMarketAge, 0.95)
    },
    coinbaseMarketAgeAtOrderMs: {
      p50: percentile(coinbaseMarketAge, 0.5),
      p95: percentile(coinbaseMarketAge, 0.95)
    },
    finalRealtimeTexts: finalSnapshots.map((item) => ({ username: item.username, realtimeText: item.realtimeText })),
    nonLiveWindows: nonLiveWindows.map((item) => ({ username: item.username, realtimeText: item.realtimeText, history: item.stateHistory }))
  };
}

app.whenReady().then(main).catch((error) => {
  console.error(error);
  stopManagedProcesses();
  app.exit(1);
});

app.on("window-all-closed", (event) => {
  event.preventDefault();
});
