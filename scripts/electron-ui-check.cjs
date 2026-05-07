const { execFileSync, spawn } = require("node:child_process");
const { app, BrowserWindow } = require("electron");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");

const APP_URL = process.env.APP_URL || "http://127.0.0.1:5173";
const API_URL = process.env.API_URL || "http://127.0.0.1:8787";
const PROJECT_ROOT = path.resolve(__dirname, "..");
const NODE_EXECUTABLE = process.env.npm_node_execpath || process.env.NODE || process.execPath;
const NODE_CHILD_ENV =
  NODE_EXECUTABLE === process.execPath
    ? { ELECTRON_RUN_AS_NODE: "1" }
    : {};

const managedProcesses = [];

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

async function waitForHttp(url, label, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await isHttpReady(url)) {
      return;
    }
    await wait(250);
  }
  throw new Error(`${label} did not become ready at ${url}`);
}

function spawnManaged(label, command, args, options = {}) {
  const child = spawn(command, args, {
    cwd: PROJECT_ROOT,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
    ...options,
    env: {
      ...process.env,
      ...NODE_CHILD_ENV,
      ...(options.env ?? {})
    }
  });
  managedProcesses.push(child);
  child.stdout?.on("data", (chunk) => {
    if (process.env.UI_CHECK_VERBOSE === "1") {
      process.stdout.write(`[${label}] ${chunk}`);
    }
  });
  child.stderr?.on("data", (chunk) => {
    if (process.env.UI_CHECK_VERBOSE === "1") {
      process.stderr.write(`[${label}] ${chunk}`);
    }
  });
  return child;
}

function stopManagedProcesses() {
  for (const child of managedProcesses.splice(0).reverse()) {
    if (!child.killed && child.exitCode === null) {
      if (process.platform === "win32" && child.pid) {
        try {
          execFileSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
          continue;
        } catch {
          // Fall through to the normal signal path below.
        }
      }
      child.kill();
    }
  }
}

function exitWithFailure(error) {
  console.error(error);
  stopManagedProcesses();
  if (app.isReady()) {
    app.exit(1);
    return;
  }
  process.exit(1);
}

function portFromUrl(url, fallback) {
  try {
    const parsed = new URL(url);
    return Number(parsed.port || (parsed.protocol === "https:" ? 443 : 80));
  } catch {
    return fallback;
  }
}

async function ensureApiServer() {
  if (await isHttpReady(`${API_URL}/health`)) {
    return;
  }
  const apiPort = portFromUrl(API_URL, 8787);
  spawnManaged(
    "api",
    NODE_EXECUTABLE,
    [path.join(PROJECT_ROOT, "node_modules", "tsx", "dist", "cli.cjs"), path.join(PROJECT_ROOT, "apps", "server", "src", "index.ts")],
    {
      env: {
        PORT: String(apiPort),
        MATCHING_SERVICE_PORT: String(apiPort + 1),
        MATCHING_SERVICE_URL: `http://127.0.0.1:${apiPort + 1}`,
        EMBEDDED_MATCHING_SERVICE: "true",
        PERSISTENCE_MODE: "memory",
        CHAINLINK_ENABLED: "false",
        SERVER_STRICT_PERSISTENCE: "false",
        UPSTREAM_PROXY_URL: "",
        JWT_SECRET: "btc-paper-trading-ui-test-secret",
        DATABASE_URL: "",
        REDIS_URL: ""
      }
    }
  );
  await waitForHttp(`${API_URL}/health`, "API server", 90000);
}

async function ensureRendererServer() {
  if (await isHttpReady(APP_URL)) {
    return;
  }
  const appPort = portFromUrl(APP_URL, 5173);
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
      String(appPort)
    ]
  );
  await waitForHttp(APP_URL, "Vite renderer", 60000);
}

async function evaluateTradeLayout(window, width, height) {
  window.setContentSize(width, height);
  await wait(1200);
  return window.webContents.executeJavaScript(`
    (() => {
      const root = document.documentElement;
      const body = document.body;
      const intervalButtons = Array.from(document.querySelectorAll(".chart-toolbar.compact button"))
        .map((button) => (button.textContent || "").trim())
        .filter((text) => ["30s", "1m", "5m", "15m", "1h"].includes(text));
      const hasTradePage = Boolean(document.querySelector(".terminal-page"));
      const overflowFree =
        root.scrollWidth === root.clientWidth &&
        root.scrollHeight === root.clientHeight &&
        body.scrollWidth === body.clientWidth &&
        body.scrollHeight === body.clientHeight;
      const allModulesPresent = [
        ".terminal-left",
        ".terminal-center",
        ".terminal-right",
        ".terminal-chart-block",
        ".terminal-depth",
        ".terminal-order",
        ".health-grid",
        ".strategy-list"
      ].every((selector) => Boolean(document.querySelector(selector)));
      const monitorCells = Array.from(document.querySelectorAll(".terminal-monitor .monitor-cell, .terminal-monitor .monitor-timer"));
      const monitorCellsFit = monitorCells.every((node) => {
        const element = node;
        return element.scrollWidth <= element.clientWidth + 1 && element.scrollHeight <= element.clientHeight + 1;
      });
      const monitorCellMetrics = monitorCells.map((node) => {
        const element = node;
        return {
          className: element.className,
          clientWidth: element.clientWidth,
          scrollWidth: element.scrollWidth,
          clientHeight: element.clientHeight,
          scrollHeight: element.scrollHeight,
          text: (element.textContent || "").replace(/\\s+/g, " ").trim()
        };
      });
      return {
        width: window.innerWidth,
        height: window.innerHeight,
        bodyText: (body.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 500),
        crashBoundary: Boolean(document.querySelector(".app-crash-boundary")),
        loginPage: Boolean(document.querySelector(".terminal-login-page")),
        rootClientWidth: root.clientWidth,
        rootClientHeight: root.clientHeight,
        rootScrollWidth: root.scrollWidth,
        rootScrollHeight: root.scrollHeight,
        bodyClientWidth: body.clientWidth,
        bodyClientHeight: body.clientHeight,
        bodyScrollWidth: body.scrollWidth,
        bodyScrollHeight: body.scrollHeight,
        hasTradePage,
        overflowFree,
        allModulesPresent,
        monitorCellsFit,
        monitorCellMetrics,
        intervalButtons
      };
    })();
  `);
}

async function waitForRendererSelector(window, selector, label, timeoutMs = 30000) {
  return waitForRendererCondition(
    window,
    `Boolean(document.querySelector(${JSON.stringify(selector)}))`,
    label,
    timeoutMs
  );
}

async function waitForRendererCondition(window, expression, label, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const present = await window.webContents
      .executeJavaScript(`Boolean(${expression})`)
      .catch(() => false);
    if (present) {
      return;
    }
    await wait(250);
  }
  const bodyText = await window.webContents
    .executeJavaScript(`(document.body?.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 500)`)
    .catch((error) => `Unable to read renderer body: ${error.message}`);
  throw new Error(`${label} did not appear. body=${JSON.stringify(bodyText)}`);
}

async function waitForRendererSelectorResult(window, selector, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const present = await window.webContents
      .executeJavaScript(`Boolean(document.querySelector(${JSON.stringify(selector)}))`)
      .catch(() => false);
    if (present) {
      return true;
    }
    await wait(250);
  }
  return false;
}

async function assertZoomLocked(window, label) {
  const zoomFactor = window.webContents.getZoomFactor();
  if (Math.abs(zoomFactor - 1) > 0.001) {
    throw new Error(`${label}: expected Electron zoom factor 1, got ${zoomFactor}`);
  }
}

async function main() {
  app.commandLine.appendSwitch("disable-gpu");
  app.setPath("userData", path.join(os.tmpdir(), `paper-trading-ui-check-${process.pid}`));
  await ensureApiServer();
  await ensureRendererServer();
  await app.whenReady();
  const window = new BrowserWindow({
    width: 1440,
    height: 1000,
    show: false,
    webPreferences: {
      contextIsolation: true,
      nodeIntegration: false
    }
  });
  await window.loadURL(APP_URL);
  await wait(5000);
  await window.webContents.executeJavaScript(`
    (() => {
      window.__uiErrors = [];
      window.addEventListener("error", (event) => {
        window.__uiErrors.push(String(event.error?.message || event.message || "error"));
      });
      window.addEventListener("unhandledrejection", (event) => {
        window.__uiErrors.push(String(event.reason?.message || event.reason || "unhandledrejection"));
      });
    })();
  `);
  await window.webContents.executeJavaScript(`
    (async () => {
      const response = await fetch("${API_URL}/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: "admin", password: "admin123" })
      });
      const data = await response.json();
      if (!data.token) {
        throw new Error("Admin login failed in UI check.");
      }
      await fetch("${API_URL}/api/me/language", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: "Bearer " + data.token },
        body: JSON.stringify({ language: "en-US" })
      });
      localStorage.setItem("paper-trading-token", data.token);
      location.reload();
    })();
  `);
  await waitForRendererSelector(window, ".terminal-login-page, .terminal-page, .page-tabs", "Initial app shell", 30000);
  if (!(await waitForRendererSelectorResult(window, ".terminal-page", 45000))) {
    await window.webContents.executeJavaScript(`
      (() => {
        const setReactInputValue = (input, value) => {
          const descriptor =
            Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value") ||
            Object.getOwnPropertyDescriptor(Object.getPrototypeOf(input), "value");
          if (!descriptor || typeof descriptor.set !== "function") {
            throw new Error("Input value setter not found.");
          }
          descriptor.set.call(input, value);
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
        };
        const inputs = Array.from(document.querySelectorAll(".terminal-login-card input"));
        const languageSelect = document.querySelector(".terminal-login-card select");
        const usernameInput = inputs[0];
        const passwordInput = inputs[1];
        const signInButton = Array.from(document.querySelectorAll(".terminal-sign-button"))
          .find((button) => !(button.disabled));
        if (!usernameInput || !passwordInput || !signInButton || !languageSelect) {
          throw new Error("Login form was not available after token bootstrap timeout.");
        }
        setReactInputValue(usernameInput, "admin");
        setReactInputValue(passwordInput, "admin123");
        languageSelect.value = "en-US";
        languageSelect.dispatchEvent(new Event("change", { bubbles: true }));
        signInButton.click();
      })();
    `);
  }
  await waitForRendererCondition(
    window,
    `document.querySelector(".terminal-page") && !document.querySelector(".terminal-login-page")`,
    "Stable trade page after login",
    60000
  );
  await window.webContents.executeJavaScript(`
    (async () => {
      const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
      for (let attempt = 0; attempt < 20; attempt += 1) {
        const tradeButton = Array.from(document.querySelectorAll(".page-tabs button"))
          .find((button) => (button.textContent || "").trim().toLowerCase().includes("trade"));
        if (tradeButton) {
          tradeButton.click();
          await sleep(300);
          return true;
        }
        await sleep(250);
      }
      return false;
    })();
  `);
  await waitForRendererCondition(
    window,
    `document.querySelector(".terminal-page") && !document.querySelector(".terminal-login-page")`,
    "Stable trade page before layout checks",
    30000
  );
  await wait(1200);
  await window.webContents.executeJavaScript(`
    (() => {
      window.__uiErrors = [];
      window.addEventListener("error", (event) => {
        window.__uiErrors.push(String(event.error?.message || event.message || "error"));
      });
      window.addEventListener("unhandledrejection", (event) => {
        window.__uiErrors.push(String(event.reason?.message || event.reason || "unhandledrejection"));
      });
    })();
  `);
  await assertZoomLocked(window, "after login");
  const trade1440 = await evaluateTradeLayout(window, 1440, 900);
  if (!trade1440.hasTradePage || !trade1440.overflowFree || !trade1440.allModulesPresent || !trade1440.monitorCellsFit) {
    throw new Error(`Trade page 1440x900 check failed: ${JSON.stringify(trade1440)}`);
  }
  if (["30s", "1m", "5m", "15m", "1h"].some((label) => !trade1440.intervalButtons.includes(label))) {
    throw new Error(`Missing trade interval buttons at 1440x900: ${JSON.stringify(trade1440.intervalButtons)}`);
  }
  const trade1920 = await evaluateTradeLayout(window, 1920, 1080);
  if (!trade1920.hasTradePage || !trade1920.overflowFree || !trade1920.allModulesPresent || !trade1920.monitorCellsFit) {
    throw new Error(`Trade page 1920x1080 check failed: ${JSON.stringify(trade1920)}`);
  }
  window.webContents.sendInputEvent({ type: "keyDown", keyCode: "Control" });
  window.webContents.sendInputEvent({ type: "keyDown", keyCode: "+", modifiers: ["control"] });
  window.webContents.sendInputEvent({ type: "keyUp", keyCode: "+", modifiers: ["control"] });
  window.webContents.sendInputEvent({ type: "keyDown", keyCode: "-", modifiers: ["control"] });
  window.webContents.sendInputEvent({ type: "keyUp", keyCode: "-", modifiers: ["control"] });
  window.webContents.sendInputEvent({ type: "keyDown", keyCode: "0", modifiers: ["control"] });
  window.webContents.sendInputEvent({ type: "keyUp", keyCode: "0", modifiers: ["control"] });
  window.webContents.sendInputEvent({ type: "mouseWheel", x: 960, y: 420, deltaY: -420, modifiers: ["control"] });
  window.webContents.sendInputEvent({ type: "keyUp", keyCode: "Control" });
  await wait(300);
  await assertZoomLocked(window, "after zoom shortcut stress");
  const result = await window.webContents.executeJavaScript(`
    (async () => {
      const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
      const setReactInputValue = (input, value) => {
        const descriptor =
          Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value") ||
          Object.getOwnPropertyDescriptor(Object.getPrototypeOf(input), "value");
        if (!descriptor || typeof descriptor.set !== "function") {
          throw new Error("Input value setter not found.");
        }
        descriptor.set.call(input, value);
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.dispatchEvent(new Event("change", { bubbles: true }));
      };
      const buttons = () => Array.from(document.querySelectorAll("button"));
      const byText = (text) => buttons().find((button) => (button.textContent || "").includes(text));
      const clickAny = (...texts) => {
        const button = texts.map(byText).find(Boolean);
        if (!button) throw new Error("Missing button: " + texts.join(" / "));
        button.click();
      };
      const assertPageStable = (label) => {
        const root = document.documentElement;
        const body = document.body;
        const bodyText = (body.textContent || "").replace(/\s+/g, " ").trim();
        if (bodyText.length < 80) {
          throw new Error(label + ": page appears blank after chart wheel. bodyText=" + JSON.stringify(bodyText));
        }
        if (document.querySelector(".app-crash-boundary")) {
          throw new Error(label + ": renderer error boundary is visible.");
        }
        const requiredSelectors = [
          ".terminal-page",
          ".terminal-body",
          ".terminal-chart-block",
          ".terminal-order"
        ];
        for (const selector of requiredSelectors) {
          const element = document.querySelector(selector);
          if (!element) {
            throw new Error(label + ": missing " + selector);
          }
          const rect = element.getBoundingClientRect();
          if (rect.width <= 0 || rect.height <= 0) {
            throw new Error(label + ": invalid size for " + selector + " " + JSON.stringify({
              width: rect.width,
              height: rect.height
            }));
          }
        }
        const charts = Array.from(document.querySelectorAll(".terminal-center .terminal-chart-block .candle-chart"));
        const hasMainChart = Boolean(document.querySelector(".terminal-center .terminal-chart-block:not(.chainlink) .candle-chart"));
        const hasMainPlaceholder = Boolean(document.querySelector(".terminal-center .terminal-chart-block:not(.chainlink) .chart-empty"));
        const hasChainlinkChart = Boolean(document.querySelector(".terminal-chart-block.chainlink .candle-chart"));
        const hasChainlinkPlaceholder = Boolean(document.querySelector(".terminal-chart-block.chainlink .chart-empty"));
        if ((!hasMainChart && !hasMainPlaceholder) || (!hasChainlinkChart && !hasChainlinkPlaceholder)) {
          throw new Error(label + ": expected chart or placeholder blocks, got " + JSON.stringify({
            charts: charts.length,
            hasMainChart,
            hasMainPlaceholder,
            hasChainlinkChart,
            hasChainlinkPlaceholder
          }));
        }
        for (const [index, chart] of charts.entries()) {
          const rect = chart.getBoundingClientRect();
          if (rect.width <= 0 || rect.height <= 0) {
            throw new Error(label + ": invalid chart " + index + " size " + JSON.stringify({
              width: rect.width,
              height: rect.height
            }));
          }
        }
        const overflowFree =
          root.scrollWidth === root.clientWidth &&
          root.scrollHeight === root.clientHeight &&
          body.scrollWidth === body.clientWidth &&
          body.scrollHeight === body.clientHeight;
        if (!overflowFree) {
          throw new Error(label + ": page overflow after chart wheel " + JSON.stringify({
            rootClientWidth: root.clientWidth,
            rootClientHeight: root.clientHeight,
            rootScrollWidth: root.scrollWidth,
            rootScrollHeight: root.scrollHeight,
            bodyClientWidth: body.clientWidth,
            bodyClientHeight: body.clientHeight,
            bodyScrollWidth: body.scrollWidth,
            bodyScrollHeight: body.scrollHeight
          }));
        }
      };
      const assertVisibleCountInRange = (label) => {
        const visibleInput = document.querySelector('input[aria-label="visible candles"]');
        const visibleCount = Number(visibleInput?.value ?? NaN);
        if (!Number.isFinite(visibleCount) || visibleCount < 10 || visibleCount > 200) {
          throw new Error(label + ": visible candle count out of range " + visibleInput?.value);
        }
      };
      const boxesOverlap = (first, second) => {
        return !(
          first.right <= second.left + 0.5 ||
          second.right <= first.left + 0.5 ||
          first.bottom <= second.top + 0.5 ||
          second.bottom <= first.top + 0.5
        );
      };
      const assertPriceOverlayLabels = (label) => {
        const mainChart = document.querySelector(".terminal-center .terminal-chart-block:not(.chainlink) .candle-chart");
        if (!mainChart) {
          throw new Error(label + ": missing main chart for price overlay labels.");
        }
        const ptbText = mainChart.querySelector('[data-overlay-label="ptb"]');
        const btcText = mainChart.querySelector('[data-overlay-label="btc"]');
        const ptbBox = mainChart.querySelector('[data-overlay-label="ptb-box"]');
        const btcBox = mainChart.querySelector('[data-overlay-label="btc-box"]');
        if (!ptbText || !btcText || !ptbBox || !btcBox) {
          throw new Error(label + ": missing PTB/BTC price overlay labels.");
        }
        const ptbLabel = (ptbText.textContent || "").replace(/\\s+/g, " ").trim();
        const btcLabel = (btcText.textContent || "").replace(/\\s+/g, " ").trim();
        if (!ptbLabel.startsWith("PTB ")) {
          throw new Error(label + ": target price label should be PTB, got " + JSON.stringify(ptbLabel));
        }
        if (!btcLabel.startsWith("BTC ")) {
          throw new Error(label + ": current price label should be BTC, got " + JSON.stringify(btcLabel));
        }
        if (boxesOverlap(ptbBox.getBoundingClientRect(), btcBox.getBoundingClientRect())) {
          throw new Error(label + ": PTB/BTC price labels overlap.");
        }
      };

      const chartNodes = () => Array.from(document.querySelectorAll(".terminal-center .terminal-chart-block .candle-chart"));
      const chartReadinessDebug = () => {
        const chartBlocks = Array.from(document.querySelectorAll(".terminal-center .terminal-chart-block")).map((block) => {
          const rect = block.getBoundingClientRect();
          return {
            className: block.className,
            width: Math.round(rect.width),
            height: Math.round(rect.height),
            hasChart: Boolean(block.querySelector(".candle-chart")),
            text: (block.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 160)
          };
        });
        return {
          charts: chartNodes().length,
          expanded: Boolean(document.querySelector(".terminal-center.book-expanded")),
          chartBlocks,
          body: (document.body.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 500)
        };
      };
      const ensureChainlinkChartMode = async () => {
        for (let attempt = 0; attempt < 20; attempt += 1) {
          if (!document.querySelector(".terminal-center.book-expanded")) {
            return;
          }
          const closeButton = Array.from(document.querySelectorAll(".terminal-depth button"))
            .find((button) => (button.textContent || "").trim().toUpperCase() === "CL");
          if (closeButton) {
            closeButton.click();
          }
          await sleep(250);
        }
        throw new Error("Unable to close expanded order book before chart stress: " + JSON.stringify(chartReadinessDebug()));
      };
      const waitForCandlestickCharts = async () => {
        await ensureChainlinkChartMode();
        for (let attempt = 0; attempt < 120; attempt += 1) {
          const hasMainPlaceholder = Boolean(document.querySelector(".terminal-center .terminal-chart-block:not(.chainlink) .chart-empty"));
          const hasChainlinkChart = Boolean(document.querySelector(".terminal-chart-block.chainlink .candle-chart"));
          const hasChainlinkPlaceholder = Boolean(document.querySelector(".terminal-chart-block.chainlink .chart-empty"));
          if (chartNodes().length >= 2) {
            return { hasMainChart: true, hasSecondaryChart: true };
          }
          if (chartNodes().length >= 1 && hasChainlinkPlaceholder) {
            return { hasMainChart: true, hasSecondaryChart: false };
          }
          if (hasMainPlaceholder && (hasChainlinkChart || hasChainlinkPlaceholder)) {
            return { hasMainChart: false, hasSecondaryChart: hasChainlinkChart };
          }
          await sleep(500);
        }
        throw new Error("Expected a main chart plus a CL chart or placeholder before wheel stress: " + JSON.stringify(chartReadinessDebug()));
      };
      const chartAt = (index) => {
        const chart = chartNodes()[index];
        if (!chart) throw new Error("Missing trade chart index " + index);
        return chart;
      };
      const getVisibleCount = () => {
        const visibleInput = document.querySelector('input[aria-label="visible candles"]');
        return Number(visibleInput?.value ?? NaN);
      };
      const getYZooms = () => chartNodes().map((chart) => Number(chart.dataset.yZoom ?? NaN));
      const activeInterval = () => {
        const button = Array.from(document.querySelectorAll(".chart-toolbar.compact button"))
          .find((node) => node.classList.contains("on") && ["30s", "1m", "5m", "15m", "1h"].includes((node.textContent || "").trim()));
        return (button?.textContent || "1m").trim();
      };
      const defaultVisibleForInterval = (interval) => interval === "1h" || interval === "15m" ? 24 : interval === "5m" ? 30 : interval === "30s" ? 50 : 60;
      const assertSyncedYZoom = (label) => {
        const zooms = getYZooms();
        if (zooms.length < 1 || zooms.some((value) => !Number.isFinite(value))) {
          throw new Error(label + ": missing chart y zoom probes " + JSON.stringify(zooms));
        }
        if (zooms.length >= 2 && Math.abs(zooms[0] - zooms[1]) > 0.002) {
          throw new Error(label + ": chart y zooms are not synchronized " + JSON.stringify(zooms));
        }
      };
      const assertResetState = (label) => {
        const expectedVisible = defaultVisibleForInterval(activeInterval());
        const visibleCount = getVisibleCount();
        if (visibleCount !== expectedVisible) {
          throw new Error(label + ": expected reset visible count " + expectedVisible + " but got " + visibleCount);
        }
        assertSyncedYZoom(label);
        const zooms = getYZooms();
        if (zooms.some((value) => Math.abs(value - 1) > 0.002)) {
          throw new Error(label + ": expected reset y zoom 1 but got " + JSON.stringify(zooms));
        }
      };
      const spinChart = async (chartIndex, deltaY, shiftKey, count, ctrlKey = false) => {
        for (let index = 0; index < count; index += 1) {
          const chart = chartAt(chartIndex);
          const chartRect = chart.getBoundingClientRect();
          const chartPoint = {
            clientX: chartRect.left + chartRect.width * 0.42,
            clientY: chartRect.top + chartRect.height * 0.48
          };
          chart.dispatchEvent(new WheelEvent("wheel", {
            deltaY,
            shiftKey,
            ctrlKey,
            bubbles: true,
            cancelable: true,
            clientX: chartPoint.clientX,
            clientY: chartPoint.clientY
          }));
          await sleep(12);
          if (index < 3 || index % 10 === 9) {
            assertPageStable("chart " + chartIndex + " wheel stress " + deltaY + " shift=" + shiftKey + " step=" + index);
            assertVisibleCountInRange("chart " + chartIndex + " wheel stress " + deltaY + " shift=" + shiftKey + " step=" + index);
            assertSyncedYZoom("chart " + chartIndex + " wheel stress step=" + index);
          }
        }
        await sleep(320);
        assertPageStable("chart " + chartIndex + " wheel stress " + deltaY + " shift=" + shiftKey + " complete");
        assertVisibleCountInRange("chart " + chartIndex + " wheel stress " + deltaY + " shift=" + shiftKey + " complete");
        assertSyncedYZoom("chart " + chartIndex + " wheel stress complete");
      };
      const doubleClickChart = async (chartIndex) => {
        const chart = chartAt(chartIndex);
        const rect = chart.getBoundingClientRect();
        chart.dispatchEvent(new MouseEvent("dblclick", {
          bubbles: true,
          cancelable: true,
          clientX: rect.left + rect.width * 0.5,
          clientY: rect.top + rect.height * 0.5
        }));
        await sleep(320);
      };
      const hoverChart = async (chartIndex) => {
        const chart = chartAt(chartIndex);
        const hitbox = chart.querySelector(".chart-hover-hitbox");
        if (!hitbox) {
          throw new Error("Missing chart hover hitbox " + chartIndex);
        }
        const rect = chart.getBoundingClientRect();
        const clientX = rect.left + rect.width * 0.52;
        const clientY = rect.top + rect.height * 0.48;
        hitbox.dispatchEvent(new MouseEvent("mouseenter", { bubbles: true, cancelable: true, clientX, clientY }));
        hitbox.dispatchEvent(new MouseEvent("mousemove", { bubbles: true, cancelable: true, clientX, clientY }));
        await sleep(80);
      };

      const chartReadiness = await waitForCandlestickCharts();
      const hasSecondaryChart = chartReadiness.hasSecondaryChart;
      for (let attempt = 0; attempt < 20; attempt += 1) {
        if (document.querySelector('[data-overlay-label="ptb"]') && document.querySelector('[data-overlay-label="btc"]')) {
          break;
        }
        await sleep(250);
      }
      assertPageStable("before wheel stress");
      if (!chartReadiness.hasMainChart) {
        const tradeText = (document.body.textContent || "").replace(/\\s+/g, " ");
        if (!tradeText.includes("B5")) {
          throw new Error("Trade page did not render the B5 chart placeholder.");
        }
        if ((window.__uiErrors || []).length > 0) {
          throw new Error("Frontend runtime errors with chart placeholders: " + JSON.stringify(window.__uiErrors));
        }
        return;
      }
      assertSyncedYZoom("before wheel stress");
      assertPriceOverlayLabels("before wheel stress");
      const initialVisibleCount = getVisibleCount();
      await hoverChart(0);
      await spinChart(0, -120, false, 3);
      assertPriceOverlayLabels("after hovered main chart three-wheel stress");
      if (hasSecondaryChart) {
        await hoverChart(1);
        await spinChart(1, -120, false, 3);
        assertPriceOverlayLabels("after hovered CL chart three-wheel stress");
      }
      await spinChart(0, -120, false, 5);
      if (!(getVisibleCount() < initialVisibleCount)) {
        throw new Error("Main chart wheel-up did not zoom in by reducing visible candles.");
      }
      await spinChart(0, 120, false, 40);
      if (hasSecondaryChart) {
        const beforeChainlinkWheelVisibleCount = getVisibleCount();
        await spinChart(1, -120, false, 3);
        if (!(getVisibleCount() < beforeChainlinkWheelVisibleCount)) {
          throw new Error("CL chart wheel-up did not update the shared visible candle count.");
        }
      }
      const beforeMainShiftZoom = getYZooms();
      await spinChart(0, -120, true, 4);
      const afterMainShiftZoom = getYZooms();
      if (!(afterMainShiftZoom[0] > beforeMainShiftZoom[0])) {
        throw new Error("Main chart Shift+wheel did not zoom the price axis in.");
      }
      if (hasSecondaryChart && !(afterMainShiftZoom[1] > beforeMainShiftZoom[1])) {
        throw new Error("Main chart Shift+wheel did not zoom both price axes in.");
      }
      assertPriceOverlayLabels("after main Shift+wheel");
      if (hasSecondaryChart) {
        await spinChart(1, 120, true, 4);
      } else {
        await spinChart(0, 120, true, 4);
      }
      const afterChainlinkShiftZoom = getYZooms();
      if (!(afterChainlinkShiftZoom[0] < afterMainShiftZoom[0])) {
        throw new Error("Shift+wheel did not zoom the price axis out.");
      }
      if (hasSecondaryChart && !(afterChainlinkShiftZoom[1] < afterMainShiftZoom[1])) {
        throw new Error("CL chart Shift+wheel did not zoom both price axes out.");
      }
      assertPriceOverlayLabels("after secondary Shift+wheel");
      await spinChart(0, 5000, false, 3);
      await spinChart(0, -5000, false, 3);
      await spinChart(hasSecondaryChart ? 1 : 0, 5000, true, 3);
      await spinChart(hasSecondaryChart ? 1 : 0, -5000, true, 3);
      await spinChart(0, -120, false, 3, true);
      await spinChart(hasSecondaryChart ? 1 : 0, 120, false, 3, true);
      assertPriceOverlayLabels("after extreme and ctrl wheel stress");
      await doubleClickChart(hasSecondaryChart ? 1 : 0);
      assertResetState("secondary double-click reset");
      assertPriceOverlayLabels("after secondary double-click reset");
      await spinChart(0, -120, false, 4);
      await spinChart(0, -120, true, 4);
      await doubleClickChart(0);
      assertResetState("main double-click reset");
      assertPriceOverlayLabels("after main double-click reset");
      for (const interval of ["30s", "1m", "5m", "15m", "1h"]) {
        const intervalButton = Array.from(document.querySelectorAll(".chart-toolbar.compact button"))
          .find((button) => (button.textContent || "").trim() === interval);
        if (!intervalButton) throw new Error("Missing " + interval + " interval button after wheel stress.");
        intervalButton.click();
        await sleep(250);
        if (!intervalButton.classList.contains("on")) {
          throw new Error("Trade page controls stopped responding after " + interval + " interval click.");
        }
        assertPageStable("after interval " + interval);
        assertVisibleCountInRange("after interval " + interval);
        assertResetState("after interval " + interval);
        assertPriceOverlayLabels("after interval " + interval);
        await spinChart(0, -120, false, 2);
        if (hasSecondaryChart) {
          await spinChart(1, -120, true, 2);
        } else {
          await spinChart(0, -120, true, 2);
        }
        assertSyncedYZoom("after interval " + interval + " follow-up wheels");
        assertPriceOverlayLabels("after interval " + interval + " follow-up wheels");
        await doubleClickChart(0);
        assertResetState("after interval " + interval + " follow-up reset");
        assertPriceOverlayLabels("after interval " + interval + " follow-up reset");
      }
      if (!document.querySelector(".terminal-page") || !document.querySelector(".terminal-order") || !document.querySelector(".chart-toolbar.compact")) {
        throw new Error("Trade page key modules disappeared after wheel stress.");
      }
      const tradeText = (document.body.textContent || "").replace(/\\s+/g, " ");
      if (/Polymarket|\\bodds\\b|赔率/i.test(tradeText)) {
        throw new Error("Trade page still exposes old Polymarket/odds wording.");
      }
      if (!tradeText.includes("B5")) {
        throw new Error("Trade page did not render the B5 section.");
      }
      const topRightLinkText = Array.from(document.querySelectorAll(".terminal-top-right a"))
        .map((node) => (node.textContent || "").trim())
        .join(" ");
      if (/P_M|Binance/.test(topRightLinkText)) {
        throw new Error("Trade page still renders top-right external links.");
      }
      const centSign = String.fromCharCode(162);
      const orderOddsText = Array.from(document.querySelectorAll(".order-odds button"))
        .map((button) => (button.textContent || "").trim())
        .join(" ");
      if (!orderOddsText.includes(centSign)) {
        throw new Error("Token display prices did not render in cents.");
      }
      const kindButtonsText = Array.from(document.querySelectorAll(".order-kind-segment button"))
        .map((button) => (button.textContent || "").replace(/\s+/g, " ").trim())
        .join(" ");
      if (!kindButtonsText.includes("MARKET FOK") || !kindButtonsText.includes("LIMIT GTC")) {
        throw new Error("Order kind segment did not render MARKET/FOK and LIMIT/GTC.");
      }
      clickAny("LIMIT");
      await sleep(180);
      const limitInput = Array.from(document.querySelectorAll(".terminal-order .terminal-input input")).at(-1);
      if (!limitInput) throw new Error("Missing limit price input.");
      setReactInputValue(limitInput, "54");
      await sleep(180);
      if ((limitInput.value || "").trim() !== "54") {
        throw new Error("Limit price input did not stay in cents.");
      }
      if ((window.__uiErrors || []).length > 0) {
        throw new Error("Frontend runtime errors after wheel stress: " + JSON.stringify(window.__uiErrors));
      }

      clickAny("Log");
      await sleep(1000);

      const systemButtons = Array.from(document.querySelectorAll(".log-system-tabs button"));
      if (systemButtons.length < 4) throw new Error("Missing log system tabs.");
      for (const index of [1, 2, 3, 0]) {
        systemButtons[index].click();
        await sleep(700);
        if (!systemButtons[index].classList.contains("active")) {
          throw new Error("Log system tab did not become active.");
        }
      }

      const selects = Array.from(document.querySelectorAll(".filter-grid select"));
      const roleSelect = selects[0];
      const userSelect = selects[1];
      if (!roleSelect || !userSelect) throw new Error("Missing role/user filters.");
      roleSelect.value = "Admin";
      roleSelect.dispatchEvent(new Event("change", { bubbles: true }));
      await sleep(300);
      const adminOptions = Array.from(userSelect.options).map((option) => option.textContent || "");
      if (adminOptions.some((text) => text.includes("Tester") || text.includes("Senior Tester"))) {
        throw new Error("User filter did not narrow to Admin role.");
      }
      roleSelect.value = "Tester";
      roleSelect.dispatchEvent(new Event("change", { bubbles: true }));
      await sleep(300);
      const testerOptions = Array.from(userSelect.options).map((option) => option.textContent || "");
      if (!testerOptions.some((text) => text.includes("Tester")) || testerOptions.some((text) => text.includes("Admin"))) {
        throw new Error("User filter did not narrow to Tester role.");
      }

      clickAny("Log Info");
      await sleep(300);
      if (!document.body.textContent.includes("actionType") || !document.body.textContent.includes("Main Fields")) {
        throw new Error("Log info panel did not show facets.");
      }

      const exportButton = byText("Export");
      if (!exportButton) throw new Error("Missing export button on log page.");
      exportButton.click();
      await sleep(500);
      if (!document.body.textContent.includes("Export Wizard")) {
        throw new Error("Export wizard did not open.");
      }
      clickAny("Close");
      await sleep(300);
      clickAny("Users");
      let bulkButton;
      for (let attempt = 0; attempt < 20; attempt += 1) {
        bulkButton = byText("Bulk Register");
        if (bulkButton && !bulkButton.disabled) {
          break;
        }
        await sleep(250);
      }
      if (!bulkButton) throw new Error("Missing bulk registration button.");
      bulkButton.click();
      for (let attempt = 0; attempt < 20 && !document.body.textContent.includes("CSV / TSV"); attempt += 1) {
        await sleep(250);
      }
      if (!document.body.textContent.includes("CSV / TSV")) {
        throw new Error("Bulk registration dialog did not open.");
      }
      return {
        title: document.title,
        trade1440: ${JSON.stringify({ width: 1440, height: 900 })},
        trade1920: ${JSON.stringify({ width: 1920, height: 1080 })},
        hasExportWizard: true,
        hasBulkDialog: true,
        hasLogFacets: true,
        hasRoleUserFilter: true
      };
    })();
  `);
  await assertZoomLocked(window, "after chart wheel stress");
  console.log(JSON.stringify(result, null, 2));
  const restoreLogin = await fetch(`${API_URL}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: "admin", password: "admin123" })
  }).then((response) => response.json());
  if (restoreLogin.token) {
    await fetch(`${API_URL}/api/me/language`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${restoreLogin.token}` },
      body: JSON.stringify({ language: "zh-CN" })
    });
  }
  await window.close();
  app.quit();
  stopManagedProcesses();
}

main().catch(exitWithFailure);
