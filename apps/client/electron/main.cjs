const { app, BrowserWindow, dialog, ipcMain } = require("electron");
const { spawn } = require("node:child_process");
const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const { startProductionProxyServer } = require("./production-proxy.cjs");

const LOCAL_API_BASE_URL = "http://127.0.0.1:8787";
const PRODUCTION_PROXY_PORT = 18787;
let backendProcess;
let backendLogStream;
let productionProxyServer;
let productionProxyLocalAddress;

function packagedMetadata() {
  try {
    return require(path.resolve(__dirname, "../../../package.json"));
  } catch {
    return {};
  }
}

function appVersion() {
  return String(packagedMetadata().version || "0.0.0");
}

function windowTitle() {
  return `HT Paper Trading v${appVersion()}`;
}

function productionApiBaseUrl() {
  const metadata = packagedMetadata();
  return String(metadata.productionApiBaseUrl || process.env.VITE_API_BASE_URL || "").trim();
}

function productionClientConnectionMode() {
  const metadata = packagedMetadata();
  const mode = String(metadata.productionClientConnectionMode || process.env.PROD_CLIENT_CONNECTION_MODE || "proxy")
    .trim()
    .toLowerCase();
  return mode === "direct" ? "direct" : "proxy";
}

function configureProxyBypass() {
  const apiBaseUrl = productionApiBaseUrl();
  if (!apiBaseUrl) {
    return;
  }

  let parsed;
  try {
    parsed = new URL(apiBaseUrl);
  } catch {
    return;
  }

  const bypassRules = new Set(["localhost", "127.0.0.1", "<local>", parsed.hostname]);
  if (parsed.port) {
    bypassRules.add(`${parsed.hostname}:${parsed.port}`);
  }

  app.commandLine.appendSwitch("proxy-bypass-list", Array.from(bypassRules).join(";"));
  app.commandLine.appendSwitch("no-proxy-server");
}

function activeIpv4Addresses() {
  return Object.values(os.networkInterfaces())
    .flat()
    .filter((item) => item && item.family === "IPv4" && !item.internal)
    .map((item) => item.address);
}

function requestProductionHealth(target, localAddress) {
  return new Promise((resolve) => {
    const request = http.get(
      {
        hostname: target.hostname,
        port: target.port || 80,
        path: "/api/health/live",
        localAddress,
        timeout: 2500
      },
      (response) => {
        response.resume();
        resolve(Boolean(response.statusCode && response.statusCode >= 200 && response.statusCode < 500));
      }
    );
    request.on("timeout", () => {
      request.destroy();
      resolve(false);
    });
    request.on("error", () => resolve(false));
  });
}

async function pickProductionLocalAddress(target) {
  if (await requestProductionHealth(target)) {
    return undefined;
  }
  for (const localAddress of activeIpv4Addresses()) {
    if (await requestProductionHealth(target, localAddress)) {
      return localAddress;
    }
  }
  return undefined;
}

async function startProductionProxy() {
  const apiBaseUrl = productionApiBaseUrl();
  if (!apiBaseUrl || shouldEmbedBackend() || productionClientConnectionMode() !== "proxy") {
    return;
  }
  const target = new URL(apiBaseUrl);
  productionProxyLocalAddress = await pickProductionLocalAddress(target);
  const proxy = await startProductionProxyServer({
    targetUrl: apiBaseUrl,
    host: "127.0.0.1",
    port: PRODUCTION_PROXY_PORT,
    localAddress: productionProxyLocalAddress
  });
  productionProxyServer = proxy.server;
}

function redactNetworkAddresses(value) {
  return String(value ?? "")
    .replace(/\b(?:https?|wss?):\/\/[^\s<>"'`]+/gi, "[service address]")
    .replace(/\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?::\d{1,5})?\b/g, "[service address]");
}

function asarUnpackedPath(filePath) {
  if (!app.isPackaged) {
    return filePath;
  }
  return filePath.replace(`${path.sep}app.asar${path.sep}`, `${path.sep}app.asar.unpacked${path.sep}`);
}

function backendEntryPath() {
  return asarUnpackedPath(path.resolve(__dirname, "../../../dist/server/index.js"));
}

function rendererEntryPath() {
  return path.resolve(__dirname, "../../../dist/renderer/index.html");
}

function resolveAppIconPath() {
  const packagedIcon = path.join(process.resourcesPath, "app-icon.png");
  if (app.isPackaged && fsSync.existsSync(packagedIcon)) {
    return packagedIcon;
  }
  return path.resolve(__dirname, "../assets/app-icon.png");
}

function checkBackendHealth() {
  return new Promise((resolve) => {
    const request = http.get(`${LOCAL_API_BASE_URL}/health`, (response) => {
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

async function waitForBackend(timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await checkBackendHealth()) {
      return;
    }
    if (backendProcess?.exitCode !== null && typeof backendProcess?.exitCode !== "undefined") {
      throw new Error(`Backend exited before it became ready. Exit code: ${backendProcess.exitCode}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("Backend did not become ready in time.");
}

function shouldEmbedBackend() {
  const override = String(process.env.ELECTRON_EMBED_BACKEND ?? "").trim().toLowerCase();
  if (override === "true" || override === "1" || override === "yes") {
    return true;
  }
  if (override === "false" || override === "0" || override === "no") {
    return false;
  }
  return app.isPackaged && app.getName().toLowerCase().includes("test");
}

async function startPackagedBackend() {
  const runtimeDir = path.join(app.getPath("userData"), "test-runtime");
  fsSync.mkdirSync(runtimeDir, { recursive: true });

  const logPath = path.join(runtimeDir, "backend.log");
  backendLogStream = fsSync.createWriteStream(logPath, { flags: "a" });
  backendLogStream.write(`\n[${new Date().toISOString()}] Starting packaged backend\n`);

  backendProcess = spawn(process.execPath, [backendEntryPath()], {
    cwd: runtimeDir,
    env: {
      ...process.env,
      ELECTRON_RUN_AS_NODE: "1",
      NODE_ENV: "production",
      PORT: "8787",
      MATCHING_SERVICE_PORT: "8788",
      MATCHING_SERVICE_URL: "http://127.0.0.1:8788",
      MATCHING_SERVICE_TIMEOUT_MS: "4000",
      EMBEDDED_MATCHING_SERVICE: "true",
      PERSISTENCE_MODE: "memory",
      CHAINLINK_ENABLED: "false",
      UPSTREAM_PROXY_URL: "",
      JWT_SECRET: "btc-paper-trading-test-secret",
      DATABASE_URL: "",
      REDIS_URL: "",
      NODE_OPTIONS: [process.env.NODE_OPTIONS, "--max-old-space-size=1536"].filter(Boolean).join(" ")
    },
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true
  });

  backendProcess.stdout?.pipe(backendLogStream, { end: false });
  backendProcess.stderr?.pipe(backendLogStream, { end: false });
  backendProcess.once("exit", (code, signal) => {
    backendLogStream?.write(`[${new Date().toISOString()}] Backend exited: code=${code} signal=${signal}\n`);
  });

  await waitForBackend();
}

function stopBackend() {
  if (backendProcess && backendProcess.exitCode === null) {
    backendProcess.kill();
  }
  backendProcess = undefined;
  backendLogStream?.end();
  backendLogStream = undefined;
}

function stopProductionProxy() {
  productionProxyServer?.close();
  productionProxyServer = undefined;
}

function lockRendererZoom(win) {
  const resetZoom = () => {
    if (!win.isDestroyed()) {
      win.webContents.setZoomFactor(1);
    }
  };

  win.webContents.setVisualZoomLevelLimits(1, 1).catch(() => {});
  resetZoom();
  win.webContents.on("did-finish-load", resetZoom);
  win.webContents.on("zoom-changed", (event) => {
    event.preventDefault();
    resetZoom();
  });
  win.webContents.on("before-input-event", (event, input) => {
    const key = String(input.key || "").toLowerCase();
    const isZoomShortcut =
      (input.control || input.meta) &&
      (key === "+" || key === "=" || key === "-" || key === "_" || key === "0" || key === "numadd" || key === "numsub");
    if (isZoomShortcut) {
      event.preventDefault();
      resetZoom();
    }
  });
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1440,
    height: 900,
    minWidth: 1280,
    minHeight: 760,
    title: windowTitle(),
    backgroundColor: "#0b1020",
    icon: resolveAppIconPath(),
    webPreferences: {
      preload: path.join(__dirname, "preload.cjs"),
      contextIsolation: true,
      nodeIntegration: false
    }
  });
  lockRendererZoom(win);

  const devServerUrl = process.env.VITE_DEV_SERVER_URL || "http://127.0.0.1:5173";
  if (!app.isPackaged) {
    win.loadURL(devServerUrl);
    win.webContents.openDevTools({ mode: "detach" });
    return;
  }

  win.loadFile(rendererEntryPath());
}

app.whenReady().then(async () => {
  ipcMain.handle("export:save-file", async (_event, input) => {
    const defaultFileName =
      typeof input?.defaultFileName === "string" && input.defaultFileName.trim()
        ? input.defaultFileName.trim()
        : `paper-trading-export-${new Date().toISOString().slice(0, 10)}.zip`;
    const bytes = input?.bytes;
    if (!bytes) {
      throw new Error("No export bytes were provided.");
    }
    const result = await dialog.showSaveDialog({
      title: "Save Log Export",
      buttonLabel: "Save Export",
      defaultPath: defaultFileName,
      properties: ["createDirectory"],
      filters: [
        { name: "ZIP Archive", extensions: ["zip"] },
        { name: "All Files", extensions: ["*"] }
      ]
    });
    if (result.canceled || !result.filePath) {
      return { canceled: true };
    }
    await fs.writeFile(result.filePath, Buffer.from(bytes));
    return {
      canceled: false,
      filePath: result.filePath
    };
  });

  if (shouldEmbedBackend()) {
    await startPackagedBackend();
  } else {
    await startProductionProxy();
  }

  createWindow();
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
}).catch((error) => {
  dialog.showErrorBox(
    "BTC Paper Trading Test failed to start",
    redactNetworkAddresses(error instanceof Error ? error.message : String(error))
  );
  app.quit();
});

configureProxyBypass();

app.on("before-quit", () => {
  stopBackend();
  stopProductionProxy();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});
