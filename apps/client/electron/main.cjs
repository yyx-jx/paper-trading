const { app, BrowserWindow, dialog, ipcMain } = require("electron");
const { spawn } = require("node:child_process");
const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const http = require("node:http");
const path = require("node:path");

const LOCAL_API_BASE_URL = "http://127.0.0.1:8787";
let backendProcess;
let backendLogStream;

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
    width: 1600,
    height: 980,
    minWidth: 1280,
    minHeight: 840,
    backgroundColor: "#0b1020",
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
    error instanceof Error ? error.message : String(error)
  );
  app.quit();
});

app.on("before-quit", () => {
  stopBackend();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});
