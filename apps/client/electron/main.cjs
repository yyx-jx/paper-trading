const { app, BrowserWindow, dialog, ipcMain } = require("electron");
const fs = require("node:fs/promises");
const path = require("node:path");

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

  const devServerUrl = process.env.VITE_DEV_SERVER_URL || "http://127.0.0.1:5173";
  if (!app.isPackaged) {
    win.loadURL(devServerUrl);
    win.webContents.openDevTools({ mode: "detach" });
    return;
  }

  win.loadFile(path.resolve(__dirname, "../../../dist/renderer/index.html"));
}

app.whenReady().then(() => {
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

  createWindow();
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});
