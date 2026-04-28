const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("paperTradingDesktop", {
  platform: process.platform,
  saveFile(input) {
    return ipcRenderer.invoke("export:save-file", input);
  }
});
