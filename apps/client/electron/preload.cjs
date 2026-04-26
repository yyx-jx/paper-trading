const { contextBridge } = require("electron");

contextBridge.exposeInMainWorld("paperTradingDesktop", {
  platform: process.platform
});

