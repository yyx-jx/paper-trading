const { app, BrowserWindow } = require("electron");
const os = require("node:os");
const path = require("node:path");

const APP_URL = process.env.APP_URL || "http://127.0.0.1:5173";
const API_URL = process.env.API_URL || "http://127.0.0.1:8787";

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
        .filter((text) => ["1m", "5m", "15m", "1h"].includes(text));
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

async function main() {
  app.commandLine.appendSwitch("disable-gpu");
  app.setPath("userData", path.join(os.tmpdir(), `paper-trading-ui-check-${process.pid}`));
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
  await window.webContents.executeJavaScript(`
    (async () => {
      const response = await fetch("${API_URL}/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: "admin", password: "admin123" })
      });
      const data = await response.json();
      if (!data.token) throw new Error("Admin login failed in UI check.");
      await fetch("${API_URL}/api/me/language", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: "Bearer " + data.token },
        body: JSON.stringify({ language: "en-US" })
      });
      localStorage.setItem("paper-trading-token", data.token);
    })();
  `);
  await window.loadURL(APP_URL);
  await wait(3000);
  const trade1440 = await evaluateTradeLayout(window, 1440, 900);
  if (!trade1440.hasTradePage || !trade1440.overflowFree || !trade1440.allModulesPresent || !trade1440.monitorCellsFit) {
    throw new Error(`Trade page 1440x900 check failed: ${JSON.stringify(trade1440)}`);
  }
  if (["1m", "5m", "15m", "1h"].some((label) => !trade1440.intervalButtons.includes(label))) {
    throw new Error(`Missing trade interval buttons at 1440x900: ${JSON.stringify(trade1440.intervalButtons)}`);
  }
  const trade1920 = await evaluateTradeLayout(window, 1920, 1080);
  if (!trade1920.hasTradePage || !trade1920.overflowFree || !trade1920.allModulesPresent || !trade1920.monitorCellsFit) {
    throw new Error(`Trade page 1920x1080 check failed: ${JSON.stringify(trade1920)}`);
  }
  const result = await window.webContents.executeJavaScript(`
    (async () => {
      const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
      const buttons = () => Array.from(document.querySelectorAll("button"));
      const byText = (text) => buttons().find((button) => (button.textContent || "").includes(text));
      const clickAny = (...texts) => {
        const button = texts.map(byText).find(Boolean);
        if (!button) throw new Error("Missing button: " + texts.join(" / "));
        button.click();
      };

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
      await sleep(1000);
      const bulkButton = byText("Bulk Register");
      if (!bulkButton) throw new Error("Missing bulk registration button.");
      bulkButton.click();
      await sleep(500);
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
}

main().catch((error) => {
  console.error(error);
  app.quit();
  process.exitCode = 1;
});
