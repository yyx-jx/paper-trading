const { app, BrowserWindow } = require("electron");
const os = require("node:os");
const path = require("node:path");

const APP_URL = process.env.APP_URL || "http://127.0.0.1:5173";
const API_URL = process.env.API_URL || "http://127.0.0.1:8787";

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
