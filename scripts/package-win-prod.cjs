const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const outputDir = path.join(root, "deploy", "windows-production");
const readmePath = path.join(outputDir, "production-client-readme.md");

function assertInsideRoot(targetPath) {
  const relative = path.relative(root, targetPath);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`Refusing to touch path outside project root: ${targetPath}`);
  }
}

function assertProductionApiBaseUrl(value) {
  if (!value) {
    throw new Error("VITE_API_BASE_URL is required for production client packaging.");
  }
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("VITE_API_BASE_URL must be a valid URL.");
  }
  const hostname = parsed.hostname.toLowerCase();
  if (parsed.protocol !== "https:") {
    throw new Error("VITE_API_BASE_URL must use https:// for the production client.");
  }
  if (hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1" || hostname.endsWith(".localhost")) {
    throw new Error("VITE_API_BASE_URL must not point to localhost for the production client.");
  }
}

function runNodeScript(scriptPath, args, extraEnv = {}) {
  execFileSync(process.execPath, [scriptPath, ...args], {
    cwd: root,
    stdio: "inherit",
    env: {
      ...process.env,
      ...extraEnv,
      CSC_IDENTITY_AUTO_DISCOVERY: "false",
      ELECTRON_BUILDER_DISABLE_PUBLISH: "true",
      ELECTRON_EMBED_BACKEND: "false"
    }
  });
}

assertProductionApiBaseUrl(process.env.VITE_API_BASE_URL);
assertInsideRoot(outputDir);
fs.rmSync(outputDir, { recursive: true, force: true });
fs.mkdirSync(outputDir, { recursive: true });

const npmCli = process.env.npm_execpath;
if (!npmCli) {
  throw new Error("npm_execpath is not available; run this script through npm.");
}

runNodeScript(npmCli, ["run", "build"], {
  VITE_API_BASE_URL: process.env.VITE_API_BASE_URL
});
runNodeScript(require.resolve("electron-builder/cli.js"), [
  "--win",
  "nsis",
  "--x64",
  "--publish",
  "never",
  "-c.appId=com.local.btcpapertrading",
  "-c.productName=BTC Paper Trading",
  "-c.directories.output=deploy/windows-production",
  "-c.win.artifactName=BTC Paper Trading Setup.${ext}",
  "-c.nsis.shortcutName=BTC Paper Trading"
]);

for (const entry of fs.readdirSync(outputDir)) {
  if (entry === path.basename(readmePath) || entry.toLowerCase().endsWith(".exe")) {
    continue;
  }
  fs.rmSync(path.join(outputDir, entry), { recursive: true, force: true });
}

fs.writeFileSync(
  readmePath,
  `# BTC Paper Trading Production Client

This installer is the production C/S client. It does not start a local memory backend by default.

- API base URL: ${process.env.VITE_API_BASE_URL}
- WebSocket URLs are derived from the API base URL and use WSS for HTTPS origins.
- Use this installer only after the Docker + Caddy server is ready.
`,
  "utf8"
);

console.log(`\nWindows production installer is ready: ${outputDir}`);
