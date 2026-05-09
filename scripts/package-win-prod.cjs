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
  const allowInsecureHttp = process.env.ALLOW_INSECURE_PROD_HTTP === "true";
  if (parsed.protocol !== "https:" && !(allowInsecureHttp && parsed.protocol === "http:")) {
    throw new Error(
      "VITE_API_BASE_URL must use https:// for the production client. Set ALLOW_INSECURE_PROD_HTTP=true only for a temporary HTTP rollout."
    );
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

function createPortableZip() {
  const unpackedDir = path.join(outputDir, "win-unpacked");
  const exePath = path.join(unpackedDir, "BTC Paper Trading.exe");
  const zipPath = path.join(outputDir, "BTC Paper Trading Portable.zip");
  if (!fs.existsSync(exePath)) {
    throw new Error("electron-builder did not produce win-unpacked/BTC Paper Trading.exe.");
  }
  if (process.platform !== "win32") {
    console.warn("Portable zip fallback is only automated on Windows; leaving win-unpacked as the deliverable.");
    return;
  }
  let lastError;
  for (let attempt = 1; attempt <= 12; attempt += 1) {
    try {
      fs.rmSync(zipPath, { force: true });
      execFileSync("tar", ["-a", "-cf", zipPath, "-C", unpackedDir, "."], {
        cwd: root,
        stdio: "inherit"
      });
      const listing = execFileSync("tar", ["-tf", zipPath], { cwd: root, encoding: "utf8" });
      if (!listing.split(/\r?\n/).some((entry) => entry === "./BTC Paper Trading.exe" || entry === "BTC Paper Trading.exe")) {
        throw new Error("Portable zip does not contain BTC Paper Trading.exe.");
      }
      return;
    } catch (error) {
      lastError = error;
      console.warn(`Portable zip attempt ${attempt} failed; retrying after file locks settle.`);
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5000);
    }
  }
  throw lastError;
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
let nsisSucceeded = true;
try {
  runNodeScript(require.resolve("electron-builder/cli.js"), [
    "--win",
    "nsis",
    "--x64",
    "--publish",
    "never",
    "-c.appId=com.local.btcpapertrading",
    "-c.productName=BTC Paper Trading",
    `-c.extraMetadata.productionApiBaseUrl=${process.env.VITE_API_BASE_URL}`,
    "-c.directories.output=deploy/windows-production",
    "-c.win.artifactName=BTC Paper Trading Setup.${ext}",
    "-c.nsis.shortcutName=BTC Paper Trading"
  ]);
} catch (error) {
  nsisSucceeded = false;
  console.warn("\nNSIS installer build failed; creating a portable zip from win-unpacked instead.");
  createPortableZip();
}

if (nsisSucceeded) {
  for (const entry of fs.readdirSync(outputDir)) {
    if (entry === path.basename(readmePath) || entry.toLowerCase().endsWith(".exe")) {
      continue;
    }
    fs.rmSync(path.join(outputDir, entry), { recursive: true, force: true });
  }
}

fs.writeFileSync(
  readmePath,
  `# BTC Paper Trading Production Client

This installer is the production C/S client. It does not start a local memory backend by default.

- API base URL: ${process.env.VITE_API_BASE_URL}
- WebSocket URLs are derived from the API base URL and use WSS for HTTPS origins or WS for temporary HTTP origins.
- Temporary HTTP mode: ${process.env.ALLOW_INSECURE_PROD_HTTP === "true" ? "enabled" : "disabled"}
- System proxy bypass: production API host is added to Electron proxy-bypass-list at startup.
- Use this installer only after the Docker + Caddy server is ready.
`,
  "utf8"
);

console.log(`\nWindows production client is ready: ${outputDir}`);
