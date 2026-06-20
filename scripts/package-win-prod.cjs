const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const outputDir = path.join(root, "deploy", "windows-production");
const readmePath = path.join(outputDir, "production-client-readme.md");
const redactedApiBaseUrl = "http://<PRODUCTION_HOST>:10001";
const rendererApiBaseUrl = "http://127.0.0.1:18787";
const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const versionOverride = String(process.env.PACKAGE_VERSION_OVERRIDE || "").trim();
const packageVersion = versionOverride || String(packageJson.version || "0.0.0");
const packageVersionForMetadata = packageVersion.replace(/_/g, "-");
const connectionMode = String(process.env.PROD_CLIENT_CONNECTION_MODE || "proxy").trim().toLowerCase();
const productionArtifactName = `BTC Paper Trading Setup ${packageVersion}${connectionMode === "direct" ? " Direct" : ""}.\${ext}`;
const productionInstallerName = productionArtifactName.replace("${ext}", "exe");

function assertPackageVersion(value) {
  if (!/^\d+\.\d+\.\d+(?:[-_][0-9A-Za-z.-]+)?$/.test(value)) {
    throw new Error(`Invalid package version override: ${value}`);
  }
}

function rendererApiBaseUrlForMode() {
  return connectionMode === "direct" ? process.env.VITE_API_BASE_URL : rendererApiBaseUrl;
}

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

function runLocalBuilds() {
  runNodeScript(require.resolve("tsup/dist/cli-default.js"), ["--config", "tsup.server.config.ts"]);
  runNodeScript(require.resolve("tsup/dist/cli-default.js"), ["--config", "tsup.matching.config.ts"]);
  const viteBinPath = path.join(path.dirname(require.resolve("vite/package.json")), "bin", "vite.js");
  runNodeScript(viteBinPath, ["build", "--config", "apps/client/vite.config.ts"], {
    VITE_API_BASE_URL: rendererApiBaseUrlForMode()
  });
}

function createPortableZip() {
  const unpackedDir = path.join(outputDir, "win-unpacked");
  const exePath = path.join(unpackedDir, "BTC Paper Trading.exe");
  const zipPath = path.join(outputDir, `BTC Paper Trading Portable ${packageVersion}.zip`);
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
assertPackageVersion(packageVersion);
if (!["proxy", "direct"].includes(connectionMode)) {
  throw new Error("PROD_CLIENT_CONNECTION_MODE must be proxy or direct.");
}
assertInsideRoot(outputDir);
fs.rmSync(outputDir, { recursive: true, force: true });
fs.mkdirSync(outputDir, { recursive: true });
runLocalBuilds();
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
    "-c.win.signAndEditExecutable=false",
    `-c.extraMetadata.version=${packageVersionForMetadata}`,
    `-c.extraMetadata.productionApiBaseUrl=${process.env.VITE_API_BASE_URL}`,
    "-c.directories.output=deploy/windows-production",
    `-c.win.artifactName=${productionArtifactName}`,
    "-c.nsis.shortcutName=BTC Paper Trading",
    `-c.extraMetadata.productionClientConnectionMode=${connectionMode}`
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

- API base URL shown in this README: ${redactedApiBaseUrl}
- Client version: ${packageVersion}
- Installer filename: ${productionInstallerName}
- Client runtime URL: ${connectionMode === "direct" ? redactedApiBaseUrl : rendererApiBaseUrl}
- Connection mode: ${connectionMode}
- WebSocket URLs are derived from the API base URL and use WSS for HTTPS origins or WS for temporary HTTP origins.
- Temporary HTTP mode: ${process.env.ALLOW_INSECURE_PROD_HTTP === "true" ? "enabled" : "disabled"}
- Proxy mode: ${connectionMode === "proxy" ? "Electron starts a local proxy on 127.0.0.1 and forwards traffic to the production server." : "disabled; renderer connects to the production API directly."}
- Use this installer only after the Docker + Caddy server is ready.
`,
  "utf8"
);

console.log(`\nWindows production client is ready: ${outputDir}`);
