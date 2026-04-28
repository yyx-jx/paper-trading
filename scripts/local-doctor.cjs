const fs = require("node:fs");
const path = require("node:path");
const { execSync } = require("node:child_process");

const root = process.cwd();
const envPath = path.join(root, ".env");
const examplePath = path.join(root, ".env.example");

function parseEnv(filePath) {
  const result = {};
  if (!fs.existsSync(filePath)) {
    return result;
  }
  const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const separator = trimmed.indexOf("=");
    if (separator <= 0) {
      continue;
    }
    const key = trimmed.slice(0, separator);
    const value = trimmed.slice(separator + 1);
    result[key] = value;
  }
  return result;
}

function run(label, command) {
  try {
    const stdout = execSync(command, {
      cwd: root,
      stdio: ["ignore", "pipe", "pipe"]
    }).toString("utf8");
    return { ok: true, label, output: stdout.trim() };
  } catch (error) {
    const output = error.stderr ? error.stderr.toString("utf8").trim() : error.message;
    return { ok: false, label, output };
  }
}

function statusLine(ok, text) {
  return `${ok ? "[OK]" : "[WARN]"} ${text}`;
}

const env = parseEnv(envPath);
const exampleEnv = parseEnv(examplePath);
const effectiveEnv = Object.keys(env).length > 0 ? env : exampleEnv;

const dockerInfo = run("docker info", "docker info");
const dockerComposePs = run("docker compose ps", "docker compose -f docker-compose.local.yml ps");
const placeholderPrimary =
  !effectiveEnv.CHAINLINK_RPC_URL ||
  effectiveEnv.CHAINLINK_RPC_URL.includes("YOUR_PRIMARY_KEY") ||
  effectiveEnv.CHAINLINK_RPC_URL.includes("YOUR_ALCHEMY_KEY");
const placeholderFallback =
  !effectiveEnv.CHAINLINK_FALLBACK_RPC_URLS ||
  effectiveEnv.CHAINLINK_FALLBACK_RPC_URLS.includes("YOUR_FALLBACK_KEY") ||
  effectiveEnv.CHAINLINK_FALLBACK_RPC_URLS.includes("YOUR_INFURA_KEY");
const preferredPrimary = effectiveEnv.CHAINLINK_RPC_URL?.includes("alchemy.com");
const preferredFallback = effectiveEnv.CHAINLINK_FALLBACK_RPC_URLS?.includes("infura.io");
const embeddedMatching = effectiveEnv.EMBEDDED_MATCHING_SERVICE !== "false";
const chainlinkEnabled = effectiveEnv.CHAINLINK_ENABLED !== "false";
const upstreamProxyUrl = effectiveEnv.UPSTREAM_PROXY_URL ?? "";
const chainlinkPollMs = Number(effectiveEnv.CHAINLINK_POLL_MS ?? 0);

console.log("Local Doctor");
console.log("============");
console.log(statusLine(fs.existsSync(envPath), ".env present at repo root"));
if (!fs.existsSync(envPath)) {
  console.log("  run: npm run setup:env");
}
console.log(statusLine(dockerInfo.ok, "Docker Desktop engine reachable"));
if (!dockerInfo.ok) {
  console.log("  start Docker Desktop before running local dependencies.");
}
console.log(
  statusLine(
    !chainlinkEnabled,
    "CHAINLINK_ENABLED=false for local Binance + Polymarket testing"
  )
);
console.log(statusLine(upstreamProxyUrl === "http://127.0.0.1:7897", "UPSTREAM_PROXY_URL is set to the local proxy (http://127.0.0.1:7897)"));
if (chainlinkEnabled) {
  console.log(statusLine(!placeholderPrimary, "CHAINLINK_RPC_URL is using a real endpoint"));
  console.log(statusLine(!placeholderFallback, "CHAINLINK_FALLBACK_RPC_URLS is using real endpoints"));
  console.log(statusLine(Boolean(preferredPrimary), "Primary Chainlink RPC is using the recommended Alchemy Free endpoint"));
  console.log(statusLine(Boolean(preferredFallback), "Fallback Chainlink RPC is using the recommended Infura Free endpoint"));
  console.log(statusLine(chainlinkPollMs === 5000, "CHAINLINK_POLL_MS is set to the recommended low-cost value (5000ms)"));
} else {
  console.log("[OK] Chainlink RPC endpoints are optional in local testing mode");
}
console.log(statusLine(embeddedMatching, "EMBEDDED_MATCHING_SERVICE=true for local dev"));
console.log(statusLine(dockerComposePs.ok, "docker compose local file parses and can talk to Docker"));
if (dockerComposePs.ok && dockerComposePs.output) {
  console.log("\nCurrent docker compose local status:");
  console.log(dockerComposePs.output);
}

console.log("\nRecommended local startup order:");
console.log("1. Start Docker Desktop");
console.log("2. docker compose -f docker-compose.local.yml up -d");
console.log("3. npm run dev:server");
console.log("4. npm run dev");
console.log("5. Invoke-RestMethod http://127.0.0.1:8787/health");

if (chainlinkEnabled && (placeholderPrimary || placeholderFallback)) {
  console.log("\nFull real-source local success is currently impossible until you replace the Chainlink RPC placeholders in .env.");
}
