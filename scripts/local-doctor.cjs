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
const embeddedMatching = effectiveEnv.EMBEDDED_MATCHING_SERVICE !== "false";
const coinbaseEnabled = effectiveEnv.COINBASE_ENABLED !== "false";
const upstreamProxyUrl = effectiveEnv.UPSTREAM_PROXY_URL ?? "";
const coinbaseWsUrl = effectiveEnv.COINBASE_WS_URL ?? "";
const coinbaseRestUrl = effectiveEnv.COINBASE_REST_URL ?? "";
const coinbaseRestPollMs = Number(effectiveEnv.COINBASE_REST_POLL_MS ?? 0);

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
    !coinbaseEnabled,
    "COINBASE_ENABLED=false for local Binance + Polymarket testing"
  )
);
console.log(statusLine(upstreamProxyUrl === "http://127.0.0.1:7897", "UPSTREAM_PROXY_URL is set to the local proxy (http://127.0.0.1:7897)"));
if (coinbaseEnabled) {
  console.log(statusLine(coinbaseWsUrl === "wss://advanced-trade-ws.coinbase.com", "COINBASE_WS_URL uses the public Coinbase Advanced Trade endpoint"));
  console.log(statusLine(coinbaseRestUrl === "https://api.exchange.coinbase.com", "COINBASE_REST_URL uses the public Coinbase Exchange REST endpoint"));
  console.log(statusLine(coinbaseRestPollMs === 5000, "COINBASE_REST_POLL_MS is set to the recommended fallback cadence (5000ms)"));
} else {
  console.log("[OK] Coinbase cross-check is optional in local testing mode");
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

if (coinbaseEnabled && (!coinbaseWsUrl || !coinbaseRestUrl)) {
  console.log("\nFull real-source local success is currently impossible until you configure the Coinbase endpoints in .env.");
}
