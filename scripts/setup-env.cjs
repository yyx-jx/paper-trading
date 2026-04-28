const fs = require("node:fs");
const path = require("node:path");

const root = process.cwd();
const source = path.join(root, ".env.example");
const target = path.join(root, ".env");

if (!fs.existsSync(source)) {
  console.error("[setup:env] .env.example not found.");
  process.exit(1);
}

if (fs.existsSync(target)) {
  console.log("[setup:env] .env already exists, keeping current file.");
  process.exit(0);
}

fs.copyFileSync(source, target);
console.log("[setup:env] created .env from .env.example");
console.log("[setup:env] replace CHAINLINK_RPC_URL and CHAINLINK_FALLBACK_RPC_URLS before expecting full real-source data.");
