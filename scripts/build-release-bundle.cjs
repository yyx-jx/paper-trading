const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const root = path.resolve(__dirname, "..");
const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..+$/, "").replace("T", "_");
const releaseRoot = path.join(root, "release", `P_T_${pkg.version || "0.0.0"}_${stamp}`);
const serverRoot = path.join(releaseRoot, "server");

const excludedNames = new Set([
  ".git",
  ".claude",
  "node_modules",
  "dist",
  "data",
  "backups",
  "release",
  "deploy/windows-production",
  "deploy/windows-test",
  "paper_deploy_final"
]);
const excludedSuffixes = [".rar", ".zip", ".docx", ".log", ".pid"];
const includedTopLevel = new Set([
  "apps",
  "db",
  "deploy",
  "docs",
  "scripts",
  "Dockerfile.server",
  "docker-compose.deploy.yml",
  ".dockerignore",
  ".env.example",
  ".env.production.example",
  "package.json",
  "package-lock.json",
  "tsup.server.config.ts",
  "tsup.matching.config.ts",
  "README.md"
]);

function relativeFromRoot(filePath) {
  return path.relative(root, filePath).replace(/\\/g, "/");
}

function shouldCopy(src) {
  const rel = relativeFromRoot(src);
  if (!rel) return true;
  const first = rel.split("/")[0];
  if (!includedTopLevel.has(first)) return false;
  if (excludedNames.has(rel) || excludedNames.has(first)) return false;
  if (rel === ".env" || rel.startsWith(".env.")) {
    return rel === ".env.example" || rel === ".env.production.example";
  }
  return !excludedSuffixes.some((suffix) => rel.toLowerCase().endsWith(suffix));
}

function copyRecursive(src, dest) {
  if (!shouldCopy(src)) return;
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    fs.mkdirSync(dest, { recursive: true });
    for (const entry of fs.readdirSync(src)) {
      copyRecursive(path.join(src, entry), path.join(dest, entry));
    }
    return;
  }
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.copyFileSync(src, dest);
}

fs.rmSync(releaseRoot, { recursive: true, force: true });
fs.mkdirSync(serverRoot, { recursive: true });

for (const entry of fs.readdirSync(root)) {
  copyRecursive(path.join(root, entry), path.join(serverRoot, entry));
}

const forbidden = [];
function collectForbidden(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    const rel = path.relative(serverRoot, fullPath).replace(/\\/g, "/");
    if (
      rel === ".env" ||
      rel.startsWith("data/") ||
      rel.startsWith("backups/") ||
      rel.includes("/node_modules/") ||
      rel === "node_modules" ||
      rel === "dist" ||
      rel.startsWith("dist/")
    ) {
      forbidden.push(rel);
    }
    if (entry.isDirectory()) collectForbidden(fullPath);
  }
}
collectForbidden(serverRoot);
if (forbidden.length > 0) {
  throw new Error(`Release contains forbidden paths:\n${forbidden.join("\n")}`);
}

const checksums = [];
function addChecksums(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      addChecksums(fullPath);
      continue;
    }
    const rel = path.relative(releaseRoot, fullPath).replace(/\\/g, "/");
    const hash = crypto.createHash("sha256").update(fs.readFileSync(fullPath)).digest("hex");
    checksums.push(`${hash}  ${rel}`);
  }
}
addChecksums(releaseRoot);
fs.writeFileSync(path.join(releaseRoot, "checksums.sha256"), `${checksums.join("\n")}\n`, "utf8");

console.log(`Release bundle ready: ${releaseRoot}`);
