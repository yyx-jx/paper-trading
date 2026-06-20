import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";

const clientSrcDir = path.join("apps", "client", "src");
const i18nPath = path.join(clientSrcDir, "i18n", "index.ts");
const ignoredUiLiterals = new Set(["uiMeta"]);

function listSourceFiles(dir: string, files: string[] = []) {
  for (const entry of readdirSync(dir)) {
    const fullPath = path.join(dir, entry);
    const stats = statSync(fullPath);
    if (stats.isDirectory()) {
      listSourceFiles(fullPath, files);
    } else if (/\.(ts|tsx|js|jsx)$/.test(entry) && fullPath !== i18nPath) {
      files.push(fullPath);
    }
  }
  return files;
}

function extractResourceBlock(source: string, name: "enUS" | "zhCN") {
  const nextName = name === "enUS" ? "zhCN" : "resources";
  const match = source.match(new RegExp(`const ${name}:[\\s\\S]*?= \\{([\\s\\S]*?)\\n\\};\\s*\\n\\s*const ${nextName}`));
  assert.ok(match, `Could not locate ${name} resource block.`);
  return match[1]!;
}

function extractResourceKeys(source: string, name: "enUS" | "zhCN") {
  const block = extractResourceBlock(source, name);
  const keys = new Set<string>();
  const keyPattern = /^\s*([A-Za-z0-9_]+)\s*:/gm;
  let match: RegExpExecArray | null;
  while ((match = keyPattern.exec(block))) {
    keys.add(match[1]!);
  }
  return keys;
}

function extractUiLiterals() {
  const keys = new Map<string, string[]>();
  const literalPattern = /["'`](ui[A-Za-z0-9]+)["'`]/g;
  for (const filePath of listSourceFiles(clientSrcDir)) {
    const source = readFileSync(filePath, "utf8");
    let match: RegExpExecArray | null;
    while ((match = literalPattern.exec(source))) {
      const key = match[1]!;
      if (ignoredUiLiterals.has(key)) {
        continue;
      }
      const line = source.slice(0, match.index).split(/\r?\n/).length;
      const locations = keys.get(key) ?? [];
      locations.push(`${filePath}:${line}`);
      keys.set(key, locations);
    }
  }
  return keys;
}

const i18nSource = readFileSync(i18nPath, "utf8");
const enKeys = extractResourceKeys(i18nSource, "enUS");
const zhKeys = extractResourceKeys(i18nSource, "zhCN");
const uiLiterals = extractUiLiterals();
const missing = [...uiLiterals.entries()]
  .filter(([key]) => !enKeys.has(key) && !zhKeys.has(key))
  .map(([key, locations]) => `${key} (${locations.join(", ")})`)
  .sort();

assert.deepEqual(missing, [], `Missing i18n ui keys:\n${missing.join("\n")}`);

console.log("i18n-missing-check ok");
