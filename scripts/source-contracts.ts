import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";

type ServiceSourceName = "simulation" | "store";

function readTsFilesRecursively(dir: string): string[] {
  if (!existsSync(dir)) {
    return [];
  }
  const entries = readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name));
  const files: string[] = [];
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...readTsFilesRecursively(fullPath));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(fullPath);
    }
  }
  return files;
}

export function readServiceSourceContract(service: ServiceSourceName) {
  const root = process.cwd();
  const entryPath = path.join(root, "apps/server/src/services", `${service}.ts`);
  const moduleDir = path.join(root, "apps/server/src/services", service);
  const paths = [entryPath, ...readTsFilesRecursively(moduleDir)].filter((sourcePath) => {
    return existsSync(sourcePath) && statSync(sourcePath).isFile();
  });

  return paths
    .map((sourcePath) => {
      const relativePath = path.relative(root, sourcePath).replaceAll(path.sep, "/");
      return `\n// SOURCE: ${relativePath}\n${readFileSync(sourcePath, "utf8")}`;
    })
    .join("\n");
}

export function readSimulationServiceSource() {
  return readServiceSourceContract("simulation");
}

export function readStoreServiceSource() {
  return readServiceSourceContract("store");
}
