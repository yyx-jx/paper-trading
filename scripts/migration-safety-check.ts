import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";

const migrationsDir = path.resolve(process.cwd(), "db/migrations");
const destructivePatterns = [
  /\bDROP\s+(SCHEMA|DATABASE|TABLE|COLUMN)\b/i,
  /\bTRUNCATE\b/i,
  /\bDELETE\s+FROM\b(?![\s\S]*\bWHERE\b)/i,
  /\bALTER\s+TABLE\b[\s\S]*\bDROP\b/i
];

for (const filename of readdirSync(migrationsDir).filter((name) => name.endsWith(".sql"))) {
  const sql = readFileSync(path.join(migrationsDir, filename), "utf8");
  for (const pattern of destructivePatterns) {
    assert.doesNotMatch(sql, pattern, `${filename} contains a destructive migration pattern: ${pattern}`);
  }
}

console.log("migration-safety-check ok");
