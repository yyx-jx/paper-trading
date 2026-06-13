import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const source = readFileSync("apps/client/src/features/user/useUserSocket.ts", "utf8");
const dependencyMatch = source.match(/\n\s*}, \[\s*([\s\S]*?)\n\s*\]\);\s*\n}/);

assert.ok(dependencyMatch, "useUserSocket effect dependency list should be present");

const dependencyList = dependencyMatch[1];

assert.ok(!dependencyList.includes("input.me"), "useUserSocket should depend on stable me id, not the full me object");
assert.ok(
  !dependencyList.includes("input.activeViewedUser"),
  "useUserSocket should not reconnect when activeViewedUser object reference changes"
);
assert.ok(source.includes("useRef"), "useUserSocket should keep latest viewed user objects in refs for fallback payloads");

console.log("user socket stable dependency check passed");
