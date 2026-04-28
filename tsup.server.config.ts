import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["apps/server/src/index.ts"],
  format: ["cjs"],
  target: "node20",
  platform: "node",
  outDir: "dist/server",
  clean: true,
  noExternal: [/.*/]
});
