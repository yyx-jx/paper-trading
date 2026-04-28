import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["apps/server/src/matching-index.ts"],
  format: ["cjs"],
  target: "node20",
  platform: "node",
  outDir: "dist/matching",
  clean: true,
  noExternal: [/.*/]
});
