import path from "node:path";
import { readFileSync } from "node:fs";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const packageJson = JSON.parse(readFileSync(path.resolve(__dirname, "../../package.json"), "utf8")) as { version?: string };
const appVersion = String(packageJson.version || "0.0.0");
const appDisplayTitle = `HT Paper Trading v${appVersion}`;
const devApiProxyTarget =
  process.env.VITE_DEV_API_PROXY_TARGET || process.env.VITE_API_BASE_URL || "http://127.0.0.1:8787";

type DevProxyRequest = {
  removeHeader(name: string): void;
  setHeader(name: string, value: string): void;
};

function configureDevProxy(proxy: {
  on(event: "proxyReq" | "proxyReqWs", listener: (proxyReq: DevProxyRequest) => void): void;
}) {
  const removeBrowserOrigin = (proxyReq: DevProxyRequest) => {
    proxyReq.removeHeader("origin");
    proxyReq.setHeader("origin", "");
  };

  proxy.on("proxyReq", removeBrowserOrigin);
  proxy.on("proxyReqWs", removeBrowserOrigin);
}

export default defineConfig({
  plugins: [react()],
  define: {
    __APP_VERSION__: JSON.stringify(appVersion),
    __APP_DISPLAY_TITLE__: JSON.stringify(appDisplayTitle)
  },
  root: path.resolve(__dirname, "src"),
  base: "./",
  build: {
    outDir: path.resolve(__dirname, "../../dist/renderer"),
    emptyOutDir: true
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api": {
        target: devApiProxyTarget,
        changeOrigin: true,
        ws: true,
        configure: configureDevProxy
      },
      "/health": {
        target: devApiProxyTarget,
        changeOrigin: true,
        configure: configureDevProxy
      },
      "/ws": {
        target: devApiProxyTarget,
        changeOrigin: true,
        ws: true,
        configure: configureDevProxy
      }
    }
  }
});
