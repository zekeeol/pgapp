import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const adminProxyTarget = process.env.PGAPP_ADMIN_PROXY_TARGET ?? "http://127.0.0.1:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api/admin": adminProxyTarget
    }
  },
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts"
  }
});
