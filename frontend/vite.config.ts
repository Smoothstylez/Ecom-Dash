import { defineConfig } from "vite";

export default defineConfig(() => ({
  base: "/",
  resolve: {
    alias: {
      "@": new URL("./src", import.meta.url).pathname,
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          react: ["react", "react-dom"],
        },
      },
    },
  },
  server: {
    host: "0.0.0.0",
    port: 5173,
    allowedHosts: ["desktop-nq2nv1f-1.tail7a6430.ts.net"],
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8012",
        changeOrigin: true,
      },
      "/static": {
        target: "http://127.0.0.1:8012",
        changeOrigin: true,
      },
    },
  },
  preview: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8012",
        changeOrigin: true,
      },
    },
  },
}));
