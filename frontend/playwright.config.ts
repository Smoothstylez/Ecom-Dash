import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:8013",
    trace: "on-first-retry",
  },
  webServer: {
    command: "AUTO_SYNC_ON_STARTUP=0 LIVE_SYNC_BACKGROUND_ENABLED=0 python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8013",
    cwd: "../ecommerce-dashboard",
    url: "http://127.0.0.1:8013/api/health",
    reuseExistingServer: false,
    timeout: 120_000,
  },
});
