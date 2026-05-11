import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: '/home/luis/projects/Ecom-Dash/frontend/e2e',
  timeout: 30000,
  expect: { timeout: 10000 },
  use: { baseURL: 'http://127.0.0.1:8015', headless: true },
});
