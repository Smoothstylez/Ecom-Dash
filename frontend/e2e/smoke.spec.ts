import { expect, test } from "@playwright/test";

test("legacy analytics fallback renders dashboard shell", async ({ page }) => {
  await page.goto("/legacy?tab=analytics");

  await expect(page).toHaveTitle(/Combined Dropshipping Dashboard/);
  await expect(page.locator("#tabAnalyticsBtn")).toBeVisible();
  await expect(page.locator("#analyticsPanel.active")).toBeVisible();
});

test("legacy orders fallback activates orders panel", async ({ page }) => {
  await page.goto("/legacy?tab=orders");

  await expect(page.locator("#ordersPanel.active")).toBeVisible();
  await expect(page.locator("#ordersPanel.active .table-wrap")).toBeVisible();
});

test("primary analytics route renders react workspace", async ({ page }) => {
  await page.goto("/analytics");

  await expect(page.getByRole("heading", { name: /Frontend Migration auf React 19/i })).toBeVisible();
  await expect(page.getByText("Analytics Blueprint")).toBeVisible();
});

test("primary orders route renders migrated order table", async ({ page }) => {
  await page.goto("/orders");

  await expect(page.getByText("Orders Migration Preview")).toBeVisible();
  await expect(page.getByText("Kombinierte Orders")).toBeVisible();
});

test("primary customers route renders migrated customer table", async ({ page }) => {
  await page.goto("/customers");

  await expect(page.getByText("Customers Migration Preview")).toBeVisible();
  await expect(page.getByText("Interaktive Karte auf Basis derselben Geo-Punkte wie in der Legacy-Ansicht.")).toBeVisible();
  await expect(page.getByText("Die Kundenliste entspricht funktional dem Legacy-Overview")).toBeVisible();
});

test("primary bookings route renders migrated bookings workspace", async ({ page }) => {
  await page.goto("/bookings");

  await expect(page.getByText("Bookings Migration")).toBeVisible();
  await expect(page.getByText("Bookings-Transaktionen mit bestehender Kontozuordnung und Belegverknuepfung.")).toBeVisible();
});

test("primary google ads route renders migrated ads workspace", async ({ page }) => {
  await page.goto("/google-ads");

  await expect(page.getByText("Google Ads Migration Preview")).toBeVisible();
  await expect(page.getByText("CSV-Import und Reset laufen direkt ueber die bestehenden Google-Ads-Endpoints.")).toBeVisible();
});

test("primary ebay route renders migrated ebay overview", async ({ page }) => {
  await page.goto("/ebay");

  await expect(page.getByText("eBay Migration Preview")).toBeVisible();
  await expect(page.getByText("Read-only eBay Orders-Tabelle aus `/api/ebay/orders`." )).toBeVisible();
});

test("app-preview redirects to the primary frontend routes", async ({ page }) => {
  await page.goto("/app-preview/orders");

  await expect(page).toHaveURL(/\/orders$/);
  await expect(page.getByText("Orders Migration Preview")).toBeVisible();
});
