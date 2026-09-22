import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";

test("pool captures mixed invoice lines and renders honest product costs", async ({ page }) => {
  const writes: Record<string, unknown>[] = [];
  const data = {
    products: [{ id: "p1", name: "MINI Ultra", available_quantity: 2, reserved_quantity: 7, in_transit_quantity: 0, amazon_costed_quantity: 0, average_home_cost_cents: 1000, sales_net_cents: 7000, margin_cents: 2000, margin_complete: false }],
    invoices: [], lines: [], receipts: [], shipment_items: [], available_listings: [], listings: [], transfers: [], transfer_lines: [], payments: [], documents: [], listing_metrics: [], shipment_metrics: [], legacy_invoices: [], issues: [],
  };
  await page.route(/http:\/\/127\.0\.0\.1:5179\/api\//, route => route.fulfill({ json: { items: [], orders: [], settings: {}, data: [], totals: {} } }));
  await page.route("**/api/amazon/pool", route => route.fulfill({ json: data }));
  await page.route("**/api/amazon/pool/invoices", async route => {
    writes.push(route.request().postDataJSON());
    await route.fulfill({ json: { id: "invoice-1" } });
  });
  await page.route("**/static/css/*.css", route => route.fulfill({ contentType: "text/css", body: readFileSync(`../ecommerce-dashboard/app/static/css/${new URL(route.request().url()).pathname.split("/").pop()}`, "utf8") }));
  await page.route("https://fonts.googleapis.com/**", route => route.abort());
  await page.route("https://unpkg.com/**", route => route.abort());
  await page.goto("/amazon");
  await page.getByRole("button", { name: "Einkaufspool", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Einkaufspool", exact: true })).toBeVisible();
  await page.getByLabel("Lieferant", { exact: true }).fill("Supplier GmbH");
  await page.getByLabel("Rechnungsnummer", { exact: true }).fill("INV-001");
  await page.getByRole("combobox", { name: "Produkt", exact: true }).selectOption("p1");
  await page.getByLabel("Stück", { exact: true }).fill("9");
  await page.getByLabel("Positionsbetrag brutto").fill("107,10");
  await page.getByLabel("Enthaltene Steuer").fill("17,10");
  await page.getByRole("combobox", { name: "Vorsteuer abziehbar", exact: true }).selectOption("yes");
  await page.getByRole("button", { name: "Rechnung erfassen", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("Rechnung erfasst");
  expect(writes).toHaveLength(1);
  expect(writes[0].lines).toEqual([{ product_id: "p1", quantity: 9, gross_cents: 10710, net_cents: 9000, vat_cents: 1710, deductible_vat_cents: 1710 }]);
  expect(writes[0].request_id).toBeTruthy();
  await page.getByRole("button", { name: "Produkte & Listings", exact: true }).click();
  await expect(page.getByText("Kosten / Finanzdaten unvollständig", { exact: true })).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("button", { name: "Produkt anlegen", exact: true })).toBeVisible();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
  expect(overflow).toBe(false);
});
