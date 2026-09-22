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

test("pool lists invoices and opens the shared document preview", async ({ page }) => {
  const data = {
    products: [{ id: "p1", name: "MINI Ultra", available_quantity: 2, reserved_quantity: 0, in_transit_quantity: 0, amazon_costed_quantity: 7, average_home_cost_cents: 1000, sales_net_cents: 7000, margin_cents: 2000, margin_complete: true }],
    invoices: [{ id: "invoice-1", supplier: "Supplier GmbH", number: "INV-001", invoice_date: "2026-09-01", currency: "EUR", gross_cents: 10710, freight_cents: 300, vat_cents: 1710, notes: "Interne Prüfnote" }],
    lines: [{ id: "line-1", invoice_id: "invoice-1", product_id: "p1", quantity: 9, tax_status: "confirmed", gross_cents: 10710, net_cents: 9000, vat_cents: 1710, effective_cost_cents: 9000, deductible_vat_cents: 1710 }],
    receipts: [{ id: "receipt-1", line_id: "line-1", quantity: 9, available_quantity: 2 }],
    shipment_items: [], available_listings: [], listings: [], transfers: [], transfer_lines: [],
    payments: [{ invoice_id: "invoice-1", amount_cents: 11010 }], documents: [{ id: "doc-1", invoice_id: "invoice-1", filename: "supplier-invoice.pdf" }],
    listing_metrics: [], shipment_metrics: [], legacy_invoices: [], issues: [],
  };
  await page.route("**/api/amazon/pool", route => route.fulfill({ json: data }));
  await page.route("**/api/amazon/pool/documents/doc-1", route => route.fulfill({ contentType: "application/pdf", body: "%PDF-1.4\n%mock\n" }));
  await page.route("https://fonts.googleapis.com/**", route => route.abort());
  await page.route("https://unpkg.com/**", route => route.abort());
  await page.goto("/amazon");

  await expect(page.getByRole("button", { name: "Bestand", exact: true })).toHaveClass(/active/);
  await page.getByRole("button", { name: "Einkaufspool", exact: true }).click();
  await expect(page.getByRole("columnheader", { name: "Lieferant / Rechnung" })).toBeVisible();
  await expect(page.getByText("INV-001", { exact: false })).toBeVisible();
  await page.getByText("INV-001", { exact: false }).click();
  await expect(page.getByRole("dialog").first()).toContainText("Interne Prüfnote");
  await expect(page.locator("#previewModal")).toHaveClass(/active/);
  await expect(page.locator("#previewModal iframe")).toHaveAttribute("title", "supplier-invoice.pdf");
});
