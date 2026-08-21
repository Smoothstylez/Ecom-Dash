import { expect, test } from "@playwright/test";

test("Amazon FBA shipment list opens a wide desktop detail modal", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1024 });

  await page.route("**/api/amazon/status", (route) => route.fulfill({ json: {} }));
  await page.route("**/api/amazon/finance", (route) => route.fulfill({ json: {} }));
  await page.route("**/api/amazon/inbound/costs", (route) => route.fulfill({ json: { items: [] } }));
  await page.route("**/api/amazon/inbound/shipments", (route) => route.fulfill({
    json: {
      items: [{
        shipment_id: "FBA-LAYOUT-1",
        shipment_name: "Layout shipment",
        status: "CLOSED",
        status_label: "Empfangen",
        destination_fulfillment_center_id: "FRA7",
        quantity_shipped: 12,
        quantity_received: 12,
        sku_count: 2,
        invoice_count: 0,
        assigned_cost_cents: 0,
        transport_quote_cents: 2599,
        cost_status: "missing",
      }],
    },
  }));
  await page.route("**/api/amazon/inbound/shipments/FBA-LAYOUT-1", (route) => route.fulfill({
    json: {
      shipment: {
        shipment_id: "FBA-LAYOUT-1",
        shipment_name: "Layout shipment",
        status: "CLOSED",
        status_label: "Empfangen",
        quantity_shipped: 12,
        quantity_received: 12,
        sku_count: 2,
        invoice_count: 0,
        assigned_cost_cents: 0,
        cost_status: "missing",
      },
      items: [],
      costs: [],
      invoices: [],
      invoice_lines: [],
      cost_allocations: [],
    },
  }));

  await page.goto("/amazon", { waitUntil: "networkidle" });

  await expect(page.getByText("Storniert", { exact: true })).toHaveCount(0);
  await page.locator(".amazon-shipment-table tbody tr").first().click();
  await expect(page.locator("#detailsModal")).toHaveClass(/active/);
  const modalWidth = await page.locator("#detailsModal .modal-card").evaluate((node) => node.clientWidth);
  expect(modalWidth).toBeGreaterThanOrEqual(1100);
});

test("VAT-bearing invoice blocks the temporary net-only line submission", async ({ page }) => {
  let lineSubmissionCount = 0;

  await page.route("**/api/amazon/status", (route) => route.fulfill({ json: {} }));
  await page.route("**/api/amazon/finance", (route) => route.fulfill({ json: {} }));
  await page.route("**/api/amazon/inbound/costs", (route) => route.fulfill({ json: { items: [] } }));
  await page.route("**/api/amazon/inbound/shipments", (route) => route.fulfill({
    json: {
      items: [{
        shipment_id: "FBA-VAT-1",
        shipment_name: "VAT shipment",
        status: "CLOSED",
        status_label: "Closed",
        quantity_shipped: 1,
        quantity_received: 1,
        sku_count: 1,
        invoice_count: 1,
        assigned_cost_cents: 0,
      }],
    },
  }));
  await page.route("**/api/amazon/inbound/shipments/FBA-VAT-1", (route) => route.fulfill({
    json: {
      shipment: {
        shipment_id: "FBA-VAT-1",
        shipment_name: "VAT shipment",
        status: "CLOSED",
        status_label: "Closed",
        quantity_shipped: 1,
        quantity_received: 1,
        sku_count: 1,
        invoice_count: 1,
        assigned_cost_cents: 0,
      },
      items: [{ seller_sku: "SKU-1", fnsku: "FNSKU-1", asin: "ASIN-1", quantity_shipped: 1, quantity_received: 1 }],
      costs: [],
      invoices: [{ id: "INV-VAT-1", supplier_name: "Supplier", invoice_number: "INV-1", gross_cents: 1190, vat_cents: 190, document_path: "invoice.pdf" }],
      invoice_lines: [],
      cost_allocations: [],
    },
  }));
  await page.route("**/api/amazon/inbound/invoices/INV-VAT-1/lines", (route) => {
    lineSubmissionCount += 1;
    return route.fulfill({ status: 500 });
  });

  await page.goto("/amazon", { waitUntil: "networkidle" });
  await page.getByText("FBA-VAT-1", { exact: true }).click();
  await page.getByRole("button", { name: "Position speichern" }).click();

  await expect(page.getByText("Brutto-/Netto-/USt-Positionen werden mit dem detaillierten Editor erfasst.")).toBeVisible();
  expect(lineSubmissionCount).toBe(0);
});
