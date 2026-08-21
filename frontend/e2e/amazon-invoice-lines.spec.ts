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

test("FBA invoice drafts upload amounts and allocate every unconfirmed SKU", async ({ page }) => {
  let invoiceUploaded = false;
  let lineSaved = false;
  let costsConfirmed = false;
  let invoiceUploadBody = "";
  let linePayload: Record<string, unknown> | null = null;

  await page.route("**/api/amazon/status", (route) => route.fulfill({ json: {} }));
  await page.route("**/api/amazon/finance", (route) => route.fulfill({ json: {} }));
  await page.route("**/api/amazon/inbound/costs", (route) => route.fulfill({ json: { items: [] } }));
  await page.route("**/api/amazon/inbound/shipments", (route) => route.fulfill({
    json: {
      items: [{
        shipment_id: "FBA-TEST-1",
        shipment_name: "Invoice shipment",
        status: "CLOSED",
        status_label: "Empfangen",
        quantity_shipped: 1,
        quantity_received: 1,
        sku_count: 1,
        invoice_count: invoiceUploaded ? 1 : 0,
        assigned_cost_cents: 0,
      }],
    },
  }));
  await page.route("**/api/amazon/inbound/shipments/FBA-TEST-1", (route) => route.fulfill({
    json: {
      shipment: {
        shipment_id: "FBA-TEST-1",
        shipment_name: "Invoice shipment",
        status: "CLOSED",
        status_label: "Empfangen",
        quantity_shipped: 1,
        quantity_received: 1,
        sku_count: 1,
        invoice_count: invoiceUploaded ? 1 : 0,
        assigned_cost_cents: 0,
      },
      items: [{ seller_sku: "SKU-1", fnsku: "FNSKU-1", asin: "ASIN-1", quantity_shipped: 1, quantity_received: 1 }],
      costs: [],
      invoices: invoiceUploaded ? [{ id: "INV-1", supplier_name: "Supplier", invoice_number: "SUP-1", gross_cents: 1190, net_cents: 1000, vat_cents: 190, document_path: "supplier.pdf" }] : [],
      invoice_lines: lineSaved ? [{ id: "LINE-1", invoice_id: "INV-1", seller_sku: "SKU-1", fnsku: "FNSKU-1", asin: "ASIN-1", title: "", quantity: 1, gross_cents: 1190, net_cents: 1000, vat_cents: 190 }] : [],
      cost_allocations: costsConfirmed ? [{ id: "ALLOC-1", seller_sku: "SKU-1", fnsku: "FNSKU-1", quantity: 1, net_cents: 1000, currency: "EUR", allocation_method: "invoice" }] : [],
    },
  }));
  await page.route("**/api/amazon/inbound/shipments/FBA-TEST-1/invoices", async (route) => {
    invoiceUploadBody = route.request().postData() || "";
    invoiceUploaded = true;
    await route.fulfill({ json: { id: "INV-1" } });
  });
  await page.route("**/api/amazon/inbound/invoices/INV-1/lines", async (route) => {
    linePayload = JSON.parse(route.request().postData() || "{}");
    lineSaved = true;
    await route.fulfill({ json: { id: "LINE-1" } });
  });
  await page.route("**/api/amazon/inbound/shipments/FBA-TEST-1/cost-confirmation", async (route) => {
    costsConfirmed = true;
    await route.fulfill({ json: { ok: true } });
  });

  await page.goto("/amazon", { waitUntil: "networkidle" });
  await page.getByText("FBA-TEST-1", { exact: true }).click();
  await expect(page.getByRole("button", { name: "Dateien waehlen" })).toBeVisible();
  await page.locator(".invoice-file-input").setInputFiles({
    name: "supplier.pdf",
    mimeType: "application/pdf",
    buffer: Buffer.from("invoice"),
  });

  await expect(page.getByText("supplier.pdf", { exact: true })).toBeVisible();
  await page.getByLabel("Lieferant supplier.pdf").fill("Supplier");
  await page.getByLabel("Rechnungsnummer supplier.pdf").fill("SUP-1");
  await page.getByLabel("Brutto supplier.pdf").fill("11,90");
  await page.getByLabel("Netto supplier.pdf").fill("10,00");
  await page.getByLabel("USt supplier.pdf").fill("1,90");
  await page.getByRole("button", { name: "Hochladen" }).click();

  await expect.poll(() => invoiceUploadBody).toContain("1190");
  expect(invoiceUploadBody).toContain("1000");
  expect(invoiceUploadBody).toContain("190");

  await expect(page.getByLabel("Rechnung SKU-1")).toHaveValue("INV-1");
  await page.getByLabel("Brutto SKU-1").fill("11,90");
  await page.getByLabel("Netto SKU-1").fill("10,00");
  await page.getByLabel("USt SKU-1").fill("1,90");
  await page.getByRole("button", { name: "Position speichern" }).click();

  await expect.poll(() => linePayload).toEqual({
    seller_sku: "SKU-1",
    fnsku: "FNSKU-1",
    asin: "ASIN-1",
    title: "",
    quantity: 1,
    gross_cents: 1190,
    net_cents: 1000,
    vat_cents: 190,
  });
  await expect(page.getByLabel("Rechnung SKU-1")).toBeDisabled();
  await page.getByRole("button", { name: "Kosten bestaetigen und FIFO-Lots erzeugen" }).click();
  await expect(page.getByText("Produktkosten bereits bestaetigt; FIFO-Lots sind erzeugt.")).toBeVisible();
  await expect(page.getByLabel("Brutto SKU-1")).toHaveCount(0);
});
