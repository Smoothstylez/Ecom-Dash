import { expect, test } from "@playwright/test";

const base = process.env.ECOM_FRONTEND_URL || "http://127.0.0.1:5179";

test("ordinary file upload shows automatic assignment, shared invoice and updated VAT", async ({ page }) => {
  let imported = false;
  let uploads = 0;
  await page.route(url => url.pathname.startsWith("/api/"), async route => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/ust-report/import") {
      expect(route.request().headers()["content-type"]).toContain("multipart/form-data");
      expect(route.request().postDataBuffer()?.toString()).toContain("%PDF-1.4");
      imported = true;
      uploads++;
      await route.fulfill({ json: { items: [{ id: "file1", filename: "invoice.pdf", kind: "invoice_pdf", status: uploads === 1 ? "approved" : "duplicate", invoice_number: "AUTO-UI-1", deduction_month: "2026-08", reasons: [] }], total: 1 } });
    } else if (url.pathname === "/api/ust-report") {
      await route.fulfill({ json: { month: url.searchParams.get("month"), status: "ready", settings: {}, business_rules: {}, blockers: [], warnings: [],
        sections: { kaufland: { rows: [], returns: { count: 0 }, output_vat_cents: 19000 }, amazon: {},
          input_vat: { purchases_cents: 0, amazon_fees_cents: imported ? 1900 : 0, kaufland_fees_cents: 0, other_cents: 0, input_vat_incomplete: false } },
        totals: { output_vat_cents: 19000, input_vat_cents: imported ? 1900 : 0, vat_payable_cents: imported ? 17100 : 19000 } } });
    } else if (url.pathname === "/api/ust-report/documents") {
      await route.fulfill({ json: { items: imported ? [{ id: "book:invoice1", provider: "amazon", doc_type: "fee", invoice_number: "AUTO-UI-1", invoice_date: "2026-08-31", deduction_month: "2026-08", currency: "EUR", net_cents: 10000, vat_cents: 1900, deductible_vat_cents: 1900, effective_deductible_vat_cents: 1900, input_vat_status: "confirmed" }] : [], total: imported ? 1 : 0 } });
    } else {
      await route.fulfill({ json: { items: [], total: 0, data: [], settings: {}, orders: [], has_credentials: true } });
    }
  });
  await page.route("https://fonts.googleapis.com/**", route => route.abort());
  await page.goto(base + "/tax-report");
  await expect(page.getByRole("heading", { name: "Reports und Belege importieren" })).toBeVisible();
  const file = { name: "invoice.pdf", mimeType: "application/pdf", buffer: Buffer.from("%PDF-1.4\ntest") };
  await page.locator("#ustImportFiles").setInputFiles(file);
  await page.getByRole("button", { name: "Automatisch importieren", exact: true }).click();
  await expect(page.getByText("Automatisch übernommen", { exact: true })).toBeVisible();
  await expect(page.locator("article.kpi").filter({ hasText: "Vorsteuer" })).toContainText("19,00");
  await page.getByRole("button", { name: "Eingangsrechnungen", exact: true }).click();
  await expect(page.getByRole("row").filter({ hasText: "AUTO-UI-1" }).last()).toContainText("Freigegeben");
  await page.locator("#ustImportFiles").setInputFiles(file);
  await page.getByRole("button", { name: "Automatisch importieren", exact: true }).click();
  await expect(page.getByText("Bereits berücksichtigt", { exact: true })).toBeVisible();
  await expect(page.locator("article.kpi").filter({ hasText: "Vorsteuer" })).toContainText("19,00");
});

test("uncertain upload shows a German review reason instead of silent success", async ({ page }) => {
  await page.route(url => url.pathname.startsWith("/api/"), async route => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/api/ust-report/import") {
      await route.fulfill({ json: { items: [{ id: "unclear", filename: "unclear.pdf", kind: "invoice_pdf", status: "needs_review", reasons: ["unbekannte_position:Example"] }], total: 1 } });
    } else if (path === "/api/ust-report") {
      await route.fulfill({ json: { status: "draft", settings: {}, business_rules: {}, blockers: [], warnings: [], sections: { kaufland: { rows: [], returns: { count: 0 } }, amazon: {}, input_vat: {} }, totals: {} } });
    } else await route.fulfill({ json: { items: [], total: 0, has_credentials: true, settings: {} } });
  });
  await page.goto(base + "/tax-report");
  await page.locator("#ustImportFiles").setInputFiles({ name: "unclear.pdf", mimeType: "application/pdf", buffer: Buffer.from("%PDF-1.4\nunknown") });
  await page.getByRole("button", { name: "Automatisch importieren", exact: true }).click();
  await expect(page.getByText(/Noch nicht zugeordnet: Example/)).toBeVisible();
  await expect(page.getByText("Automatisch übernommen", { exact: true })).toHaveCount(0);
});

test('CSV history shows invoice numbers, found PDF and grouped review details', async ({ page }) => {
  await page.route(url => url.pathname.startsWith('/api/'), async route => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/ust-report/imports') {
      await route.fulfill({ json: { items: [{ id: 'csv', filename: 'rows.csv', kind: 'fee_csv', status: 'paired_review',
        invoice_numbers: ['TEST-AEU-1', 'TEST-AEU-2'], months: ['2026-08', '2026-09'],
        pairings: [{ invoice_number: 'TEST-AEU-1', pdf_filename: 'original.pdf', status: 'paired_review', document_id: 'book:1', deduction_month: '2026-08', reasons: ['unbekannte_position:Mystery logistics:7'] }],
        reasons: ['unbekannte_position:Mystery logistics:7'] }], total: 1 } });
    } else if (path === '/api/ust-report') {
      await route.fulfill({ json: { status: 'draft', settings: {}, business_rules: {}, blockers: [], warnings: [], sections: { kaufland: { rows: [], returns: { count: 0 } }, amazon: {}, input_vat: {} }, totals: {} } });
    } else await route.fulfill({ json: { items: [], total: 0, has_credentials: true, settings: {} } });
  });
  await page.goto(base + '/tax-report');
  const row = page.getByRole('row').filter({ hasText: 'rows.csv' });
  await expect(row).toContainText('TEST-AEU-1, TEST-AEU-2');
  await expect(row).toContainText('Original-PDF: original.pdf');
  await expect(row).toContainText('Zugeordnet – Rechnung zu prüfen');
  await expect(row).toContainText('Mystery logistics (7 Positionen)');
  await expect(row).toContainText('September 2026');
  await expect(row).not.toContainText('Original-PDF fehlt');
});

test('open finance check is visible without disabling report filing', async ({ page }) => {
  await page.route(url => url.pathname.startsWith('/api/'), async route => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/ust-report') {
      await route.fulfill({ json: { status: 'ready', settings: {}, business_rules: {}, blockers: [], warnings: [{ code: 'FINANCE_RECONCILIATION_INCOMPLETE', count: 1 }],
        sections: { kaufland: { rows: [], returns: { count: 0 } }, amazon: {}, input_vat: {}, finance_reconciliation: {
          status: 'incomplete', comparisons: [{ currency: 'GBP', category: 'subscription', invoice_cents: 2975, finance_cents: 2975, difference_cents: 0 }],
          details: [], timing: [], issues: [{ code: 'FINANCE_SOURCE_MISSING' }], excluded_ads_cents: 0 } },
        totals: { output_vat_cents: 19000, input_vat_cents: 1900, vat_payable_cents: 17100 } } });
    } else await route.fulfill({ json: { items: [], total: 0, has_credentials: true, settings: {} } });
  });
  await page.goto(base + '/tax-report');
  await expect(page.getByRole('region', { name: 'Finanzabgleich', exact: true })).toContainText('Nicht vollständig prüfbar');
  await expect(page.getByRole('region', { name: 'Finanzabgleich', exact: true })).toContainText('GBP');
  await expect(page.getByRole('button', { name: 'Endgültig abgeben', exact: true })).toBeEnabled();
  await expect(page.locator('article.kpi').filter({ hasText: 'Vorsteuer' })).toContainText('19,00');
});
