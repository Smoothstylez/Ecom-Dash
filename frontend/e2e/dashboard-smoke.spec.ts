import { expect, test, type Page } from "@playwright/test";

type RouteExpectation = {
  path: string;
  activeNavId: string;
  activePanelId: string;
  bodyClass?: string;
};

const routeExpectations: RouteExpectation[] = [
  { path: "/analytics", activeNavId: "tabAnalyticsBtn", activePanelId: "analyticsPanel" },
  { path: "/orders", activeNavId: "tabOrdersBtn", activePanelId: "ordersPanel" },
  { path: "/support", activeNavId: "tabSupportBtn", activePanelId: "supportPanel" },
  { path: "/customers", activeNavId: "tabCustomersBtn", activePanelId: "customersPanel" },
  { path: "/invoices", activeNavId: "tabInvoicesBtn", activePanelId: "invoicesPanel" },
  { path: "/bookings/full?subtab=transactions", activeNavId: "tabBookingsBtn", activePanelId: "bookingsPanel", bodyClass: "bookings-full" },
  { path: "/google-ads", activeNavId: "tabGoogleAdsBtn", activePanelId: "googleAdsPanel" },
  { path: "/ebay", activeNavId: "tabEbayBtn", activePanelId: "ebayPanel" },
];

type StubBookingsBootstrapOptions = {
  transactions?: Array<Record<string, unknown>>;
  orders?: Array<Record<string, unknown>>;
  monthlyInvoices?: Array<Record<string, unknown>>;
  accounts?: Array<Record<string, unknown>>;
  templates?: Array<Record<string, unknown>>;
  documents?: Array<Record<string, unknown>>;
  ledgerOrders?: Array<Record<string, unknown>>;
};

async function stubBookingsBootstrap(page: Page, options: StubBookingsBootstrapOptions = {}) {
  const transactions = options.transactions ?? [];
  const orders = options.orders ?? [];
  const monthlyInvoices = options.monthlyInvoices ?? [];
  const accounts = options.accounts ?? [];
  const templates = options.templates ?? [];
  const documents = options.documents ?? [];
  const ledgerOrders = options.ledgerOrders ?? [];

    await page.route("**/api/bookings/transactions?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: transactions,
          total: transactions.length,
          category_counts: {
            sale: transactions.filter((transaction) => String(transaction.type || "").toUpperCase() === "SALE").length,
            fee: transactions.filter((transaction) => ["FEE", "SHIPPING"].includes(String(transaction.type || "").toUpperCase())).length,
            cogs: transactions.filter((transaction) => String(transaction.type || "").toUpperCase() === "COGS").length,
            invoice: transactions.filter((transaction) => String(transaction.type || "").toUpperCase() === "EXPENSE").length,
            subscription: transactions.filter((transaction) => String(transaction.type || "").toUpperCase() === "SUBSCRIPTION").length,
            refund: transactions.filter((transaction) => String(transaction.type || "").toUpperCase() === "REFUND").length,
            other: transactions.filter((transaction) => ["PAYOUT", "ADJUSTMENT"].includes(String(transaction.type || "").toUpperCase())).length,
          },
          limit: transactions.length,
          offset: 0,
        }),
      });
    });

  await page.route("**/api/bookings/orders?**", async (route) => {
    const url = new URL(route.request().url());
    const limit = Number(url.searchParams.get("limit") || String(orders.length || 150));
    const offset = Number(url.searchParams.get("offset") || "0");
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        items: orders.slice(offset, offset + limit),
        total: orders.length,
        limit,
        offset,
      }),
    });
  });

  await page.route("**/api/bookings/ledger/orders", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ items: ledgerOrders, total: ledgerOrders.length }),
    });
  });

  await page.route("**/api/bookings/payment-accounts", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ items: accounts, total: accounts.length }),
    });
  });

  await page.route("**/api/bookings/templates", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ items: templates, total: templates.length }),
    });
  });

  await page.route("**/api/bookings/documents", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ items: documents, total: documents.length }),
    });
  });

  await page.route("**/api/bookings/monthly-invoices", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ items: monthlyInvoices, total: monthlyInvoices.length }),
    });
  });
}

async function openSettings(page: Page) {
  await page.locator("#sidebarSettingsBtn").click();
  await expect(page.locator("#settingsPanel")).toHaveClass(/active/);
}

async function disablePolling(page: Page) {
  await page.addInitScript(() => {
    window.localStorage.setItem("dash-combined.polling", JSON.stringify({ enabled: false, intervalSec: 30 }));
  });
}

test.describe("dashboard smoke", () => {
  for (const expectation of routeExpectations) {
    test(`boots ${expectation.path}`, async ({ page }) => {
      const pageErrors: string[] = [];
      page.on("pageerror", (error) => {
        pageErrors.push(error.message);
      });

      await page.goto(expectation.path, { waitUntil: "networkidle" });

      if (expectation.path === "/customers") {
        await expect(page.locator("#customersPanel")).toHaveAttribute("data-react-customers-mounted", "true");
      } else if (expectation.path === "/support") {
        await expect(page.locator("#supportPanel")).toHaveAttribute("data-react-support-mounted", "true");
      } else if (expectation.path === "/invoices") {
        await expect(page.locator("#invoicesPanel")).toHaveAttribute("data-react-invoices-mounted", "true");
      } else if (expectation.path === "/google-ads") {
        await expect(page.locator("#googleAdsPanel")).toHaveAttribute("data-react-google-ads-mounted", "true");
      } else if (expectation.path === "/ebay") {
        await expect(page.locator("#ebayPanel")).toHaveAttribute("data-react-ebay-mounted", "true");
      } else if (expectation.path.startsWith("/bookings/full")) {
        await expect(page.locator("#bookingsPanel")).toHaveAttribute("data-react-bookings-mounted", "true");
      }

      await expect(page.locator(`#${expectation.activeNavId}`)).toHaveClass(/active/);
      await expect(page.locator(`#${expectation.activePanelId}`)).toHaveClass(/active/);

      if (expectation.bodyClass) {
        await expect(page.locator("body")).toHaveClass(new RegExp(expectation.bodyClass));
      }

      expect(pageErrors, `page errors on ${expectation.path}`).toEqual([]);
    });
  }

  test("legacy app-preview links resolve to analytics", async ({ page, request }) => {
    const response = await request.get("/app-preview/analytics", { maxRedirects: 0 });

    if (response.status() === 307) {
      expect(response.headers()["location"]).toBe("/analytics");
      return;
    }

    expect(response.status()).toBe(200);

    await page.goto("/app-preview/analytics", { waitUntil: "networkidle" });
    await expect(page.locator("#analyticsPanel")).toHaveClass(/active/);
    expect(["/analytics", "/app-preview/analytics"]).toContain(new URL(page.url()).pathname);
  });

  test("orders route mounts React table host", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.goto("/orders", { waitUntil: "networkidle" });

    await expect(page.locator("#ordersBody")).toHaveAttribute("data-react-orders-mounted", "true");

    expect(pageErrors, "page errors on /orders React host").toEqual([]);
  });

  test("invoices route loads draft, saves profile and archives created invoice", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let createdInvoice = false;
    let profileSaveCount = 0;
    let draftRequests = 0;
    let previewRequests = 0;
    let createRequests = 0;

    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          total: 1,
          items: [
            {
              marketplace: "shopify",
              order_id: "order-1",
              external_order_id: "TEST-ORDER-1",
              order_date: "2026-02-03T10:00:00Z",
              customer: "Alice Example",
              article: "Alpha Product",
              total_cents: 12990,
            },
          ],
        }),
      });
    });

    await page.route(/\/api\/invoices\/profile$/, async (route) => {
      if (route.request().method() === "PUT") {
        profileSaveCount += 1;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          profile: {
            legal_name: "Demo Shop",
            street: "Musterstrasse 1",
            postcode: "10115",
            city: "Berlin",
            country: "DE",
            email: "hello@example.com",
            tax_number: "12/345/67890",
            tax_mode: "small_business",
            invoice_prefix: "RE",
            default_template: "clean",
            footer_note: "Danke fuer Ihren Einkauf.",
            payment_note: "Bereits ueber den Marktplatz bezahlt.",
            eu_invoicing_enabled: true,
          },
        }),
      });
    });

    await page.route(/\/api\/invoices\/draft\?.*$/, async (route) => {
      draftRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          invoice: {
            invoice_number_preview: "RE-2026-000001",
            invoice_date: "2026-05-31",
            delivery_date: "2026-05-31",
            currency: "EUR",
            marketplace: "shopify",
            order_id: "order-1",
            external_order_id: "TEST-ORDER-1",
            tax_treatment: "small_business",
          },
          template: { key: "clean", label: "Clean" },
          customer: {
            name: "Alice Example",
            email: "alice@example.com",
            billing_address: {
              name: "Alice Example",
              street: "Musterweg 9",
              postcode: "10115",
              city: "Berlin",
              country: "DE",
            },
          },
          items: [
            {
              position: 1,
              title: "Alpha Product",
              quantity: 1,
              unit_price_gross_cents: 12990,
              line_total_gross_cents: 12990,
            },
          ],
          totals: {
            gross_cents: 12990,
            shipping_cents: 0,
            source_tax_cents: 0,
          },
          validation: {
            blockers: createdInvoice ? ["Fuer diese Bestellung existiert bereits eine Rechnung (RE-2026-000001)."] : [],
            warnings: ["Bestellung bitte final gegenpruefen."],
            billing_source: "billing",
            ready: !createdInvoice,
          },
          existing_invoice: createdInvoice ? { id: "inv-1", invoice_number: "RE-2026-000001" } : null,
        }),
      });
    });

    await page.route(/\/api\/invoices\/preview\.pdf\?.*$/, async (route) => {
      previewRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/pdf",
        body: "%PDF-1.4 preview",
      });
    });

    await page.route(/\/api\/invoices(?:\?.*)?$/, async (route) => {
      if (route.request().method() === "POST") {
        createRequests += 1;
        createdInvoice = true;
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            ok: true,
            invoice: {
              id: "inv-1",
              invoice_number: "RE-2026-000001",
              marketplace: "shopify",
              source_order_id: "order-1",
              source_external_order_id: "TEST-ORDER-1",
              customer_name: "Alice Example",
              customer_country: "DE",
              template_key: "clean",
              total_gross_cents: 12990,
              invoice_date: "2026-05-31",
            },
          }),
        });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          total: createdInvoice ? 1 : 0,
          items: createdInvoice ? [
            {
              id: "inv-1",
              invoice_number: "RE-2026-000001",
              marketplace: "shopify",
              source_order_id: "order-1",
              source_external_order_id: "TEST-ORDER-1",
              customer_name: "Alice Example",
              customer_country: "DE",
              template_key: "clean",
              total_gross_cents: 12990,
              invoice_date: "2026-05-31",
            },
          ] : [],
        }),
      });
    });

    await page.goto("/invoices", { waitUntil: "networkidle" });

    await expect(page.locator("#invoicesPanel")).toHaveAttribute("data-react-invoices-mounted", "true");
    await page.locator("tr[data-invoice-order-row='true']").first().click();
    await expect.poll(() => draftRequests).toBeGreaterThan(0);
    await expect.poll(() => previewRequests).toBeGreaterThan(0);
    await expect(page.locator("#invoicePreviewFrame")).toBeVisible();

    await page.locator("#invoiceCreateBtn").click();
    await expect.poll(() => createRequests).toBe(1);
    await expect(page.locator("#invoicesPanel")).toContainText("RE-2026-000001");

    await page.locator("button:has-text('Profil speichern')").click();
    await expect.poll(() => profileSaveCount).toBe(1);

    expect(pageErrors, "page errors on /invoices").toEqual([]);
  });

  test("orders route renders current table state and opens details for visible rows", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route(/\/api\/orders\/[^/]+\/[^/?#]+$/, async (route) => {
      const url = new URL(route.request().url());
      const parts = url.pathname.split("/");
      const marketplace = parts.at(-2) || "shopify";
      const orderId = parts.at(-1) || "order-1";
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          summary: {
            marketplace,
            order_id: orderId,
            external_order_id: "TEST-ORDER-1",
            order_date: "2026-02-03T10:00:00Z",
            customer: "Alice Example",
            payment_method: "card",
            fulfillment_status: "fulfilled",
            total_cents: 12990,
            fees_cents: 990,
            fee_source: "api",
            after_fees_cents: 12000,
            purchase_cost_cents: 4500,
            profit_cents: 7500,
            currency: "EUR",
            invoice: null,
          },
          order: {
            customer_email: "alice@example.com",
            currency: "EUR",
          },
          customer: {
            name: "Alice Example",
            email: "alice@example.com",
          },
          shipping_address: {
            name: "Alice Example",
            city: "Berlin",
            country: "DE",
          },
          billing_address: {
            name: "Alice Example",
            city: "Berlin",
            country: "DE",
          },
          line_items: [],
          transactions: [],
          fulfillments: [],
          refunds: [],
          bookkeeping_breakdown: {
            db_available: false,
          },
        }),
      });
    });

    await page.goto("/orders", { waitUntil: "networkidle" });

    await expect(page.locator("#ordersMeta")).toContainText("Zeilen");

    const rows = page.locator("#ordersBody tr[data-react-orders-row='true']");
    if ((await rows.count()) > 0) {
      const firstRow = rows.first();
      await expect(firstRow).toBeVisible();
      await firstRow.locator("td").first().click();
      await expect(page.locator("#detailsModal")).toHaveClass(/active/);
      await expect(page.locator("#detailsTitle")).toContainText("Details");
      await expect(page.locator("#detailsContent")).toContainText("TEST-ORDER-1");
    } else {
      await expect(page.locator("#ordersBody tr[data-react-orders-empty='true']")).toBeVisible();
    }

    expect(pageErrors, "page errors in orders runtime state").toEqual([]);
  });

  test("order detail returns after booking detail closes", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              marketplace: "shopify",
              order_id: "order-1",
              external_order_id: "TEST-ORDER-1",
              order_date: "2026-02-03T10:00:00Z",
              customer: "Alice Example",
              article: "Alpha Product",
              line_items_count: 1,
              total_cents: 12990,
              fees_cents: 990,
              fee_source: "api",
              after_fees_cents: 12000,
              purchase_cost_cents: 4500,
              profit_cents: 7500,
              fulfillment_status: "fulfilled",
              payment_method: "card",
              currency: "EUR",
              invoice: null,
            },
          ],
          total: 1,
        }),
      });
    });

    await page.route(/\/api\/orders\/[^/]+\/[^/?#]+$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          summary: {
            marketplace: "shopify",
            order_id: "order-1",
            external_order_id: "TEST-ORDER-1",
            order_date: "2026-02-03T10:00:00Z",
            customer: "Alice Example",
            payment_method: "card",
            fulfillment_status: "fulfilled",
            total_cents: 12990,
            fees_cents: 990,
            fee_source: "api",
            after_fees_cents: 12000,
            purchase_cost_cents: 4500,
            profit_cents: 7500,
            currency: "EUR",
            invoice: null,
          },
          order: {
            customer_email: "alice@example.com",
            currency: "EUR",
          },
          customer: {
            name: "Alice Example",
            email: "alice@example.com",
          },
          shipping_address: {
            name: "Alice Example",
            city: "Berlin",
            country: "DE",
          },
          billing_address: {
            name: "Alice Example",
            city: "Berlin",
            country: "DE",
          },
          line_items: [],
          transactions: [],
          fulfillments: [],
          refunds: [],
          bookkeeping_breakdown: {
            db_available: true,
            matched_via: "order_id",
            income_total_cents: 12990,
            additional_expense_total_cents: 0,
            mirrored_fee_total_cents: 0,
            mirrored_cogs_total_cents: 0,
            additional_fee_cents: 0,
            additional_cogs_cents: 0,
            additional_other_cents: 0,
            type_breakdown: [],
            transactions: [
              {
                id: "tx-1",
                date: "2026-02-03T12:00:00Z",
                type: "SALE",
                direction: "IN",
                amount_gross: 12345,
                reference: "TX-REF-1",
                document_id: "doc-1",
                document_original_filename: "invoice.pdf",
                document_mime_type: "application/pdf",
              },
            ],
            documents: [],
          },
        }),
      });
    });

    await page.route("**/api/bookings/payment-accounts", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              id: "acc-1",
              name: "Bank DE",
              provider: "bank",
              is_active: true,
            },
          ],
          total: 1,
        }),
      });
    });

    await page.route("**/api/bookings/templates", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              id: "tpl-1",
              name: "Template A",
              schedule: "monthly",
              default_amount_gross: 12345,
            },
          ],
          total: 1,
        }),
      });
    });

    await page.route(/\/api\/bookings\/transactions\/tx-1$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          transaction: {
            id: "tx-1",
            date: "2026-02-03T12:00:00Z",
            type: "SALE",
            direction: "IN",
            amount_gross: 12345,
            provider: "shopify",
            status: "confirmed",
            currency: "EUR",
            reference: "TX-REF-1",
            counterparty_name: "Alice Example",
            category: "order",
            payment_account_id: "acc-1",
            payment_account: {
              name: "Bank DE",
            },
            template_id: "tpl-1",
            template: {
              name: "Template A",
            },
            document_id: "doc-1",
            document: {
              original_filename: "invoice.pdf",
              mime_type: "application/pdf",
            },
            order: {
              provider: "shopify",
              external_order_id: "TEST-ORDER-1",
            },
            source: "manual",
            source_key: "tx-1",
            notes: "created via fixture",
          },
        }),
      });
    });

    await page.goto("/orders", { waitUntil: "networkidle" });

    const firstRow = page.locator("#ordersBody tr[data-react-orders-row='true']").first();
    await expect(firstRow).toBeVisible();
    await firstRow.locator("td").first().click();
    await expect(page.locator("#detailsContent")).toContainText("TEST-ORDER-1");

    await page.locator("#detailsContent tr[data-tx-id='tx-1'] td").first().click();
    await expect(page.locator("#detailsTitle")).toContainText("Transaktion");
    await expect(page.locator("#detailsContent")).toContainText("TX-REF-1");

    await page.locator("#closeModalBtn").click();
    await expect(page.locator("#detailsModal")).toHaveClass(/active/);
    await expect(page.locator("#detailsContent")).toContainText("TEST-ORDER-1");
    await expect(page.locator("#ordersDetailsContent")).toBeVisible();

    expect(pageErrors, "page errors in order to booking return flow").toEqual([]);
  });

  test("customers route mounts React panel host", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.goto("/customers", { waitUntil: "networkidle" });

    await expect(page.locator("#customersPanel")).toHaveAttribute("data-react-customers-mounted", "true");

    expect(pageErrors, "page errors on /customers React host").toEqual([]);
  });

  test("order detail submits Kaufland shipment with predefined carrier", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let shipmentPatchCount = 0;

    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              marketplace: "kaufland",
              order_id: "order-k-1",
              external_order_id: "KAUF-ORDER-1",
              order_date: "2026-02-03T10:00:00Z",
              customer: "Alice Example",
              article: "Alpha Product",
              line_items_count: 1,
              total_cents: 12990,
              fees_cents: 990,
              fee_source: "api",
              after_fees_cents: 12000,
              purchase_cost_cents: 4500,
              profit_cents: 7500,
              currency: "EUR",
              invoice: null,
              fulfillment_status: "need_to_be_sent",
              payment_method: "Kaufland Settlement",
            },
          ],
          total: 1,
        }),
      });
    });

    await page.route("**/api/orders/kaufland/order-k-1", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          summary: {
            marketplace: "kaufland",
            order_id: "order-k-1",
            external_order_id: "KAUF-ORDER-1",
            order_date: "2026-02-03T10:00:00Z",
            customer: "Alice Example",
            payment_method: "Kaufland Settlement",
            fulfillment_status: "need_to_be_sent",
            total_cents: 12990,
            fees_cents: 990,
            fee_source: "api",
            after_fees_cents: 12000,
            purchase_cost_cents: 4500,
            profit_cents: 7500,
            currency: "EUR",
            invoice: null,
          },
          order: {},
          customer: { name: "Alice Example", email: "alice@example.com" },
          shipping_address: { name: "Alice Example", city: "Berlin", country: "DE" },
          billing_address: { name: "Alice Example", city: "Berlin", country: "DE" },
          units: [
            { id_order_unit: "unit-1", product_title: "Alpha Product", status: "need_to_be_sent", price: "129.90", revenue_gross: "120.00", vat: 19 },
          ],
          bookkeeping_breakdown: { db_available: false },
          shipment_capabilities: {
            available: true,
            marketplace: "kaufland",
            carrier_options: ["DHL", "UPS"],
            pending_units_count: 1,
            pending_units: [{ id_order_unit: "unit-1", product_title: "Alpha Product", status: "need_to_be_sent" }],
            requires_tracking_number: true,
          },
        }),
      });
    });

    await page.route("**/api/orders/kaufland/order-k-1/shipment", async (route) => {
      shipmentPatchCount += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          shipment: { marketplace: "kaufland", carrier: "UPS", tracking_number: "1Z001985YW99744790" },
          summary: {
            marketplace: "kaufland",
            order_id: "order-k-1",
            external_order_id: "KAUF-ORDER-1",
            fulfillment_status: "sent",
          },
          detail: {
            summary: {
              marketplace: "kaufland",
              order_id: "order-k-1",
              external_order_id: "KAUF-ORDER-1",
              fulfillment_status: "sent",
            },
            order: {},
            customer: { name: "Alice Example", email: "alice@example.com" },
            shipping_address: { name: "Alice Example", city: "Berlin", country: "DE" },
            billing_address: { name: "Alice Example", city: "Berlin", country: "DE" },
            units: [
              { id_order_unit: "unit-1", product_title: "Alpha Product", status: "sent", price: "129.90", revenue_gross: "120.00", vat: 19 },
            ],
            bookkeeping_breakdown: { db_available: false },
            shipment_capabilities: {
              available: false,
              marketplace: "kaufland",
              carrier_options: ["DHL", "UPS"],
              pending_units_count: 0,
              pending_units: [],
              requires_tracking_number: true,
              reason: "Diese Kaufland-Bestellung ist bereits als versendet markiert.",
            },
          },
        }),
      });
    });

    await page.goto("/orders", { waitUntil: "networkidle" });
    await page.locator("#ordersBody tr[data-react-orders-row='true'] td").first().click();
    await expect(page.locator("#ordersDetailsContent")).toContainText("Versand & Tracking");

    await page.locator("#ordersDetailsContent [data-shipment-carrier-select='true']").selectOption("UPS");
    await page.locator("#ordersDetailsContent [data-shipment-tracking-input='true']").fill("1Z001985YW99744790");
    await page.locator("#ordersDetailsContent [data-action='submit-shipment']").click();

    await expect.poll(() => shipmentPatchCount).toBe(1);
    await expect(page.locator("#ordersDetailsContent")).toContainText("Versanddaten wurden gespeichert.");
    await expect(page.locator("#ordersDetailsContent")).toContainText("bereits als versendet markiert");
    expect(pageErrors).toEqual([]);
  });

  test("order detail submits Shopify shipment with predefined carrier", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let shipmentPatchCount = 0;

    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              marketplace: "shopify",
              order_id: "order-s-1",
              external_order_id: "SHOP-ORDER-1",
              order_date: "2026-02-03T10:00:00Z",
              customer: "Alice Example",
              article: "Alpha Product",
              line_items_count: 1,
              total_cents: 12990,
              fees_cents: 990,
              fee_source: "api",
              after_fees_cents: 12000,
              purchase_cost_cents: 4500,
              profit_cents: 7500,
              currency: "EUR",
              invoice: null,
              fulfillment_status: "",
              payment_method: "Shopify Payments",
            },
          ],
          total: 1,
        }),
      });
    });

    await page.route("**/api/orders/shopify/order-s-1", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          summary: {
            marketplace: "shopify",
            order_id: "order-s-1",
            external_order_id: "SHOP-ORDER-1",
            order_date: "2026-02-03T10:00:00Z",
            customer: "Alice Example",
            payment_method: "Shopify Payments",
            fulfillment_status: "",
            total_cents: 12990,
            fees_cents: 990,
            fee_source: "api",
            after_fees_cents: 12000,
            purchase_cost_cents: 4500,
            profit_cents: 7500,
            currency: "EUR",
            invoice: null,
          },
          order: { customer_email: "alice@example.com", currency: "EUR" },
          customer: { name: "Alice Example", email: "alice@example.com" },
          shipping_address: { name: "Alice Example", city: "Berlin", country: "DE" },
          billing_address: { name: "Alice Example", city: "Berlin", country: "DE" },
          line_items: [
            { id: "line-1", title: "Alpha Product", quantity: 1, price: "129.90", fulfillment_status: "", sku: "SKU-1" },
          ],
          fulfillments: [],
          refunds: [],
          transactions: [],
          bookkeeping_breakdown: { db_available: false },
          shipment_capabilities: {
            available: true,
            marketplace: "shopify",
            carrier_options: ["DHL", "UPS"],
            pending_units_count: 1,
            pending_units: [{ id: "line-1", product_title: "Alpha Product", status: "open" }],
            requires_tracking_number: true,
          },
        }),
      });
    });

    await page.route("**/api/orders/shopify/order-s-1/shipment", async (route) => {
      shipmentPatchCount += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          shipment: { marketplace: "shopify", carrier: "DHL", tracking_number: "00340434161234567890" },
          summary: {
            marketplace: "shopify",
            order_id: "order-s-1",
            external_order_id: "SHOP-ORDER-1",
            fulfillment_status: "fulfilled",
          },
          detail: {
            summary: {
              marketplace: "shopify",
              order_id: "order-s-1",
              external_order_id: "SHOP-ORDER-1",
              fulfillment_status: "fulfilled",
            },
            order: { customer_email: "alice@example.com", currency: "EUR" },
            customer: { name: "Alice Example", email: "alice@example.com" },
            shipping_address: { name: "Alice Example", city: "Berlin", country: "DE" },
            billing_address: { name: "Alice Example", city: "Berlin", country: "DE" },
            line_items: [
              { id: "line-1", title: "Alpha Product", quantity: 1, price: "129.90", fulfillment_status: "fulfilled", sku: "SKU-1" },
            ],
            fulfillments: [
              { id: "ful-1", status: "success", tracking_number: "00340434161234567890", tracking_company: "DHL", created_at: "2026-02-03T11:00:00Z" },
            ],
            refunds: [],
            transactions: [],
            bookkeeping_breakdown: { db_available: false },
            shipment_capabilities: {
              available: false,
              marketplace: "shopify",
              carrier_options: ["DHL", "UPS"],
              pending_units_count: 0,
              pending_units: [],
              requires_tracking_number: true,
              reason: "Diese Shopify-Bestellung ist bereits als versendet markiert.",
            },
          },
        }),
      });
    });

    await page.goto("/orders", { waitUntil: "networkidle" });
    await page.locator("#ordersBody tr[data-react-orders-row='true'] td").first().click();
    await page.locator("#ordersDetailsContent [data-shipment-carrier-select='true']").selectOption("DHL");
    await page.locator("#ordersDetailsContent [data-shipment-tracking-input='true']").fill("00340434161234567890");
    await page.locator("#ordersDetailsContent [data-action='submit-shipment']").click();

    await expect.poll(() => shipmentPatchCount).toBe(1);
    await expect(page.locator("#ordersDetailsContent")).toContainText("Versanddaten wurden gespeichert.");
    await expect(page.locator("#ordersDetailsContent")).toContainText("bereits als versendet markiert");
    expect(pageErrors).toEqual([]);
  });

  test("customers route renders geo stage and current list state", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.goto("/customers", { waitUntil: "networkidle" });

    await expect(page.locator("#customersReactTop")).toContainText("Kunden (gemerged)");
    await expect(page.locator("#customersReactBottom .table-title")).toContainText("Kundenliste");
    await expect(page.locator("#customerGeoMapView")).toHaveClass(/active/);
    await expect(page.locator("#customerGeoSub")).not.toHaveText(/^\s*$/);

    const customerNames = page.locator("#customersReactBottom .customer-name-main");
    if ((await customerNames.count()) > 0) {
      await expect(customerNames.first()).toBeVisible();
    } else {
      await expect(page.locator("#customersReactBottom")).toContainText("Keine Kunden fuer den aktuellen Filter.");
    }

    expect(pageErrors, "page errors in customers runtime state").toEqual([]);
  });

  test("google ads route mounts React panel host", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route("**/api/google-ads/analytics?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          kpis: {
            ads_cost_total_cents: 1234,
            ads_cost_mapped_cents: 1000,
            ads_cost_unmapped_cents: 234,
            shopify_revenue_total_cents: 4321,
            orders_count: 2,
            profit_before_ads_total_cents: 2100,
            profit_after_ads_total_cents: 866,
            roas: 3.5,
            missing_assignments_count: 1,
            products_count: 1,
          },
          imports: {
            report: {
              filename: "report.csv",
              imported_at: "2026-01-02T10:00:00Z",
              meta: {
                report_from_day: "2026-01-01",
                report_to_day: "2026-01-02",
                rows: 2,
                non_zero_rows: 2,
              },
            },
            assignment: {
              filename: "assignment.csv",
              imported_at: "2026-01-02T10:00:00Z",
              meta: {
                rows: 1,
              },
            },
          },
          products: [
            {
              product_key: "sku-a",
              product_label: "Alpha Product",
              product_detail: "Alpha detail",
              mapped: true,
              ads_cost_cents: 1234,
              order_count: 2,
              revenue_total_cents: 4321,
              profit_before_ads_cents: 2100,
              profit_after_ads_cents: 866,
            },
          ],
          missing_assignments: [
            {
              article_id: "missing-1",
              ads_cost_cents: 234,
              day_count: 1,
            },
          ],
          trend: [
            { day: "2026-01-01", ads_cost_cents: 400, mapped_ads_cost_cents: 300, revenue_cents: 1500, profit_cents: 700, order_count: 1 },
            { day: "2026-01-02", ads_cost_cents: 834, mapped_ads_cost_cents: 700, revenue_cents: 2821, profit_cents: 1400, order_count: 1 },
          ],
        }),
      });
    });

    await page.goto("/google-ads", { waitUntil: "networkidle" });

    await expect(page.locator("#googleAdsPanel")).toHaveAttribute("data-react-google-ads-mounted", "true");
    await expect(page.locator("#googleAdsReactRoot")).toContainText("Google Ads CSV Import");
    await expect(page.locator("#googleAdsImportMeta")).toHaveCount(0);
    await expect(page.locator("#googleAdsProductsBody")).toHaveCount(0);

    const productRows = page.locator("#googleAdsReactRoot .ga-product-row");
    if ((await productRows.count()) === 0) {
      await expect(page.locator("#googleAdsReactRoot")).toContainText("Keine Daten fuer den aktuellen Filter.");
    } else {
      await expect(productRows.first()).toBeVisible();
    }

    expect(pageErrors, "page errors on /google-ads React host").toEqual([]);
  });

  test("google ads file labels sync and product detail opens", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route("**/api/google-ads/analytics?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          kpis: {
            ads_cost_total_cents: 1234,
            shopify_revenue_total_cents: 4321,
            orders_count: 2,
            profit_after_ads_total_cents: 866,
            profit_before_ads_total_cents: 2100,
            roas: 3.5,
            missing_assignments_count: 1,
            ads_cost_mapped_cents: 1000,
            ads_cost_unmapped_cents: 234,
            products_count: 1,
          },
          imports: {
            report: {
              filename: "report.csv",
              imported_at: "2026-01-02T10:00:00Z",
              meta: {
                report_from_day: "2026-01-01",
                report_to_day: "2026-01-02",
                rows: 2,
                non_zero_rows: 2,
              },
            },
            assignment: {
              filename: "assignment.csv",
              imported_at: "2026-01-02T10:00:00Z",
              meta: {
                rows: 1,
              },
            },
          },
          products: [
            {
              product_key: "sku-a",
              product_label: "Alpha Product",
              product_detail: "Alpha detail",
              mapped: true,
              ads_cost_cents: 1234,
              order_count: 2,
              revenue_total_cents: 4321,
              profit_before_ads_cents: 2100,
              profit_after_ads_cents: 866,
            },
          ],
          missing_assignments: [
            {
              article_id: "missing-1",
              ads_cost_cents: 234,
              day_count: 1,
            },
          ],
          trend: [
            { day: "2026-01-01", ads_cost_cents: 400, mapped_ads_cost_cents: 300, revenue_cents: 1500, profit_cents: 700, order_count: 1 },
            { day: "2026-01-02", ads_cost_cents: 834, mapped_ads_cost_cents: 700, revenue_cents: 2821, profit_cents: 1400, order_count: 1 },
          ],
        }),
      });
    });

    await page.route(/\/api\/google-ads\/product-detail\?/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          kpis: {
            ads_cost_total_cents: 1234,
            revenue_total_cents: 4321,
            profit_before_ads_cents: 2100,
            profit_after_ads_cents: 866,
            roas: 3.5,
            orders_count: 2,
          },
          trend: [
            { day: "2026-01-01", ads_cost_cents: 400, revenue_cents: 1500, profit_cents: 700 },
            { day: "2026-01-02", ads_cost_cents: 834, revenue_cents: 2821, profit_cents: 1400 },
          ],
        }),
      });
    });

    await page.goto("/google-ads", { waitUntil: "networkidle" });

    await page.setInputFiles("#googleAdsReportInput", {
      name: "report.csv",
      mimeType: "text/csv",
      buffer: Buffer.from("date,cost\n2026-01-01,1\n"),
    });
    await expect(page.locator("#googleAdsReportFileLabel")).toHaveText("report.csv");

    await page.setInputFiles("#googleAdsAssignmentInput", {
      name: "assignment.csv",
      mimeType: "text/csv",
      buffer: Buffer.from("sku,article\na,b\n"),
    });
    await expect(page.locator("#googleAdsAssignmentFileLabel")).toHaveText("assignment.csv");

    const firstProductRow = page.locator("#googleAdsReactRoot .ga-product-row[data-product-key='sku-a']");
    await expect(firstProductRow).toBeVisible();
    await firstProductRow.click();
    await expect(page.locator("#gaProductDetailRow")).toBeVisible();
    await expect(firstProductRow).toHaveClass(/ga-row-active/);
    await expect(page.locator("#gaProductDetailRow .ga-product-detail")).toContainText("Ads Kosten");

    expect(pageErrors, "page errors in google ads detail flow").toEqual([]);
  });

  test("ebay route mounts React panel host", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route("**/api/ebay/summary", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          available: true,
          kpis: {
            total_orders: 2,
            total_returns: 0,
            total_revenue: 120,
            total_purchase: 40,
            total_fees: 10,
            total_profit: 70,
            margin_pct: 58.3,
            first_date: "2026-01-01",
            last_date: "2026-01-02",
          },
          shops: [
            { shop: "alpha", count: 2, first_date: "2026-01-01", last_date: "2026-01-02", revenue: 120, fees: 10, purchase: 40, profit: 70 },
          ],
          top_articles: [{ artikel: "Alpha Item", count: 2, revenue: 120, profit: 70 }],
          import_meta: {
            imported_at: "2026-01-03T10:00:00Z",
            source_file: "ebay.csv",
            shops: "alpha",
            total_orders: 2,
            total_returns: 0,
          },
        }),
      });
    });
    await page.route(/\/api\/ebay\/orders(?:\?.*)?$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          orders: [
            { datum: "2026-01-01", shop: "alpha", category: "order", artikel: "Alpha Item", kunde_name: "Alice", order_number: "A-1", preis: 60, gebuehren: 5, ali_preis: 20, gewinn: 35, is_return: 0 },
            { datum: "2026-01-02", shop: "alpha", category: "computer", artikel: "Alpha PC", kunde_name: "Bob", order_number: "A-2", preis: 60, gebuehren: 5, ali_preis: 20, gewinn: 35, is_return: 0 },
          ],
          total: 2,
        }),
      });
    });

    await page.goto("/ebay", { waitUntil: "networkidle" });

    await expect(page.locator("#ebayPanel")).toHaveAttribute("data-react-ebay-mounted", "true");
    await expect(page.locator("#ebayReactRoot")).toContainText("eBay Shops");
    await expect(page.locator("#ebayShopFilter")).toHaveCount(0);
    await expect(page.locator("#ebayCategoryFilter")).toHaveCount(0);

    const firstOrderText = await page.locator("#ebayReactRoot .orders-table tbody tr").first().textContent();
    if (String(firstOrderText || "").includes("Keine eBay Bestellungen fuer aktuellen Filter.")) {
      await expect(page.locator("#ebayReactRoot")).toContainText("Keine eBay Daten importiert.");
    } else {
      await expect(page.locator("#ebayReactRoot .orders-table tbody tr").first()).toBeVisible();
    }

    expect(pageErrors, "page errors on /ebay React host").toEqual([]);
  });

  test("ebay react filters update rendered rows without legacy selects", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route("**/api/ebay/summary", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          available: true,
          kpis: {
            total_orders: 3,
            total_returns: 1,
            total_revenue: 210,
            total_purchase: 90,
            total_fees: 20,
            total_profit: 100,
            margin_pct: 47.6,
            first_date: "2026-01-01",
            last_date: "2026-01-03",
          },
          shops: [
            { shop: "alpha", count: 2, first_date: "2026-01-01", last_date: "2026-01-02", revenue: 120, fees: 10, purchase: 50, profit: 60 },
            { shop: "beta", count: 1, first_date: "2026-01-03", last_date: "2026-01-03", revenue: 90, fees: 10, purchase: 40, profit: 40 },
          ],
          top_articles: [
            { artikel: "Alpha Item", count: 2, revenue: 120, profit: 60 },
          ],
          import_meta: {
            imported_at: "2026-01-03T10:00:00Z",
            source_file: "ebay.csv",
            shops: "alpha,beta",
            total_orders: 3,
            total_returns: 1,
          },
        }),
      });
    });
    await page.route(/\/api\/ebay\/orders(?:\?.*)?$/, async (route) => {
      const url = new URL(route.request().url());
      const shop = url.searchParams.get("shop") || "";
      const category = url.searchParams.get("category") || "";
      const orders = [
        { datum: "2026-01-01", shop: "alpha", category: "order", artikel: "Alpha Item", kunde_name: "Alice", order_number: "A-1", preis: 60, gebuehren: 5, ali_preis: 25, gewinn: 30, is_return: 0 },
        { datum: "2026-01-02", shop: "alpha", category: "computer", artikel: "Alpha PC", kunde_name: "Bob", order_number: "A-2", preis: 60, gebuehren: 5, ali_preis: 25, gewinn: 30, is_return: 0 },
        { datum: "2026-01-03", shop: "beta", category: "return", artikel: "Beta Return", kunde_name: "Cara", order_number: "B-1", preis: 90, gebuehren: 10, ali_preis: 40, gewinn: 40, is_return: 1 },
      ].filter((order) => {
        if (shop && order.shop !== shop) {
          return false;
        }
        if (category && order.category !== category) {
          return false;
        }
        return true;
      });
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ orders, total: orders.length }),
      });
    });

    await page.goto("/ebay", { waitUntil: "networkidle" });

    const selects = page.locator("#ebayReactRoot select");
    await expect(selects.nth(0)).toHaveValue("");
    await expect(selects.nth(0).locator("option")).toHaveCount(3);
    await expect(page.locator("#ebayReactRoot")).toContainText("alpha");
    await expect(page.locator("#ebayReactRoot")).toContainText("beta");

    await selects.nth(0).selectOption("alpha");
    const ordersBody = page.locator("#ebayReactRoot .orders-table tbody");
    await expect(ordersBody).toContainText("A-1");
    await expect(ordersBody).toContainText("A-2");
    await expect(ordersBody).not.toContainText("B-1");

    await selects.nth(1).selectOption("computer");
    await expect(ordersBody).toContainText("Alpha PC");
    await expect(ordersBody).not.toContainText("Alpha Item");
    await expect(ordersBody).toHaveText(/Alpha PC/);
    expect(pageErrors, "page errors in ebay filter flow").toEqual([]);
  });

  test("ebay orders use real pagination controls", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    const allOrders = Array.from({ length: 220 }, (_, index) => ({
      datum: `2026-01-${String((index % 28) + 1).padStart(2, "0")}`,
      shop: index % 2 === 0 ? "alpha" : "beta",
      category: index % 3 === 0 ? "computer" : "order",
      artikel: `Article ${index + 1}`,
      kunde_name: `Customer ${index + 1}`,
      order_number: `EB-${index + 1}`,
      preis: 40 + index,
      gebuehren: 5,
      ali_preis: 12 + index,
      gewinn: 23,
      is_return: 0,
    }));

    await page.route("**/api/ebay/summary", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          available: true,
          kpis: {
            total_orders: allOrders.length,
            total_returns: 0,
            total_revenue: 1000,
            total_purchase: 400,
            total_fees: 80,
            total_profit: 520,
            margin_pct: 52,
            first_date: "2026-01-01",
            last_date: "2026-01-28",
          },
          shops: [
            { shop: "alpha", count: 110, first_date: "2026-01-01", last_date: "2026-01-28", revenue: 500, fees: 40, purchase: 200, profit: 260 },
            { shop: "beta", count: 110, first_date: "2026-01-01", last_date: "2026-01-28", revenue: 500, fees: 40, purchase: 200, profit: 260 },
          ],
          top_articles: [{ artikel: "Article 1", count: 1, revenue: 40, profit: 23 }],
          import_meta: {
            imported_at: "2026-01-29T10:00:00Z",
            source_file: "ebay.csv",
            shops: "alpha,beta",
            total_orders: allOrders.length,
            total_returns: 0,
          },
        }),
      });
    });

    const orderRequests: Array<{ limit: string; offset: string; shop: string; category: string }> = [];
    await page.route(/\/api\/ebay\/orders(?:\?.*)?$/, async (route) => {
      const url = new URL(route.request().url());
      const limit = String(url.searchParams.get("limit") || "150");
      const offset = String(url.searchParams.get("offset") || "0");
      const shop = String(url.searchParams.get("shop") || "");
      const category = String(url.searchParams.get("category") || "");
      orderRequests.push({ limit, offset, shop, category });

      const filtered = allOrders.filter((order) => {
        if (shop && order.shop !== shop) {
          return false;
        }
        if (category && order.category !== category) {
          return false;
        }
        return true;
      });
      const numericLimit = Number(limit || "150");
      const numericOffset = Number(offset || "0");
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          orders: filtered.slice(numericOffset, numericOffset + numericLimit),
          total: filtered.length,
          limit: numericLimit,
          offset: numericOffset,
        }),
      });
    });

    await page.goto("/ebay", { waitUntil: "networkidle" });

    await expect(page.locator("#ebayOrdersMeta")).toContainText("1-150 / 220 Zeilen");
    await expect(page.locator("#ebayReactRoot .orders-table tbody tr")).toHaveCount(150);
    await expect(page.locator("#ebayReactRoot")).toContainText("Seite 1 von 2");
    await expect(page.locator("#ebayPrevPageBtn")).toBeDisabled();
    await expect(page.locator("#ebayNextPageBtn")).toBeEnabled();

    await page.locator("#ebayNextPageBtn").click();
    await expect(page.locator("#ebayOrdersMeta")).toContainText("151-220 / 220 Zeilen");
    await expect(page.locator("#ebayReactRoot .orders-table tbody tr")).toHaveCount(70);
    await expect(page.locator("#ebayPrevPageBtn")).toBeEnabled();
    await expect(page.locator("#ebayNextPageBtn")).toBeDisabled();
    await expect(page.locator("#ebayReactRoot")).toContainText("EB-220");

    await page.locator("#ebayShopSelect").selectOption("alpha");
    await expect(page.locator("#ebayOrdersMeta")).toContainText("1-110 / 110 Zeilen");
    await expect(page.locator("#ebayPrevPageBtn")).toHaveCount(0);
    await expect(page.locator("#ebayNextPageBtn")).toHaveCount(0);

    expect(orderRequests.map((request) => `${request.limit}:${request.offset}:${request.shop}:${request.category}`)).toEqual([
      "150:0::",
      "150:150::",
      "150:0:alpha:",
    ]);
    expect(pageErrors).toEqual([]);
  });

  test("settings design button opens React-owned theme modal", async ({ page }) => {
    await page.goto("/orders", { waitUntil: "networkidle" });

    await page.locator("#sidebarSettingsBtn").click();
    await expect(page.locator("#settingsPanel")).toHaveClass(/active/);

    await page.locator("#themeModalOpenBtn").click();
    await expect(page.locator("#themeModal")).toHaveAttribute("data-react-owned", "true");
    await expect(page.locator("#themeModal")).toHaveClass(/active/);
    await expect(page.locator("#settingsPanel")).not.toHaveClass(/active/);

    await page.locator('#themeModal .theme-card[data-theme-id="dark"]').click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");

    await page.locator("#customThemeCard").click();
    await expect(page.locator("#customThemeEditor")).toBeVisible();

    await page.locator("#cteBackBtn").click();
    await expect(page.locator("#themeGridView")).toBeVisible();

    await page.locator("#themeModalCloseBtn").click();
    await expect(page.locator("#themeModal")).not.toHaveClass(/active/);
  });

  test("settings layout button follows active route state", async ({ page }) => {
    await disablePolling(page);
    await page.route("**/api/analytics/kpis?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          order_count: 1,
          revenue_total_cents: 1000,
          fees_total_cents: 100,
          after_fees_total_cents: 900,
          purchase_total_cents: 200,
          profit_total_cents: 700,
        }),
      });
    });
    await page.goto("/analytics", { waitUntil: "networkidle" });

    await openSettings(page);
    await expect(page.locator("#layoutEditMenuBtn")).toBeEnabled();
    await page.locator("#closeSettingsBtn").click();

    await page.locator("#tabOrdersBtn").click();
    await expect(page).toHaveURL(/\/orders$/);

    await openSettings(page);
    await expect(page.locator("#layoutEditMenuBtn")).toBeDisabled();
  });

  test("global refresh only reloads the active route", async ({ page }) => {
    await disablePolling(page);

    let analyticsRequests = 0;
    let ordersRequests = 0;
    let customersOverviewRequests = 0;
    let customersLocationRequests = 0;
    let bookingsRequests = 0;
    let googleAdsRequests = 0;
    let ebaySummaryRequests = 0;
    let ebayOrdersRequests = 0;

    await page.route("**/api/health", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ sync_status: {}, live_sync_status: {}, bookkeeping_module: {} }),
      });
    });
    await page.route("**/api/sync/credentials", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ shopify_configured: false, kaufland_configured: false, storage: "environment" }),
      });
    });

    await page.route("**/api/analytics/kpis?**", async (route) => {
      analyticsRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          order_count: 1,
          revenue_total_cents: 1000,
          fees_total_cents: 100,
          purchase_total_cents: 200,
          profit_total_cents: 700,
          marketplace_split: { shopify_revenue_cents: 1000, kaufland_revenue_cents: 0 },
          previous_period: null,
          trend: [],
          top_articles: [],
          top_customers: [],
          payments: [],
        }),
      });
    });
    await page.route("**/api/orders?**", async (route) => {
      ordersRequests += 1;
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });
    await page.route("**/api/customers?**", async (route) => {
      customersOverviewRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ total: 0, kpis: {}, items: [] }),
      });
    });
    await page.route("**/api/customers/locations?**", async (route) => {
      customersLocationRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ summary: { orders_total: 0, unresolved_orders_count: 0, points_total: 0 }, points: [] }),
      });
    });
    await page.route("**/api/bookings/**", async (route) => {
      bookingsRequests += 1;
      const url = route.request().url();
      if (url.includes("/transactions")) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ items: [], total: 0, category_counts: {}, limit: 150, offset: 0 }),
        });
        return;
      }
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });
    await page.route("**/api/google-ads/analytics?**", async (route) => {
      googleAdsRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ kpis: {}, imports: {}, products: [], missing_assignments: [], trend: [] }),
      });
    });
    await page.route("**/api/ebay/summary", async (route) => {
      ebaySummaryRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ available: true, kpis: {}, shops: [], top_articles: [], import_meta: {} }),
      });
    });
    await page.route("**/api/ebay/orders**", async (route) => {
      ebayOrdersRequests += 1;
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ orders: [], total: 0 }) });
    });

    await page.goto("/analytics", { waitUntil: "networkidle" });
    const baseline = {
      analyticsRequests,
      ordersRequests,
      customersOverviewRequests,
      customersLocationRequests,
      bookingsRequests,
      googleAdsRequests,
      ebaySummaryRequests,
      ebayOrdersRequests,
    };

    await expect(page.locator("#tabOrdersBtn")).toBeVisible();
    await page.locator("#tabOrdersBtn").click();
    await page.waitForLoadState("networkidle");
    await page.locator("#tabCustomersBtn").click();
    await page.waitForLoadState("networkidle");
    await page.locator("#tabBookingsBtn").click();
    await page.waitForLoadState("networkidle");
    await page.locator("#tabGoogleAdsBtn").click();
    await page.waitForLoadState("networkidle");
    await page.locator("#tabEbayBtn").click();
    await page.waitForLoadState("networkidle");
    await page.locator("#tabAnalyticsBtn").click();
    await page.waitForLoadState("networkidle");

    const beforeRefresh = {
      analyticsRequests,
      ordersRequests,
      customersOverviewRequests,
      customersLocationRequests,
      bookingsRequests,
      googleAdsRequests,
      ebaySummaryRequests,
      ebayOrdersRequests,
    };

    await openSettings(page);
    await page.locator("#refreshBtn").click();
    await page.waitForTimeout(300);

    expect(analyticsRequests - beforeRefresh.analyticsRequests).toBe(1);
    expect(ordersRequests - beforeRefresh.ordersRequests).toBe(0);
    expect(customersOverviewRequests - beforeRefresh.customersOverviewRequests).toBe(0);
    expect(customersLocationRequests - beforeRefresh.customersLocationRequests).toBe(0);
    expect(bookingsRequests - beforeRefresh.bookingsRequests).toBe(0);
    expect(googleAdsRequests - beforeRefresh.googleAdsRequests).toBe(0);
    expect(ebaySummaryRequests - beforeRefresh.ebaySummaryRequests).toBe(0);
    expect(ebayOrdersRequests - beforeRefresh.ebayOrdersRequests).toBe(0);
    expect(baseline.analyticsRequests).toBeGreaterThan(0);
  });

  test("google ads refresh triggers only one analytics request when status panel is closed", async ({ page }) => {
    await disablePolling(page);
    let analyticsRequests = 0;

    await page.route("**/api/sync/credentials", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ shopify_configured: false, kaufland_configured: false, storage: "environment" }),
      });
    });

    await page.route("**/api/google-ads/analytics?**", async (route) => {
      analyticsRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ kpis: {}, imports: {}, products: [], missing_assignments: [], trend: [] }),
      });
    });

    await page.goto("/google-ads", { waitUntil: "networkidle" });
    const beforeRefresh = analyticsRequests;

    await openSettings(page);
    await page.locator("#refreshBtn").click();
    await page.waitForTimeout(300);

    expect(analyticsRequests - beforeRefresh).toBeLessThanOrEqual(1);
  });

  test("settings closes after global refresh so sidebar navigation stays clickable", async ({ page }) => {
    await disablePolling(page);

    await page.goto("/analytics", { waitUntil: "networkidle" });

    await openSettings(page);
    await page.locator("#refreshBtn").click();
    await expect(page.locator("#settingsPanel")).not.toHaveClass(/active/);
    await expect(page.locator("#settingsPanel")).toHaveAttribute("aria-hidden", "true");

    await page.locator("#tabOrdersBtn").click();
    await expect(page).toHaveURL(/\/orders$/);
  });

  test("customers globe failure falls back without page error", async ({ page }) => {
    await disablePolling(page);
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route("**/api/customers?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          total: 1,
          kpis: {
            customers_count: 1,
          },
          items: [
            {
              customer_id: "cust-1",
              customer_name: "Alice Example",
              marketplaces: ["shopify"],
              order_count: 1,
              repeat_customer: false,
              revenue_total_cents: 12990,
              profit_total_cents: 7500,
              last_order_date: "2026-02-03T10:00:00Z",
            },
          ],
        }),
      });
    });
    await page.route("**/api/customers/locations?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          summary: {
            orders_total: 1,
            unresolved_orders_count: 0,
            points_total: 1,
          },
          points: [
            {
              lat: 52.52,
              lng: 13.405,
              city: "Berlin",
              country: "Germany",
              order_count: 1,
              revenue_total_cents: 12990,
              profit_total_cents: 7500,
              dominant_marketplace: "shopify",
            },
          ],
        }),
      });
    });

    await page.addInitScript(() => {
      window.topojson = {
        feature: () => ({ features: [] }),
      } as Window["topojson"];
      window.Globe = class FakeGlobe {
        constructor() {
          throw new Error("forced globe init failure");
        }
      } as unknown as Window["Globe"];
    });

    await page.goto("/customers", { waitUntil: "networkidle" });
    await page.locator("#customerGeoModeGlobeBtn").click();

    await expect(page.locator("#customerGeoMapView")).toHaveClass(/active/);
    await expect(page.locator("#customerGeoGlobeView")).toContainText("Hex-Globus Fehler: forced globe init failure");
    expect(pageErrors).toEqual([]);
  });

  test("customers list uses real pagination controls while geo stays unpaginated", async ({ page }) => {
    await disablePolling(page);
    const pageErrors: string[] = [];
    const overviewRequests: Array<{ limit: string; offset: string }> = [];
    const geoRequests: Array<{ limit: string; offset: string }> = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    const allCustomers = Array.from({ length: 220 }, (_, index) => ({
      customer_id: `CUST-${String(index + 1).padStart(5, "0")}`,
      customer_name: `Customer ${index + 1}`,
      emails: [`customer${index + 1}@example.com`],
      phones: [`+4912345${String(index + 1).padStart(4, "0")}`],
      primary_address: {
        street: `Street ${index + 1}`,
        postcode: `10${String(index).padStart(3, "0")}`,
        city: "Berlin",
        country: "DE",
      },
      marketplaces: [index % 2 === 0 ? "shopify" : "kaufland"],
      order_count: (index % 4) + 1,
      repeat_customer: index % 3 === 0,
      revenue_total_cents: 10000 + index,
      profit_total_cents: 4000 + index,
      last_order_date: `2026-03-${String((index % 28) + 1).padStart(2, "0")}T10:00:00Z`,
      top_articles: [`Article ${index + 1}`],
    }));

    await page.route("**/api/customers?**", async (route) => {
      const url = new URL(route.request().url());
      const limit = String(url.searchParams.get("limit") || "");
      const offset = String(url.searchParams.get("offset") || "0");
      overviewRequests.push({ limit, offset });
      const numericLimit = Number(limit || "150");
      const numericOffset = Number(offset || "0");
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          total: allCustomers.length,
          items: allCustomers.slice(numericOffset, numericOffset + numericLimit),
          limit: numericLimit,
          offset: numericOffset,
          kpis: {
            customers_count: allCustomers.length,
            repeat_customers_count: allCustomers.filter((item) => item.repeat_customer).length,
            repeat_customers_rate_pct: 33.3,
            avg_orders_per_customer: 2.2,
            orders_total_count: 484,
            avg_revenue_per_customer_cents: 12345,
            revenue_total_cents: 987654,
            with_email_count: allCustomers.length,
            with_phone_count: allCustomers.length,
            with_address_count: allCustomers.length,
            cross_market_customers_count: 0,
            shopify_customers_count: 110,
            kaufland_customers_count: 110,
          },
        }),
      });
    });

    await page.route("**/api/customers/locations?**", async (route) => {
      const url = new URL(route.request().url());
      geoRequests.push({
        limit: String(url.searchParams.get("limit") || ""),
        offset: String(url.searchParams.get("offset") || ""),
      });
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          summary: {
            orders_total: 220,
            points_total: 2,
            unresolved_orders_count: 0,
            resolved_source_coordinates_count: 2,
            resolved_geocoded_count: 0,
            resolved_country_centroid_count: 0,
            geocode_attempts: 0,
            geocode_successes: 0,
            cache_location_hits: 2,
            cache_hit: true,
            generated_in_ms: 12,
          },
          points: [
            { lat: 52.52, lng: 13.405, order_count: 120, revenue_total_cents: 500000, profit_total_cents: 200000, dominant_marketplace: "shopify", country: "DE", city: "Berlin", weight: 120 },
            { lat: 48.137, lng: 11.575, order_count: 100, revenue_total_cents: 487654, profit_total_cents: 180000, dominant_marketplace: "kaufland", country: "DE", city: "Muenchen", weight: 100 },
          ],
        }),
      });
    });

    await page.goto("/customers", { waitUntil: "networkidle" });

    await expect(page.locator("#customersMeta")).toContainText("1-150 / 220 Zeilen");
    await expect(page.locator("#customersReactBottom tbody tr")).toHaveCount(150);
    await expect(page.locator("#customersReactBottom")).toContainText("Seite 1 von 2");
    await expect(page.locator("#customersPrevPageBtn")).toBeDisabled();
    await expect(page.locator("#customersNextPageBtn")).toBeEnabled();
    await expect(page.locator("#customersReactBottom")).toContainText("Customer 150");
    await expect(page.locator("#customersReactBottom")).not.toContainText("Customer 151");

    await page.locator("#customersNextPageBtn").click();
    await expect(page.locator("#customersMeta")).toContainText("151-220 / 220 Zeilen");
    await expect(page.locator("#customersReactBottom tbody tr")).toHaveCount(70);
    await expect(page.locator("#customersReactBottom")).toContainText("Customer 220");
    await expect(page.locator("#customersPrevPageBtn")).toBeEnabled();
    await expect(page.locator("#customersNextPageBtn")).toBeDisabled();

    expect(overviewRequests.map((request) => `${request.limit}:${request.offset}`)).toEqual(["150:0", "150:150"]);
    expect(geoRequests.map((request) => `${request.limit}:${request.offset}`)).toEqual([":"]);
    expect(pageErrors).toEqual([]);
  });

  test("bookings route mounts React panel host", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    await expect(page.locator("#bookingsPanel")).toHaveAttribute("data-react-bookings-mounted", "true");

    expect(pageErrors, "page errors on /bookings React host").toEqual([]);
  });

  test("bookings panel no longer renders duplicate in-panel subnav", async ({ page }) => {
    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    await expect(page.locator("#bookingsPanel .bookings-subtab-bar .subtabbar")).toHaveCount(0);
    await expect(page.locator("#bookingsSubnav")).toBeVisible();
  });

  test("bookings subtabs switch cleanly", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    await page.locator("#bookingsSubnav [data-bookings-subtab='orders']").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=orders$/);
    await expect(page.locator("#bookingsOrdersPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='templates']").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=templates$/);
    await expect(page.locator("#bookingsTemplatesPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='accounts']").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=accounts$/);
    await expect(page.locator("#bookingsAccountsPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='documents']").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=documents$/);
    await expect(page.locator("#bookingsDocumentsPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='transactions']").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=transactions$/);
    await expect(page.locator("#bookingsTransactionsPanel")).toHaveClass(/active/);

    expect(pageErrors, "page errors in bookings subtabs").toEqual([]);
  });

  test("bookings subtab survives reload and browser history", async ({ page }) => {
    await stubBookingsBootstrap(page);
    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    await page.locator("#bookingsSubnav [data-bookings-subtab='documents']").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=documents$/);
    await page.reload({ waitUntil: "networkidle" });
    await expect(page.locator("#bookingsDocumentsPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='orders']").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=orders$/);
    await page.goBack();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=documents$/);
    await expect(page.locator("#bookingsDocumentsPanel")).toHaveClass(/active/);
  });

  test("non-bookings routes do not inherit the bookings subtab query", async ({ page }) => {
    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0 }),
      });
    });
    await page.route("**/api/customers?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ total: 0, kpis: {}, items: [] }),
      });
    });
    await page.route("**/api/customers/locations?**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ summary: { orders_total: 0, unresolved_orders_count: 0, points_total: 0 }, points: [] }),
      });
    });
    await stubBookingsBootstrap(page);

    await page.goto("/orders", { waitUntil: "networkidle" });
    await expect(page).toHaveURL(/\/orders$/);

    await page.locator("#tabCustomersBtn").click();
    await expect(page).toHaveURL(/\/customers$/);

    await page.locator("#tabBookingsBtn").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=transactions$/);

    await page.locator("#tabOrdersBtn").click();
    await expect(page).toHaveURL(/\/orders$/);
  });

  test("bookings only keeps the active subtab data mounted", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let transactionRequests = 0;
    let monthlyInvoiceRequests = 0;
    let ledgerOrderRequests = 0;
    let orderRequests = 0;
    let documentRequests = 0;
    let accountRequests = 0;
    let templateRequests = 0;

    await page.route("**/api/bookings/transactions?**", async (route) => {
      transactionRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0, category_counts: {}, limit: 150, offset: 0 }),
      });
    });
    await page.route("**/api/bookings/monthly-invoices", async (route) => {
      monthlyInvoiceRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0 }),
      });
    });
    await page.route("**/api/bookings/ledger/orders", async (route) => {
      ledgerOrderRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0 }),
      });
    });
    await page.route("**/api/bookings/orders?**", async (route) => {
      orderRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0 }),
      });
    });
    await page.route("**/api/bookings/documents", async (route) => {
      documentRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0 }),
      });
    });
    await page.route("**/api/bookings/payment-accounts", async (route) => {
      accountRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0 }),
      });
    });
    await page.route("**/api/bookings/templates", async (route) => {
      templateRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0 }),
      });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    expect(transactionRequests).toBe(1);
    expect(monthlyInvoiceRequests).toBe(1);
    expect(ledgerOrderRequests).toBe(1);
    expect(orderRequests).toBe(0);
    expect(documentRequests).toBe(0);
    expect(accountRequests).toBe(1);
    expect(templateRequests).toBe(1);
    await expect(page.locator("#bookingsTransactionsReactRoot table")).toHaveCount(1);
    await expect(page.locator("#bookingsOrdersReactRoot table")).toHaveCount(0);
    await expect(page.locator("#bookingsDocumentsReactRoot table")).toHaveCount(0);

    await page.locator("#bookingsSubnav [data-bookings-subtab='documents']").click();
    await expect.poll(() => documentRequests).toBe(1);
    await expect.poll(() => accountRequests).toBe(2);
    await expect.poll(() => templateRequests).toBe(2);
    expect(orderRequests).toBe(0);
    expect(transactionRequests).toBe(1);
    expect(monthlyInvoiceRequests).toBe(1);
    expect(ledgerOrderRequests).toBe(1);
    await expect(page.locator("#bookingsTransactionsReactRoot table")).toHaveCount(0);
    await expect(page.locator("#bookingsDocumentsReactRoot table")).toHaveCount(1);

    await page.locator("#bookingsSubnav [data-bookings-subtab='orders']").click();
    await expect.poll(() => orderRequests).toBe(1);
    await expect.poll(() => accountRequests).toBe(3);
    await expect.poll(() => templateRequests).toBe(3);
    expect(documentRequests).toBe(1);
    expect(transactionRequests).toBe(1);
    expect(monthlyInvoiceRequests).toBe(1);
    expect(ledgerOrderRequests).toBe(1);
    await expect(page.locator("#bookingsDocumentsReactRoot table")).toHaveCount(0);
    await expect(page.locator("#bookingsOrdersReactRoot table")).toHaveCount(1);

    expect(pageErrors).toEqual([]);
  });

  test("orders purchase enter saves once and upload avoids full reload", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let ordersFetchCount = 0;
    let purchasePatchCount = 0;
    let invoiceUploadCount = 0;

    const ordersBody = {
      items: [
        {
          marketplace: "shopify",
          order_id: "order-1",
          external_order_id: "TEST-ORDER-1",
          order_date: "2026-02-03T10:00:00Z",
          customer: "Alice Example",
          article: "Alpha Product",
          line_items_count: 1,
          total_cents: 12990,
          fees_cents: 990,
          fee_source: "api",
          after_fees_cents: 12000,
          purchase_cost_cents: 4500,
          profit_cents: 7500,
          currency: "EUR",
          invoice: null,
          fulfillment_status: "fulfilled",
          payment_method: "Shopify Payments",
        },
      ],
      total: 1,
    };

    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      ordersFetchCount += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(ordersBody),
      });
    });

    await page.route("**/api/orders/shopify/order-1/purchase", async (route) => {
      purchasePatchCount += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ ok: true }),
      });
    });

    await page.route("**/api/orders/shopify/order-1/invoice", async (route) => {
      invoiceUploadCount += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          enrichment: {
            invoice_document_id: "doc-1",
            original_filename: "invoice.pdf",
            stored_filename: "invoice.pdf",
            mime_type: "application/pdf",
            uploaded_at: "2026-05-14T12:00:00Z",
          },
        }),
      });
    });

    await page.goto("/orders", { waitUntil: "networkidle" });
    await expect.poll(() => ordersFetchCount).toBe(1);

    const input = page.locator("#ordersBody tr[data-react-orders-row='true'] .purchase-input").first();
    await input.click();
    await input.fill("55.00");
    await input.press("Enter");
    await expect.poll(() => purchasePatchCount).toBe(1);

    const invoiceInput = page.locator("#ordersBody tr[data-react-orders-row='true'] .invoice-file-input").first();
    await invoiceInput.setInputFiles({
      name: "invoice.pdf",
      mimeType: "application/pdf",
      buffer: Buffer.from("%PDF-1.4 invoice"),
    });

    await expect.poll(() => invoiceUploadCount).toBe(1);
    await expect.poll(() => ordersFetchCount).toBe(1);
    await expect(page.locator("#ordersBody")).toContainText("invoice.pdf");

    expect(pageErrors).toEqual([]);
  });

  test("orders uses real pagination controls", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    const allItems = Array.from({ length: 220 }, (_, index) => ({
      marketplace: index % 2 === 0 ? "shopify" : "kaufland",
      order_id: `order-${index + 1}`,
      external_order_id: `TEST-ORDER-${index + 1}`,
      order_date: `2026-02-${String((index % 28) + 1).padStart(2, "0")}T10:00:00Z`,
      customer: index === 199 ? "Special Customer" : `Customer ${index + 1}`,
      article: `Article ${index + 1}`,
      line_items_count: 1,
      total_cents: 10000 + index,
      fees_cents: 500,
      fee_source: "api",
      after_fees_cents: 9500 + index,
      purchase_cost_cents: index % 2 === 0 ? 4000 : 0,
      profit_cents: 5500 + index,
      currency: "EUR",
      invoice: index % 3 === 0 ? { document_id: `doc-${index + 1}`, original_filename: `invoice-${index + 1}.pdf` } : null,
      fulfillment_status: index % 5 === 0 ? "refunded" : "fulfilled",
      raw_status: index % 5 === 0 ? "refunded" : "fulfilled",
      payment_method: index % 2 === 0 ? "Shopify Payments" : "PayPal",
    }));

    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      const url = new URL(route.request().url());
      const offset = Number(url.searchParams.get("offset") || "0");
      const limit = Number(url.searchParams.get("limit") || "150");
      const q = String(url.searchParams.get("q") || "").trim().toLowerCase();
      const payments = url.searchParams.getAll("payment");
      const hasPurchaseCost = url.searchParams.get("has_purchase_cost") === "true";
      const hasInvoice = url.searchParams.get("has_invoice") === "true";
      const hideCanceled = url.searchParams.get("hide_canceled") === "true";
      const status = String(url.searchParams.get("status") || "").trim().toLowerCase();

      let filtered = allItems.slice();
      if (q) {
        filtered = filtered.filter((item) => {
          return [item.customer, item.external_order_id, item.article].some((value) => String(value || "").toLowerCase().includes(q));
        });
      }
      if (payments.length) {
        filtered = filtered.filter((item) => payments.includes(String(item.payment_method || "")));
      }
      if (hasPurchaseCost) {
        filtered = filtered.filter((item) => Number(item.purchase_cost_cents || 0) > 0);
      }
      if (hasInvoice) {
        filtered = filtered.filter((item) => Boolean(item.invoice));
      }
      if (hideCanceled) {
        filtered = filtered.filter((item) => String(item.fulfillment_status || "").toLowerCase() !== "refunded");
      }
      if (status === "returns") {
        filtered = filtered.filter((item) => String(item.fulfillment_status || "").toLowerCase() === "refunded");
      }

      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: filtered.slice(offset, offset + limit),
          total: filtered.length,
          limit,
          offset,
        }),
      });
    });

    await page.goto("/orders", { waitUntil: "networkidle" });

    await expect(page.locator("#ordersMeta")).toContainText("1-150 / 176 Zeilen");
    await expect(page.locator("#ordersBody tr[data-react-orders-row='true']")).toHaveCount(150);
    await expect(page.locator("#ordersPanel")).toContainText("Seite 1 von 2");
    await expect(page.locator("#ordersPrevPageBtn")).toBeDisabled();
    await expect(page.locator("#ordersNextPageBtn")).toBeEnabled();

    await page.locator("#ordersNextPageBtn").click();
    await expect(page.locator("#ordersMeta")).toContainText("151-176 / 176 Zeilen");
    await expect(page.locator("#ordersBody tr[data-react-orders-row='true']")).toHaveCount(26);
    await expect(page.locator("#ordersBody")).toContainText("TEST-ORDER-220");
    await expect(page.locator("#ordersPrevPageBtn")).toBeEnabled();
    await expect(page.locator("#ordersNextPageBtn")).toBeDisabled();

    await page.locator("#ordersFilterBtn").click();
    await page.locator("#ordersPaymentChips button[data-value='PayPal']").click();
    await page.locator("#ordersFilterBtn").click();
    await expect(page.locator("#ordersMeta")).toContainText("1-88 / 88 Zeilen");
    await expect(page.locator("#ordersPrevPageBtn")).toHaveCount(0);
    await expect(page.locator("#ordersNextPageBtn")).toHaveCount(0);

    expect(pageErrors).toEqual([]);
  });

  test("bookings transactions use real pagination controls", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    const allTransactions = Array.from({ length: 220 }, (_, index) => ({
      id: `tx-${index + 1}`,
      date: `2026-02-${String((index % 28) + 1).padStart(2, "0")}T10:00:00Z`,
      type: index % 4 === 0 ? "SALE" : index % 4 === 1 ? "FEE" : index % 4 === 2 ? "SHIPPING" : "COGS",
      provider: index % 2 === 0 ? "shopify" : "paypal",
      direction: index % 3 === 1 ? "OUT" : "IN",
      amount_gross: 1000 + index,
      reference: `REF-${index + 1}`,
      notes: index === 199 ? "Special note" : `Note ${index + 1}`,
      payment_account_id: null,
      document_id: null,
    }));

    await page.route("**/api/bookings/transactions?**", async (route) => {
      const url = new URL(route.request().url());
      const limit = Number(url.searchParams.get("limit") || "150");
      const offset = Number(url.searchParams.get("offset") || "0");
      const category = String(url.searchParams.get("category") || "").trim().toLowerCase();

      let filtered = allTransactions.slice();
      if (category === "sale") {
        filtered = filtered.filter((item) => item.type === "SALE");
      }
      if (category === "fee") {
        filtered = filtered.filter((item) => item.type === "FEE" || item.type === "SHIPPING");
      }
      if (category === "cogs") {
        filtered = filtered.filter((item) => item.type === "COGS");
      }

      const categoryCounts = {
        sale: allTransactions.filter((item) => item.type === "SALE").length,
        fee: allTransactions.filter((item) => item.type === "FEE" || item.type === "SHIPPING").length,
        cogs: allTransactions.filter((item) => item.type === "COGS").length,
        invoice: 0,
        subscription: 0,
        refund: 0,
        other: 0,
      };

      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: filtered.slice(offset, offset + limit),
          total: filtered.length,
          category_counts: categoryCounts,
          limit,
          offset,
        }),
      });
    });
    await page.route("**/api/bookings/ledger/orders", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });
    await page.route("**/api/bookings/monthly-invoices", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });
    await page.route("**/api/bookings/payment-accounts", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });
    await page.route("**/api/bookings/templates", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });
    await page.route("**/api/bookings/orders?**", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });
    await page.route("**/api/bookings/documents", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    await expect(page.locator("#bookingsTransactionsMeta")).toContainText("1-150 / 220 Zeilen");
    await expect(page.locator("#bookingsTransactionsReactRoot tbody tr")).toHaveCount(150);
    await expect(page.locator("#bookingsPrevPageBtn")).toBeDisabled();
    await expect(page.locator("#bookingsNextPageBtn")).toBeEnabled();

    await page.locator("#bookingsNextPageBtn").click();
    await expect(page.locator("#bookingsTransactionsMeta")).toContainText("151-220 / 220 Zeilen");
    await expect(page.locator("#bookingsTransactionsReactRoot tbody tr")).toHaveCount(70);
    await expect(page.locator("#bookingsPrevPageBtn")).toBeEnabled();
    await expect(page.locator("#bookingsNextPageBtn")).toBeDisabled();
    await expect(page.locator("#bookingsTransactionsReactRoot")).toContainText("REF-220");

    await page.locator("#bookingTxLegend .tx-legend-item[data-filter-category='sale']").click();
    await expect(page.locator("#bookingsTransactionsMeta")).toContainText("1-55 / 55 Zeilen");
    await expect(page.locator("#bookingsPrevPageBtn")).toHaveCount(0);
    await expect(page.locator("#bookingsNextPageBtn")).toHaveCount(0);

    expect(pageErrors).toEqual([]);
  });

  test("bookings orders use real pagination controls", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    const allOrders = Array.from({ length: 220 }, (_, index) => ({
      marketplace: index % 2 === 0 ? "shopify" : "ebay",
      order_id: `order-${index + 1}`,
      external_order_id: `BOOK-${String(index + 1).padStart(3, "0")}`,
      order_date: `2026-02-${String((index % 28) + 1).padStart(2, "0")}T10:00:00Z`,
      customer: `Customer ${index + 1}`,
      revenue_cents: 15000 + index,
      bookkeeping_income_cents: 15000 + index,
      total_costs_cents: 7000 + index,
      fees_cents: 1000,
      purchase_cents: 5500,
      bookkeeping_expense_cents: 500 + (index % 3),
      profit_cents: 8000,
      bookkeeping_matched_via: index % 2 === 0 ? "order_id" : "external_order_id",
      documents_count: index % 4,
    }));

    await stubBookingsBootstrap(page, { orders: allOrders });

    await page.goto("/bookings/full?subtab=orders", { waitUntil: "networkidle" });

    await expect(page.locator("#bookingsOrdersMeta")).toContainText("1-150 / 220 Zeilen");
    await expect(page.locator("#bookingsOrdersReactRoot tr[data-order-id]")).toHaveCount(150);
    await expect(page.locator("#bookingsOrdersPrevPageBtn")).toBeDisabled();
    await expect(page.locator("#bookingsOrdersNextPageBtn")).toBeEnabled();
    await expect(page.locator("#bookingsOrdersReactRoot")).toContainText("BOOK-150");
    await expect(page.locator("#bookingsOrdersReactRoot")).not.toContainText("BOOK-220");

    await page.locator("#bookingsOrdersNextPageBtn").click();
    await expect(page.locator("#bookingsOrdersMeta")).toContainText("151-220 / 220 Zeilen");
    await expect(page.locator("#bookingsOrdersReactRoot tr[data-order-id]")).toHaveCount(70);
    await expect(page.locator("#bookingsOrdersPrevPageBtn")).toBeEnabled();
    await expect(page.locator("#bookingsOrdersNextPageBtn")).toBeDisabled();
    await expect(page.locator("#bookingsOrdersReactRoot")).toContainText("BOOK-151");
    await expect(page.locator("#bookingsOrdersReactRoot")).toContainText("BOOK-220");
    await expect(page.locator("#bookingsOrdersReactRoot")).not.toContainText("BOOK-001");

    expect(pageErrors).toEqual([]);
  });

  test("orders status filter keeps a single active selection", async ({ page }) => {
    const pageErrors: string[] = [];
    const requestedStatuses: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.route(/\/api\/orders(?:\?.*)?$/, async (route) => {
      const url = new URL(route.request().url());
      requestedStatuses.push(String(url.searchParams.get("status") || ""));
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: [], total: 0, limit: 150, offset: 0 }),
      });
    });

    await page.goto("/orders", { waitUntil: "networkidle" });

    const fulfilledChip = page.locator("#ordersStatusChips button[data-value='fulfilled']");
    const paidChip = page.locator("#ordersStatusChips button[data-value='paid']");
    const returnsChip = page.getByRole("button", { name: "Retouren / Cancel" });

    await page.locator("#ordersFilterBtn").click();
    await fulfilledChip.click();
    await expect(fulfilledChip).toHaveClass(/active/);
    await expect.poll(() => requestedStatuses[requestedStatuses.length - 1]).toBe("fulfilled");

    await paidChip.click();
    await expect(fulfilledChip).not.toHaveClass(/active/);
    await expect(paidChip).toHaveClass(/active/);
    await expect.poll(() => requestedStatuses[requestedStatuses.length - 1]).toBe("paid");

    await returnsChip.click();
    await expect(paidChip).not.toHaveClass(/active/);
    await expect(returnsChip).toHaveClass(/active/);
    await expect.poll(() => requestedStatuses[requestedStatuses.length - 1]).toBe("returns");

    expect(pageErrors).toEqual([]);
  });

  test("heavy routes unmount when switching views", async ({ page }) => {
    await page.goto("/orders", { waitUntil: "networkidle" });
    await expect(page.locator("#ordersPanel")).toBeVisible();

    await page.locator("#tabCustomersBtn").click();
    await expect(page).toHaveURL(/\/customers$/);
    await expect(page.locator("#customersPanel")).toBeVisible();
    await expect(page.locator("#ordersPanel")).toHaveCount(0);

    await page.locator("#tabBookingsBtn").click();
    await expect(page).toHaveURL(/\/bookings\/full\?subtab=transactions$/);
    await expect(page.locator("#bookingsPanel")).toBeVisible();
    await expect(page.locator("#customersPanel")).toHaveCount(0);
  });

  test("bookings class controls drive new button and tools", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    const newButton = page.locator("#bookingsNewBtn");
    const transactionTools = page.locator("#bookingsTransactionTools");
    const monthlyInvoiceSection = page.locator("#sammelrechnungSection");
    const bookingClassBar = page.locator("#bookingClassBar");

    await expect(bookingClassBar).toBeVisible();

    await expect(page.locator("#bookingClassAutoBtn")).toHaveClass(/active/);
    await expect(newButton).toBeHidden();
    await expect(transactionTools).not.toHaveClass(/open/);
    await expect(monthlyInvoiceSection).toBeHidden();

    await page.locator("#bookingClassSingleBtn").click();
    await expect(page.locator("#bookingClassSingleBtn")).toHaveClass(/active/);
    await expect(newButton).toBeVisible();
    await expect(newButton).toHaveAttribute("data-target", "bookingsTransactionTools");
    await expect(newButton).toContainText("Neue Transaktion");
    await expect(transactionTools).not.toHaveClass(/open/);
    await expect(monthlyInvoiceSection).toBeHidden();
    await newButton.click();
    await expect(newButton).toHaveAttribute("aria-expanded", "true");
    await expect(transactionTools).toHaveClass(/open/);
    await expect(transactionTools).toBeVisible();

    await page.locator("#bookingClassMonthlyBtn").click();
    await expect(page.locator("#bookingClassMonthlyBtn")).toHaveClass(/active/);
    await expect(newButton).toBeVisible();
    await expect(newButton).toHaveAttribute("data-target", "sammelrechnungTools");
    await expect(newButton).toContainText("Neue Sammelrechnung");
    await expect(monthlyInvoiceSection).toBeVisible();
    await expect(transactionTools).toBeHidden();
    await expect(page.locator("#sammelrechnungTools")).not.toHaveClass(/open/);
    await newButton.click();
    await expect(newButton).toHaveAttribute("aria-expanded", "true");
    await expect(page.locator("#sammelrechnungTools")).toHaveClass(/open/);
    await expect(page.locator("#sammelrechnungTools")).toBeVisible();

    await page.locator("#bookingsSubnav [data-bookings-subtab='templates']").click();
    await expect(page.locator("#bookingsTemplatesPanel")).toHaveClass(/active/);
    await expect(newButton).toBeVisible();
    await expect(newButton).toHaveAttribute("data-target", "bookingsTemplateTools");
    await expect(newButton).toContainText("Neues Template");
    await expect(bookingClassBar).toBeHidden();
    await expect(page.locator("#bookingsTemplateTools")).not.toHaveClass(/open/);
    await newButton.click();
    await expect(newButton).toHaveAttribute("aria-expanded", "true");
    await expect(page.locator("#bookingsTemplateTools")).toHaveClass(/open/);
    await expect(page.locator("#bookingsTemplateTools")).toBeVisible();

    await page.locator("#bookingsSubnav [data-bookings-subtab='orders']").click();
    await expect(page.locator("#bookingsOrdersPanel")).toHaveClass(/active/);
    await expect(newButton).toBeHidden();

    await page.locator("#bookingsSubnav [data-bookings-subtab='transactions']").click();
    await expect(page.locator("#bookingsTransactionsPanel")).toHaveClass(/active/);
    await expect(bookingClassBar).toBeVisible();
    await expect(page.locator("#bookingClassMonthlyBtn")).toHaveClass(/active/);
    await expect(newButton).toBeVisible();
    await expect(newButton).toHaveAttribute("data-target", "sammelrechnungTools");

    expect(pageErrors, "page errors in bookings class controls").toEqual([]);
  });

  test("bookings orders detail button opens order modal from React table", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await stubBookingsBootstrap(page, {
      orders: [
        {
          marketplace: "shopify",
          order_id: "order-1",
          external_order_id: "TEST-ORDER-1",
          order_date: "2026-02-03T10:00:00Z",
          customer: "Alice Example",
          revenue_cents: 12990,
          bookkeeping_income_cents: 12990,
          total_costs_cents: 5490,
          fees_cents: 990,
          purchase_cents: 4500,
          bookkeeping_expense_cents: 0,
          profit_cents: 7500,
          bookkeeping_matched_via: "order_id",
          documents_count: 1,
        },
      ],
    });
    await page.route(/\/api\/orders\/[^/]+\/[^/?#]+$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          summary: {
            marketplace: "shopify",
            order_id: "order-1",
            external_order_id: "TEST-ORDER-1",
            order_date: "2026-02-03T10:00:00Z",
            customer: "Alice Example",
            payment_method: "card",
            fulfillment_status: "fulfilled",
            total_cents: 12990,
            fees_cents: 990,
            fee_source: "api",
            after_fees_cents: 12000,
            purchase_cost_cents: 4500,
            profit_cents: 7500,
            currency: "EUR",
            invoice: null,
          },
          order: {
            customer_email: "alice@example.com",
            currency: "EUR",
          },
          customer: {
            name: "Alice Example",
            email: "alice@example.com",
          },
          shipping_address: {
            name: "Alice Example",
            city: "Berlin",
            country: "DE",
          },
          billing_address: {
            name: "Alice Example",
            city: "Berlin",
            country: "DE",
          },
          line_items: [],
          transactions: [],
          fulfillments: [],
          refunds: [],
          bookkeeping_breakdown: {
            db_available: false,
          },
        }),
      });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });
    await page.locator("#bookingsSubnav [data-bookings-subtab='orders']").click();

    const detailsButton = page.locator("#bookingsOrdersReactRoot [data-action='details']").first();
    await expect(detailsButton).toBeVisible();
    await detailsButton.click();

    await expect(page.locator("#detailsModal")).toHaveAttribute("data-react-owned", "true");
    await expect(page.locator("#detailsModal")).toHaveClass(/active/);
    await expect(page.locator("#detailsTitle")).toContainText("Details");
    await expect(page.locator("#detailsContent")).toContainText("TEST-ORDER-1");
    await page.locator("#closeModalBtn").click();
    await expect(page.locator("#detailsModal")).not.toHaveClass(/active/);
    expect(pageErrors, "page errors in bookings order details flow").toEqual([]);
  });

  test("bookings transaction row opens detail modal from React table", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await stubBookingsBootstrap(page, {
      transactions: [
        {
          id: "tx-1",
          date: "2026-02-03T12:00:00Z",
          type: "SALE",
          provider: "shopify",
          direction: "IN",
          amount_gross: 12345,
          reference: "TX-REF-1",
          notes: "created via fixture",
          payment_account_id: "acc-1",
          document_id: "doc-1",
          document: {
            original_filename: "invoice.pdf",
            mime_type: "application/pdf",
          },
        },
      ],
      accounts: [
        {
          id: "acc-1",
          name: "Bank DE",
          provider: "bank",
          is_active: true,
        },
      ],
      templates: [
        {
          id: "tpl-1",
          name: "Template A",
          schedule: "monthly",
          default_amount_gross: 12345,
        },
      ],
    });
    await page.route(/\/api\/bookings\/transactions\/tx-1$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          transaction: {
            id: "tx-1",
            date: "2026-02-03T12:00:00Z",
            type: "SALE",
            direction: "IN",
            amount_gross: 12345,
            provider: "shopify",
            status: "booked",
            currency: "EUR",
            reference: "TX-REF-1",
            counterparty_name: "Alice Example",
            category: "order",
            payment_account_id: "acc-1",
            payment_account: {
              name: "Bank DE",
            },
            template_id: "tpl-1",
            template: {
              name: "Template A",
            },
            document_id: "doc-1",
            document: {
              original_filename: "invoice.pdf",
              mime_type: "application/pdf",
            },
            order: {
              provider: "shopify",
              external_order_id: "TEST-ORDER-1",
            },
            source: "manual",
            source_key: "tx-1",
            notes: "created via fixture",
          },
        }),
      });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    const transactionRow = page.locator("#bookingsTransactionsReactRoot tr[data-booking-id]").first();
    await expect(transactionRow).toBeVisible();
    await transactionRow.locator("td").first().click();

    await expect(page.locator("#detailsModal")).toHaveAttribute("data-react-owned", "true");
    await expect(page.locator("#detailsModal")).toHaveClass(/active/);
    await expect(page.locator("#detailsTitle")).toContainText("Transaktion");
    await expect(page.locator("#detailsContent")).toContainText("TX-REF-1");
    expect(pageErrors, "page errors in bookings transaction details flow").toEqual([]);
  });

  test("bookings monthly invoice row opens detail modal from React table", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await stubBookingsBootstrap(page, {
      monthlyInvoices: [
        {
          id: "inv-1",
          provider: "paypal",
          period_from: "2026-02-01T00:00:00Z",
          period_to: "2026-02-29T23:59:59Z",
          invoice_amount_cents: 5000,
          calculated_sum_cents: 5000,
          difference_cents: 0,
          status: "matched",
          notes: "February invoice",
          document_id: "doc-2",
          document: {
            original_filename: "paypal-feb.pdf",
            mime_type: "application/pdf",
          },
        },
      ],
    });
    await page.route(/\/api\/bookings\/monthly-invoices\/inv-1$/, async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          invoice: {
            id: "inv-1",
            provider: "paypal",
            period_from: "2026-02-01T00:00:00Z",
            period_to: "2026-02-29T23:59:59Z",
            invoice_amount_cents: 5000,
            calculated_sum_cents: 5000,
            difference_cents: 0,
            status: "matched",
            notes: "February invoice",
            currency: "EUR",
            created_at: "2026-03-01T10:00:00Z",
            updated_at: "2026-03-01T10:30:00Z",
            document_id: "doc-2",
            document: {
              original_filename: "paypal-feb.pdf",
              mime_type: "application/pdf",
            },
            transactions: [
              {
                id: "tx-1",
                type: "SALE",
                date: "2026-02-03T12:00:00Z",
                amount_gross: 5000,
                counterparty_name: "Alice Example",
                reference: "TX-REF-1",
                notes: "linked",
              },
            ],
          },
        }),
      });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });
    await page.locator("#bookingClassMonthlyBtn").click();

    const invoiceRow = page.locator("#bookingsMonthlyInvoicesReactRoot tr[data-invoice-id]").first();
    await expect(invoiceRow).toBeVisible();
    await invoiceRow.locator("td").first().click();

    await expect(page.locator("#detailsModal")).toHaveAttribute("data-react-owned", "true");
    await expect(page.locator("#detailsModal")).toHaveClass(/active/);
    await expect(page.locator("#detailsTitle")).toContainText("Sammelrechnung");
    await expect(page.locator("#detailsContent")).toContainText("paypal-feb.pdf");
    expect(pageErrors, "page errors in bookings invoice details flow").toEqual([]);
  });

  test("bookings create transaction posts payload and updates React table", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let created = false;
    let transactionFetchCount = 0;
    let createPayload: Record<string, unknown> | null = null;
    let uploadSeen = false;

    await page.route("**/api/bookings/transactions", async (route) => {
      const request = route.request();
      if (request.method() === "POST") {
        createPayload = JSON.parse(request.postData() || "{}");
        created = true;
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ invoice: null, transaction: { id: "tx-new" } }),
        });
        return;
      }

      await route.continue();
    });

    await page.route("**/api/bookings/transactions?**", async (route) => {
      transactionFetchCount += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: created
            ? [
                {
                  id: "tx-new",
                  date: "2026-02-03T00:00:00Z",
                  type: "SHIPPING",
                  provider: "shopify",
                  direction: "IN",
                  amount_gross: 12345,
                  reference: "AUTO-1",
                  notes: "created via test",
                  payment_account_id: null,
                  document_id: null,
                },
              ]
            : [],
          total: created ? 1 : 0,
          category_counts: created ? { fee: 1 } : {},
          limit: 150,
          offset: 0,
        }),
      });
    });

    await page.route("**/api/bookings/documents/upload", async (route) => {
      uploadSeen = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ document: { id: "doc-new" } }),
      });
    });

    await page.route("**/api/bookings/monthly-invoices", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/payment-accounts", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/templates", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/orders?**", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/ledger/orders", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/documents", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });
    await page.locator("#bookingClassSingleBtn").click();
    await page.locator("#bookingsNewBtn").click();

    await page.fill("#createBookingDate", "2026-02-03");
    await page.selectOption("#createBookingType", "SHIPPING");
    await page.fill("#createBookingAmount", "123,45");
    await page.fill("#createBookingProvider", "shopify");
    await page.fill("#createBookingCounterparty", "DHL");
    await page.fill("#createBookingReference", "AUTO-1");
    await page.fill("#createBookingCategory", "manual-test");
    await page.fill("#createBookingNotes", "created via test");
    await page.setInputFiles("#createBookingDocumentFile", {
      name: "invoice.pdf",
      mimeType: "application/pdf",
      buffer: Buffer.from("%PDF-1.4 create booking test"),
    });
    await page.click("#createBookingTxBtn");

    await expect.poll(() => created).toBe(true);
    await expect.poll(() => uploadSeen).toBe(true);
    expect(createPayload).toMatchObject({
      type: "SHIPPING",
      direction: "IN",
      provider: "shopify",
      counterparty_name: "DHL",
      category: "manual-test",
      reference: "AUTO-1",
      notes: "created via test",
      booking_class: "single",
    });
    await expect.poll(() => transactionFetchCount).toBeGreaterThan(1);
    await expect(page.locator("#bookingsTransactionsReactRoot")).toContainText("AUTO-1");
    expect(pageErrors, "page errors in bookings create transaction flow").toEqual([]);
  });

  test("bookings template create, account autosave, and document upload use expected APIs", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let templatesCreated = false;
    let accountsCreated = false;
    let documentsUploaded = false;
    let templateCreatePayload: Record<string, unknown> | null = null;
    let accountPatchPayload: Record<string, unknown> | null = null;
    let uploadSeen = false;

    await page.route("**/api/bookings/templates", async (route) => {
      const request = route.request();
      if (request.method() === "POST") {
        templateCreatePayload = JSON.parse(request.postData() || "{}");
        templatesCreated = true;
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ id: "tpl-1" }),
        });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: templatesCreated
            ? [
                {
                  id: "tpl-1",
                  name: "Shopify Abo",
                  type: "SUBSCRIPTION",
                  direction: "OUT",
                  counterparty_name: "OpenAI",
                  start_date: "2026-01-01",
                  default_amount_gross: 1999,
                  schedule: "monthly",
                  payment_account_id: "acc-1",
                  active: true,
                },
              ]
            : [],
          total: templatesCreated ? 1 : 0,
        }),
      });
    });

    await page.route("**/api/bookings/payment-accounts", async (route) => {
      const request = route.request();
      if (request.method() === "POST") {
        accountsCreated = true;
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ id: "acc-1" }),
        });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: accountsCreated
            ? [{ id: "acc-1", name: "Bank DE", provider: "bank", is_active: true }]
            : [],
          total: accountsCreated ? 1 : 0,
        }),
      });
    });

    await page.route("**/api/bookings/payment-accounts/acc-1", async (route) => {
      accountPatchPayload = JSON.parse(route.request().postData() || "{}");
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true }) });
    });

    await page.route("**/api/bookings/documents/upload", async (route) => {
      uploadSeen = true;
      documentsUploaded = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ document: { id: "doc-1" } }),
      });
    });

    await page.route("**/api/bookings/documents", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: documentsUploaded
            ? [{ id: "doc-1", uploaded_at: "2026-02-04T10:00:00Z", original_filename: "invoice.pdf", stored_filename: "doc-1.pdf", mime_type: "application/pdf", _count: { transactions: 0 } }]
            : [],
          total: documentsUploaded ? 1 : 0,
        }),
      });
    });

    await page.route("**/api/bookings/transactions?**", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0, category_counts: {}, limit: 150, offset: 0 }) });
    });

    await page.route("**/api/bookings/orders?**", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/ledger/orders", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });

    await page.locator("#bookingsSubnav [data-bookings-subtab='accounts']").click();
    await page.locator("#bookingsNewBtn").click();
    await page.fill("#accountNameInput", "Bank DE");
    await page.fill("#accountProviderInput", "bank");
    await page.click("#createAccountBtn");
    const createdAccountNameInput = page.locator("#bookingsAccountsReactRoot tr[data-account-id='acc-1'] [data-field='name']");
    await expect(createdAccountNameInput).toHaveValue("Bank DE");

    const accountNameInput = createdAccountNameInput;
    await accountNameInput.fill("Bank DE Updated");
    await accountNameInput.blur();
    await expect.poll(() => accountPatchPayload).not.toBeNull();
    expect(accountPatchPayload).toMatchObject({ name: "Bank DE Updated", provider: "bank", is_active: true });

    await page.locator("#bookingsSubnav [data-bookings-subtab='templates']").click();
    await page.locator("#bookingsNewBtn").click();
    await page.fill("#templateNameInput", "Shopify Abo");
    await page.fill("#templateAmountInput", "19,99");
    await page.fill("#templateProviderInput", "shopify");
    await page.fill("#templateCounterpartyInput", "OpenAI");
    await page.fill("#templateStartDateInput", "2026-01-01");
    await page.click("#createTemplateBtn");
    await expect.poll(() => templatesCreated).toBe(true);
    expect(templateCreatePayload).toMatchObject({
      name: "Shopify Abo",
      provider: "shopify",
      counterparty_name: "OpenAI",
      schedule: "monthly",
      active: true,
    });
    await expect(page.locator("#bookingsTemplatesReactRoot tr[data-template-id='tpl-1'] [data-field='name']")).toHaveValue("Shopify Abo");

    await page.locator("#bookingsSubnav [data-bookings-subtab='documents']").click();
    await page.locator("#bookingsNewBtn").click();
    await page.setInputFiles("#bookingDocumentFileInput", {
      name: "invoice.pdf",
      mimeType: "application/pdf",
      buffer: Buffer.from("%PDF-1.4 test"),
    });
    await page.fill("#bookingDocumentNotesInput", "Lieferantenrechnung");
    await page.click("#uploadBookingDocumentBtn");
    await expect.poll(() => uploadSeen).toBe(true);
    await expect(page.locator("#bookingsDocumentsReactRoot")).toContainText("invoice.pdf");

    expect(pageErrors, "page errors in bookings mutation flows").toEqual([]);
  });

  test("bookings monthly invoice upload and delete use expected APIs", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    let invoiceDocumentLinked = false;
    let invoiceDeleted = false;
    let invoiceUploadSeen = false;
    let invoiceDeleteSeen = false;
    let invoicePatchPayload: Record<string, unknown> | null = null;

    const buildMonthlyInvoices = () => {
      const items: Array<Record<string, unknown>> = [
        {
          id: "inv-upload",
          provider: "paypal",
          period_from: "2026-02-01T00:00:00Z",
          period_to: "2026-02-28T23:59:59Z",
          invoice_amount_cents: 5000,
          calculated_sum_cents: 5000,
          difference_cents: 0,
          status: "draft",
          notes: "Needs document",
          document_id: invoiceDocumentLinked ? "doc-upload" : null,
          document: invoiceDocumentLinked
            ? {
                original_filename: "paypal-upload.pdf",
                mime_type: "application/pdf",
              }
            : null,
        },
      ];

      if (!invoiceDeleted) {
        items.push({
          id: "inv-delete",
          provider: "shopify_payments",
          period_from: "2026-01-01T00:00:00Z",
          period_to: "2026-01-31T23:59:59Z",
          invoice_amount_cents: 3200,
          calculated_sum_cents: 3200,
          difference_cents: 0,
          status: "matched",
          notes: "Delete me",
          document_id: null,
          document: null,
        });
      }

      return items;
    };

    await page.route("**/api/bookings/transactions?**", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0, category_counts: {}, limit: 150, offset: 0 }) });
    });

    await page.route("**/api/bookings/orders?**", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/ledger/orders", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/payment-accounts", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/templates", async (route) => {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
    });

    await page.route("**/api/bookings/documents", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: invoiceDocumentLinked
            ? [{ id: "doc-upload", uploaded_at: "2026-02-04T10:00:00Z", original_filename: "paypal-upload.pdf", stored_filename: "doc-upload.pdf", mime_type: "application/pdf", _count: { transactions: 0 } }]
            : [],
          total: invoiceDocumentLinked ? 1 : 0,
        }),
      });
    });

    await page.route("**/api/bookings/monthly-invoices", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items: buildMonthlyInvoices(), total: buildMonthlyInvoices().length }),
      });
    });

    await page.route("**/api/bookings/documents/upload", async (route) => {
      invoiceUploadSeen = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ document: { id: "doc-upload" } }),
      });
    });

    await page.route("**/api/bookings/monthly-invoices/inv-upload", async (route) => {
      invoicePatchPayload = JSON.parse(route.request().postData() || "{}");
      invoiceDocumentLinked = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          invoice: buildMonthlyInvoices()[0],
        }),
      });
    });

    await page.route("**/api/bookings/monthly-invoices/inv-delete", async (route) => {
      invoiceDeleteSeen = true;
      invoiceDeleted = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ ok: true, deleted: true }),
      });
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });
    await page.locator("#bookingClassMonthlyBtn").click();
    await expect(page.locator("#bookingsPanel")).toHaveAttribute("data-react-bookings-mounted", "true");
    await expect(page.locator("#bookingMonthlyInvoiceUploadInput")).toBeAttached();

    const uploadRow = page.locator("#bookingsMonthlyInvoicesReactRoot tr[data-invoice-id='inv-upload']");
    await expect(uploadRow).toBeVisible();

    await uploadRow.locator("button[data-action='upload-invoice-doc']").click();
    await page.locator("#bookingMonthlyInvoiceUploadInput").setInputFiles({
      name: "paypal-upload.pdf",
      mimeType: "application/pdf",
      buffer: Buffer.from("%PDF-1.4 monthly invoice"),
    });

    await expect.poll(() => invoiceUploadSeen).toBe(true);
    await expect.poll(() => invoicePatchPayload).not.toBeNull();
    expect(invoicePatchPayload).toMatchObject({ document_id: "doc-upload" });
    await expect(uploadRow.locator("button[data-action='preview-document']")).toBeVisible();
    await expect(uploadRow.locator("button[data-action='upload-invoice-doc']")).toHaveCount(0);

    const deleteRow = page.locator("#bookingsMonthlyInvoicesReactRoot tr[data-invoice-id='inv-delete']");
    await expect(deleteRow).toBeVisible();
    page.once("dialog", (dialog) => {
      void dialog.accept();
    });
    await deleteRow.locator("button[data-action='delete-invoice']").click();

    await expect.poll(() => invoiceDeleteSeen).toBe(true);
    await expect(page.locator("#bookingsMonthlyInvoicesReactRoot tr[data-invoice-id='inv-delete']")).toHaveCount(0);

    expect(pageErrors, "page errors in bookings monthly invoice mutation flow").toEqual([]);
  });

  test("bookings document preview opens from React table", async ({ page }) => {
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => {
      pageErrors.push(error.message);
    });

    await stubBookingsBootstrap(page, {
      documents: [
        {
          id: "doc-preview",
          uploaded_at: "2026-02-04T10:00:00Z",
          original_filename: "invoice.pdf",
          stored_filename: "invoice.pdf",
          mime_type: "application/pdf",
          _count: { transactions: 1 },
        },
      ],
    });

    await page.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });
    await page.locator("#bookingsSubnav [data-bookings-subtab='documents']").click();

    await page.locator("#bookingsDocumentsReactRoot button[data-action='preview-document']").click();
    await expect(page.locator("#previewModal")).toHaveAttribute("data-react-owned", "true");
    await expect(page.locator("#previewModal")).toHaveClass(/active/);
    await expect(page.locator("#previewTitle")).toContainText("invoice.pdf");
    await page.locator("#previewZoomIn").click();
    await expect(page.locator("#previewZoomLevel")).toHaveText("125%");
    await page.locator("#closePreviewBtn").click();
    await expect(page.locator("#previewModal")).not.toHaveClass(/active/);
    expect(pageErrors, "page errors in bookings preview flow").toEqual([]);
  });

  test("desktop and mobile layouts keep migrated route chrome usable", async ({ browser }) => {
    const desktopPage = await browser.newPage({ viewport: { width: 1440, height: 1024 } });
    const mobilePage = await browser.newPage({ viewport: { width: 390, height: 844 } });

    await desktopPage.goto("/google-ads", { waitUntil: "networkidle" });
    const desktopGoogleAdsLayout = await desktopPage.evaluate(() => {
      const row = document.querySelector(".google-ads-import-row");
      const styles = row ? getComputedStyle(row) : null;
      return {
        flexDirection: styles?.flexDirection || null,
        alignItems: styles?.alignItems || null,
      };
    });
    expect(desktopGoogleAdsLayout.flexDirection).toBe("row");
    expect(desktopGoogleAdsLayout.alignItems).toBe("flex-end");

    await desktopPage.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });
    const desktopBookingsLayout = await desktopPage.evaluate(() => {
      const bar = document.querySelector("#bookingsPanel .bookings-subtab-bar");
      return bar ? getComputedStyle(bar).justifyContent : null;
    });
    expect(desktopBookingsLayout).toBe("flex-end");

    await mobilePage.goto("/google-ads", { waitUntil: "networkidle" });
    const mobileGoogleAdsLayout = await mobilePage.evaluate(() => {
      const row = document.querySelector(".google-ads-import-row");
      const actions = document.querySelector(".google-ads-action-field");
      return {
        rowDirection: row ? getComputedStyle(row).flexDirection : null,
        actionDirection: actions ? getComputedStyle(actions).flexDirection : null,
      };
    });
    expect(mobileGoogleAdsLayout.rowDirection).toBe("column");
    expect(mobileGoogleAdsLayout.actionDirection).toBe("column");

    await mobilePage.goto("/bookings/full?subtab=transactions", { waitUntil: "networkidle" });
    await expect(mobilePage.locator("#bookingsSubnav")).toBeVisible();
    await expect(mobilePage.locator("#bookingsNewBtn")).toBeHidden();
    await mobilePage.locator("#bookingClassSingleBtn").click();
    await expect(mobilePage.locator("#bookingsNewBtn")).toBeVisible();

    await mobilePage.goto("/customers", { waitUntil: "networkidle" });
    const mobileCustomerGeoHeight = await mobilePage.evaluate(() => {
      const stage = document.getElementById("customerGeoMapView")?.parentElement;
      return stage ? getComputedStyle(stage).minHeight : null;
    });
    expect(mobileCustomerGeoHeight).toBe("360px");

    await desktopPage.close();
    await mobilePage.close();
  });
});
