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
  { path: "/customers", activeNavId: "tabCustomersBtn", activePanelId: "customersPanel" },
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
      body: JSON.stringify({ items: transactions, total: transactions.length }),
    });
  });

  await page.route("**/api/bookings/orders?**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ items: orders, total: orders.length }),
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

  test("redirects old app-preview links", async ({ request }) => {
    const response = await request.get("/app-preview/analytics", { maxRedirects: 0 });
    expect(response.status()).toBe(307);
    expect(response.headers()["location"]).toBe("/analytics");
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
    await expect(page.locator("#bookingsOrdersPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='templates']").click();
    await expect(page.locator("#bookingsTemplatesPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='accounts']").click();
    await expect(page.locator("#bookingsAccountsPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='documents']").click();
    await expect(page.locator("#bookingsDocumentsPanel")).toHaveClass(/active/);

    await page.locator("#bookingsSubnav [data-bookings-subtab='transactions']").click();
    await expect(page.locator("#bookingsTransactionsPanel")).toHaveClass(/active/);

    expect(pageErrors, "page errors in bookings subtabs").toEqual([]);
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
    let createPayload: Record<string, unknown> | null = null;

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
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: created
            ? [
                {
                  id: "tx-new",
                  date: "2026-02-03T00:00:00Z",
                  type: "SALE",
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
        }),
      });
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
    await page.fill("#createBookingAmount", "123,45");
    await page.fill("#createBookingProvider", "shopify");
    await page.fill("#createBookingReference", "AUTO-1");
    await page.fill("#createBookingNotes", "created via test");
    await page.click("#createBookingTxBtn");

    await expect.poll(() => created).toBe(true);
    expect(createPayload).toMatchObject({
      type: "SALE",
      direction: "IN",
      provider: "shopify",
      reference: "AUTO-1",
      notes: "created via test",
      booking_class: "single",
    });
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
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
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
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ items: [], total: 0 }) });
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

    await page.route("**/api/bookings/documents", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              id: "doc-preview",
              uploaded_at: "2026-02-04T10:00:00Z",
              original_filename: "invoice.pdf",
              stored_filename: "invoice.pdf",
              mime_type: "application/pdf",
              _count: { transactions: 1 },
            },
          ],
          total: 1,
        }),
      });
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
    expect(desktopGoogleAdsLayout.alignItems).toBe("end");

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
