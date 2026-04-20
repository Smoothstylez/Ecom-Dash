import { createDefaultOrdersClientFilters, type OrderSummary } from "@/features/orders/types";
import { filterOrders, getOrdersClientFilterCount, isCanceledOrder } from "@/features/orders/filter-orders";

const baseOrder: OrderSummary = {
  marketplace: "shopify",
  order_id: "1",
  external_order_id: "#1001",
  fulfillment_status: "fulfilled",
  payment_method: "PayPal",
  purchase_cost_cents: 1200,
  invoice: { document_id: "inv-1" },
};

describe("orders filter logic", () => {
  it("treats refund-like states as canceled orders", () => {
    expect(isCanceledOrder({ ...baseOrder, financial_status: "partially_refunded" })).toBe(true);
    expect(isCanceledOrder(baseOrder)).toBe(false);
  });

  it("hides canceled orders by default", () => {
    const filters = createDefaultOrdersClientFilters();
    const rows = [baseOrder, { ...baseOrder, order_id: "2", financial_status: "refunded" }];

    expect(filterOrders(rows, filters)).toHaveLength(1);
  });

  it("supports purchase and invoice filters", () => {
    const filters = createDefaultOrdersClientFilters();
    filters.hasPurchaseCost = true;
    filters.hasInvoice = true;

    const rows = [
      baseOrder,
      { ...baseOrder, order_id: "2", purchase_cost_cents: 0 },
      { ...baseOrder, order_id: "3", invoice: null },
    ];

    expect(filterOrders(rows, filters)).toEqual([baseOrder]);
  });

  it("counts active client filters like the legacy dashboard", () => {
    const filters = createDefaultOrdersClientFilters();
    filters.orderStatus = ["fulfilled"];
    filters.orderPayment = ["PayPal"];
    filters.hideCanceled = false;
    filters.noInvoice = true;

    expect(getOrdersClientFilterCount(filters)).toBe(4);
  });
});
