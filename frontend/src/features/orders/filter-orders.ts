import type { OrderSummary, OrdersClientFilters } from "@/features/orders/types";

const RETURN_KEYWORDS = [
  "cancel",
  "cancelled",
  "canceled",
  "void",
  "return",
  "returned",
  "refund",
  "refunded",
  "partially_refunded",
  "rma",
  "revoked",
  "returning",
] as const;

export function isReturnLikeStatus(value?: string) {
  const token = String(value ?? "").trim().toLowerCase();
  if (!token) {
    return false;
  }

  return RETURN_KEYWORDS.some((keyword) => token.includes(keyword));
}

export function isCanceledOrder(order: OrderSummary) {
  return [order.fulfillment_status, order.financial_status, order.raw_status].some((value) => isReturnLikeStatus(value));
}

export function filterOrders(orders: OrderSummary[], filters: OrdersClientFilters) {
  const statusSet = new Set(filters.orderStatus.map((value) => value.toLowerCase()));
  const paymentSet = new Set(filters.orderPayment);

  return orders.filter((order) => {
    if (
      filters.hideCanceled &&
      !filters.returnsOnly &&
      !statusSet.has("cancelled") &&
      !statusSet.has("refunded") &&
      !statusSet.has("canceled") &&
      isCanceledOrder(order)
    ) {
      return false;
    }

    if (statusSet.size) {
      const fulfillmentStatus = String(order.fulfillment_status ?? "").trim().toLowerCase();
      if (!statusSet.has(fulfillmentStatus)) {
        return false;
      }
    }

    if (paymentSet.size) {
      const paymentMethod = String(order.payment_method ?? "").trim();
      if (!paymentSet.has(paymentMethod)) {
        return false;
      }
    }

    if (filters.returnsOnly && !isCanceledOrder(order)) {
      return false;
    }

    if (filters.hasPurchaseCost && !(Number(order.purchase_cost_cents ?? 0) > 0)) {
      return false;
    }

    if (filters.noPurchaseCost && Number(order.purchase_cost_cents ?? 0) > 0) {
      return false;
    }

    if (filters.hasInvoice && !order.invoice) {
      return false;
    }

    if (filters.noInvoice && order.invoice) {
      return false;
    }

    return true;
  });
}

export function getOrdersClientFilterCount(filters: OrdersClientFilters) {
  return (
    filters.orderStatus.length +
    filters.orderPayment.length +
    (filters.returnsOnly ? 1 : 0) +
    (filters.hasPurchaseCost ? 1 : 0) +
    (filters.noPurchaseCost ? 1 : 0) +
    (filters.hasInvoice ? 1 : 0) +
    (filters.noInvoice ? 1 : 0) +
    (filters.hideCanceled ? 0 : 1)
  );
}
