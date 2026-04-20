export interface OrderInvoiceSummary {
  document_id?: string;
  original_filename?: string;
  stored_filename?: string;
  mime_type?: string;
  uploaded_at?: string;
}

export interface OrderBookkeepingBreakdown {
  db_available?: boolean;
  matched_via?: string;
  income_total_cents?: number;
  expense_total_cents?: number;
  additional_expense_total_cents?: number;
  fee_total_cents?: number;
  additional_fee_cents?: number;
  cogs_total_cents?: number;
  additional_cogs_cents?: number;
  mirrored_fee_total_cents?: number;
  mirrored_cogs_total_cents?: number;
  other_expenses_cents?: number;
  additional_other_cents?: number;
}

export interface OrderDetailPayload {
  summary?: OrderSummary;
  order?: Record<string, unknown>;
  order_raw?: Record<string, unknown>;
  line_items?: Array<Record<string, unknown>>;
  transactions?: Array<Record<string, unknown>>;
  fulfillments?: Array<Record<string, unknown>>;
  refunds?: Array<Record<string, unknown>>;
  units?: Array<Record<string, unknown>>;
  shipping_address?: Record<string, unknown>;
  billing_address?: Record<string, unknown>;
  customer?: Record<string, unknown>;
  bookkeeping_breakdown?: OrderBookkeepingBreakdown;
}

export interface OrderSummary {
  marketplace?: string;
  order_id?: string;
  external_order_id?: string;
  order_date?: string;
  customer?: string;
  article?: string;
  line_items_count?: number;
  total_cents?: number;
  fees_cents?: number;
  after_fees_cents?: number;
  shipping_cents?: number;
  currency?: string;
  fulfillment_status?: string;
  raw_status?: string;
  financial_status?: string;
  payment_method?: string;
  fee_source?: string;
  purchase_cost_cents?: number;
  purchase_currency?: string;
  purchase_supplier?: string;
  purchase_notes?: string;
  profit_cents?: number;
  invoice?: OrderInvoiceSummary | null;
}

export interface OrdersPayload {
  total?: number;
  items?: OrderSummary[];
  limit?: number;
  offset?: number;
}

export interface OrdersClientFilters {
  orderStatus: string[];
  orderPayment: string[];
  returnsOnly: boolean;
  hideCanceled: boolean;
  hasPurchaseCost: boolean;
  noPurchaseCost: boolean;
  hasInvoice: boolean;
  noInvoice: boolean;
}

export interface OrdersServerFilters {
  q: string;
  marketplace: string;
  from: string;
  to: string;
}

export function createDefaultOrdersClientFilters(): OrdersClientFilters {
  return {
    orderStatus: [],
    orderPayment: [],
    returnsOnly: false,
    hideCanceled: true,
    hasPurchaseCost: false,
    noPurchaseCost: false,
    hasInvoice: false,
    noInvoice: false,
  };
}

export function createDefaultOrdersServerFilters(): OrdersServerFilters {
  return {
    q: "",
    marketplace: "",
    from: "",
    to: "",
  };
}
