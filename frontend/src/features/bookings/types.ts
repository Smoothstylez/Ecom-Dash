export interface BookingDocumentSummary {
  id?: string;
  original_filename?: string;
  stored_filename?: string;
  file_path?: string;
  mime_type?: string;
  uploaded_at?: string;
  notes?: string;
}

export interface BookingOrderReference {
  id?: string;
  provider?: string;
  external_order_id?: string;
}

export interface BookingTemplateReference {
  id?: string;
  name?: string;
  schedule?: string;
}

export interface BookingPaymentAccount {
  id?: string;
  name?: string;
  provider?: string;
  is_active?: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface BookingTemplate {
  id?: string;
  name?: string;
  type?: string;
  direction?: string;
  default_amount_gross?: number | null;
  currency?: string;
  provider?: string;
  counterparty_name?: string | null;
  category?: string | null;
  vat_rate?: number | null;
  payment_account_id?: string | null;
  schedule?: string;
  day_of_month?: number | null;
  start_date?: string | null;
  active?: boolean;
  notes_default?: string | null;
  created_at?: string;
  updated_at?: string;
  payment_account?: BookingPaymentAccount | null;
}

export interface BookingDocument {
  id?: string;
  original_filename?: string;
  stored_filename?: string;
  file_path?: string;
  mime_type?: string;
  uploaded_at?: string;
  notes?: string | null;
  _count?: {
    transactions?: number;
  };
}

export interface MonthlyInvoiceTransaction {
  id?: string;
  date?: string;
  type?: string;
  direction?: string;
  amount_gross?: number;
  currency?: string;
  provider?: string;
  counterparty_name?: string | null;
  category?: string | null;
  reference?: string | null;
  notes?: string | null;
  order_id?: string | null;
  source?: string;
  status?: string;
  booking_class?: string;
}

export interface MonthlyInvoice {
  id?: string;
  provider?: string;
  period_from?: string;
  period_to?: string;
  invoice_amount_cents?: number;
  currency?: string;
  calculated_sum_cents?: number;
  difference_cents?: number;
  document_id?: string | null;
  notes?: string | null;
  status?: string;
  created_at?: string;
  updated_at?: string;
  document?: {
    id?: string;
    original_filename?: string;
    mime_type?: string;
  } | null;
  transactions?: MonthlyInvoiceTransaction[];
}

export interface BookingTransaction {
  id?: string;
  date?: string;
  type?: string;
  direction?: string;
  amount_gross?: number;
  currency?: string;
  vat_rate?: number | null;
  vat_amount?: number | null;
  amount_net?: number | null;
  provider?: string;
  counterparty_name?: string | null;
  category?: string | null;
  reference?: string | null;
  order_id?: string | null;
  document_id?: string | null;
  template_id?: string | null;
  payment_account_id?: string | null;
  period_key?: string | null;
  notes?: string | null;
  source?: string;
  source_key?: string | null;
  status?: string;
  booking_class?: string;
  created_at?: string;
  updated_at?: string;
  order?: BookingOrderReference | null;
  document?: BookingDocumentSummary | null;
  template?: BookingTemplateReference | null;
  payment_account?: BookingPaymentAccount | null;
}

export interface BookingTransactionsPayload {
  total?: number;
  items?: BookingTransaction[];
  db_available?: boolean;
}

export interface BookingPaymentAccountsPayload {
  total?: number;
  items?: BookingPaymentAccount[];
  db_available?: boolean;
}

export interface BookingTemplatesPayload {
  total?: number;
  items?: BookingTemplate[];
  db_available?: boolean;
}

export interface BookingDocumentsPayload {
  total?: number;
  items?: BookingDocument[];
  db_available?: boolean;
}

export interface MonthlyInvoicesPayload {
  total?: number;
  items?: MonthlyInvoice[];
  db_available?: boolean;
}

export interface BookingOrderItem {
  marketplace?: string;
  order_id?: string;
  external_order_id?: string;
  order_date?: string;
  customer?: string;
  article?: string;
  currency?: string;
  revenue_cents?: number;
  fees_cents?: number;
  purchase_cents?: number;
  bookkeeping_fee_cents?: number;
  bookkeeping_cogs_cents?: number;
  bookkeeping_other_cents?: number;
  bookkeeping_additional_fee_cents?: number;
  bookkeeping_additional_cogs_cents?: number;
  bookkeeping_additional_other_cents?: number;
  bookkeeping_expense_total_cents?: number;
  bookkeeping_mirrored_fee_cents?: number;
  bookkeeping_mirrored_cogs_cents?: number;
  bookkeeping_income_cents?: number;
  bookkeeping_expense_cents?: number;
  total_costs_cents?: number;
  profit_cents?: number;
  documents_count?: number;
  bookkeeping_matched_via?: string;
}

export interface BookingOrdersPayload {
  total?: number;
  items?: BookingOrderItem[];
}

export interface BookingLedgerOrdersPayload {
  total?: number;
  items?: Array<{
    id?: string;
    provider?: string;
    external_order_id?: string;
    order_date?: string;
    currency?: string;
    revenue_gross?: number;
    revenue_net?: number | null;
    vat_amount?: number | null;
    status?: string;
    created_at?: string;
    updated_at?: string;
    _count?: {
      transactions?: number;
    };
  }>;
  db_available?: boolean;
}
