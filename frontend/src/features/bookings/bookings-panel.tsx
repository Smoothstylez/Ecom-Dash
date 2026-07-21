import type { BookingsSubtab } from "@/app/dashboard-shell-state";

function classNames(...parts: Array<string | false>) {
  return parts.filter(Boolean).join(" ");
}

type BookingsPanelProps = {
  panelRef: (element: HTMLDivElement | null) => void;
  bookingsSubtab: BookingsSubtab;
};

export function BookingsPanel({ panelRef, bookingsSubtab }: BookingsPanelProps) {
  const transactionsActive = bookingsSubtab === "transactions";
  const ordersActive = bookingsSubtab === "orders";
  const templatesActive = bookingsSubtab === "templates";
  const accountsActive = bookingsSubtab === "accounts";
  const documentsActive = bookingsSubtab === "documents";

  return (
    <div ref={panelRef} id="bookingsPanel" className="tab-panel active" data-react-bookings-mounted="true">
      <div className="bookings-subtab-bar">
        <button id="bookingsNewBtn" className="bookings-tools-toggle" type="button" aria-expanded="false" data-target="bookingsTransactionTools">
          <svg viewBox="0 0 20 20"><line x1="10" y1="4" x2="10" y2="16" /><line x1="4" y1="10" x2="16" y2="10" /></svg>
          Neue Transaktion
        </button>
      </div>

      <div id="bookingClassBar" className="booking-class-bar">
        <div id="bookingClassControl" className="subtabbar booking-class-subtabbar">
          <button id="bookingClassAllBtn" className="subtab-btn subtab-btn-sub" type="button" data-booking-class="all">Gesamt</button>
          <button id="bookingClassAutoBtn" className="subtab-btn subtab-btn-sub active" type="button" data-booking-class="automatic">Automatisch</button>
          <button id="bookingClassMonthlyBtn" className="subtab-btn subtab-btn-sub" type="button" data-booking-class="monthly">Monatlich</button>
          <button id="bookingClassSingleBtn" className="subtab-btn subtab-btn-sub" type="button" data-booking-class="single">Einzeln</button>
        </div>
      </div>

      <section className="card table-card">
        <div id="bookingsTransactionsPanel" className={classNames("bookings-subpanel", transactionsActive && "active")}>
          <div id="bookingsTransactionTools" className="bookings-tools">
            <div className="bookings-form-grid">
              <div className="control">
                <label htmlFor="createBookingDate">Datum</label>
                <input id="createBookingDate" type="date" />
              </div>
              <div className="control">
                <label htmlFor="createBookingType">Typ</label>
                <select id="createBookingType" defaultValue="SALE">
                  <option value="SALE">SALE</option>
                  <option value="COGS">COGS</option>
                  <option value="FEE">FEE</option>
                  <option value="SHIPPING">SHIPPING</option>
                  <option value="SUBSCRIPTION">SUBSCRIPTION</option>
                  <option value="EXPENSE">EXPENSE</option>
                  <option value="REFUND">REFUND</option>
                  <option value="PAYOUT">PAYOUT</option>
                  <option value="ADJUSTMENT">ADJUSTMENT</option>
                </select>
              </div>
              <div className="control">
                <label htmlFor="createBookingDirection">Richtung</label>
                <select id="createBookingDirection" defaultValue="IN">
                  <option value="IN">IN</option>
                  <option value="OUT">OUT</option>
                </select>
              </div>
              <div className="control">
                <label htmlFor="createBookingAmount">Betrag (EUR)</label>
                <input id="createBookingAmount" type="text" inputMode="decimal" placeholder="0,00" />
              </div>
              <div className="control">
                <label htmlFor="createBookingVatAmount">Vorsteuer (EUR)</label>
                <input id="createBookingVatAmount" type="text" inputMode="decimal" placeholder="0,00" />
              </div>
              <div className="control">
                <label htmlFor="createBookingVatDeductible">Vorsteuer abziehbar</label>
                <select id="createBookingVatDeductible" defaultValue="false">
                  <option value="false">nein</option>
                  <option value="true">ja</option>
                </select>
              </div>
              <div className="control">
                <label htmlFor="createBookingProvider">Provider</label>
                <input id="createBookingProvider" type="text" placeholder="shopify" />
              </div>
              <div className="control">
                <label htmlFor="createBookingCounterparty">Gegenpartei</label>
                <input id="createBookingCounterparty" type="text" placeholder="Optional" />
              </div>
              <div className="control">
                <label htmlFor="createBookingStatus">Status</label>
                <select id="createBookingStatus" defaultValue="confirmed">
                  <option value="pending">pending</option>
                  <option value="confirmed">confirmed</option>
                  <option value="reconciled">reconciled</option>
                </select>
              </div>
              <div className="control">
                <label htmlFor="createBookingReference">Referenz</label>
                <input id="createBookingReference" type="text" placeholder="Order-ID / Beleg-Nr." />
              </div>
              <div className="control">
                <label htmlFor="createBookingCategory">Kategorie</label>
                <input id="createBookingCategory" type="text" placeholder="Optional" />
              </div>
              <div className="control">
                <label htmlFor="createBookingOrder">Order-Link (optional)</label>
                <select id="createBookingOrder" />
              </div>
              <div className="control">
                <label htmlFor="createBookingAccount">Konto (optional)</label>
                <select id="createBookingAccount" />
              </div>
              <div className="control">
                <label htmlFor="createBookingTemplate">Template (optional)</label>
                <select id="createBookingTemplate" />
              </div>
              <div className="control" style={{ gridColumn: "1 / -1" }}>
                <label htmlFor="createBookingNotes">Notiz</label>
                <input id="createBookingNotes" type="text" placeholder="Optional" />
              </div>
              <div className="control" style={{ gridColumn: "1 / -1" }}>
                <label htmlFor="createBookingDocumentFile">Beleg (optional)</label>
                <input id="createBookingDocumentFile" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp,.txt,.csv,.zip,.doc,.docx" />
              </div>
            </div>
            <div className="bookings-form-actions">
              <button id="createBookingTxBtn" className="btn-inline primary" type="button">Transaktion anlegen</button>
            </div>
          </div>

          <div id="bookingTxLegend" className="booking-type-legend" />

          <div id="bookingsTransactionsReactRoot" />

          <div id="sammelrechnungSection" className="sammelrechnung-section" style={{ display: "none" }}>
            <div id="bookingsMonthlyInvoicesReactRoot" />

            <div id="sammelrechnungTools" className="bookings-tools">
              <div className="bookings-form-grid">
                <div className="control">
                  <label htmlFor="createSammelProvider">Provider</label>
                  <select id="createSammelProvider" defaultValue="paypal">
                    <option value="paypal">PayPal Fees</option>
                    <option value="shopify_payments">Shopify Payments Fees</option>
                    <option value="kaufland">Kaufland Fees</option>
                    <option value="google_ads">Google Ads</option>
                    <option value="ebay">eBay Fees</option>
                  </select>
                </div>
                <div className="control control-menu-wrap sammel-month-wrap">
                  <label>Monat</label>
                  <button id="sammelMonthBtn" className="control-menu-trigger sammel-month-trigger" type="button" aria-expanded="false" aria-controls="sammelMonthMenu">-</button>
                  <div id="sammelMonthMenu" className="control-menu sammel-month-menu" aria-hidden="true">
                    <div className="sammel-month-nav">
                      <button id="sammelYearPrevBtn" className="menu-item" type="button">&larr;</button>
                      <span id="sammelYearLabel" className="sammel-year-label">2025</span>
                      <button id="sammelYearNextBtn" className="menu-item" type="button">&rarr;</button>
                    </div>
                    <div id="sammelMonthGrid" className="sammel-month-grid" />
                  </div>
                </div>
                <div className="control">
                  <label htmlFor="createSammelAmount">Rechnungsbetrag (EUR)</label>
                  <input id="createSammelAmount" type="text" inputMode="decimal" placeholder="0,00" />
                </div>
                <div className="control">
                  <label htmlFor="createSammelVatAmount">Vorsteuer (EUR)</label>
                  <input id="createSammelVatAmount" type="text" inputMode="decimal" placeholder="0,00" />
                </div>
                <div className="control">
                  <label htmlFor="createSammelNotes">Notiz</label>
                  <input id="createSammelNotes" type="text" placeholder="Optional" />
                </div>
                <div className="control">
                  <label>Beleg</label>
                  <div className="sammel-file-field">
                    <label className="sammel-file-btn" htmlFor="createSammelFile">Datei waehlen</label>
                    <input id="createSammelFile" className="sammel-file-input" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp" />
                    <div id="createSammelFileName" className="sammel-file-name">Optional</div>
                  </div>
                </div>
              </div>
              <div id="sammelPreview" className="sammel-preview" style={{ display: "none" }} />
              <div className="bookings-form-actions">
                <button id="createSammelBtn" className="btn-inline primary" type="button">Sammelrechnung anlegen</button>
              </div>
            </div>
          </div>
        </div>

        <div id="bookingsOrdersPanel" className={classNames("bookings-subpanel", ordersActive && "active")}>
          <div id="bookingsOrdersReactRoot" />
        </div>

        <div id="bookingsTemplatesPanel" className={classNames("bookings-subpanel", templatesActive && "active")}>
          <div id="bookingsTemplateTools" className="bookings-tools">
            <div className="bookings-form-grid">
              <div className="control">
                <label htmlFor="templateNameInput">Name</label>
                <input id="templateNameInput" type="text" placeholder="Shopify Abo" />
              </div>
              <div className="control">
                <label htmlFor="templateTypeInput">Typ</label>
                <select id="templateTypeInput" defaultValue="SUBSCRIPTION">
                  <option value="SUBSCRIPTION">SUBSCRIPTION</option>
                  <option value="EXPENSE">EXPENSE</option>
                  <option value="FEE">FEE</option>
                  <option value="SHIPPING">SHIPPING</option>
                  <option value="COGS">COGS</option>
                  <option value="SALE">SALE</option>
                </select>
              </div>
              <div className="control">
                <label htmlFor="templateDirectionInput">Richtung</label>
                <select id="templateDirectionInput" defaultValue="OUT">
                  <option value="OUT">OUT</option>
                  <option value="IN">IN</option>
                </select>
              </div>
              <div className="control">
                <label htmlFor="templateAmountInput">Default Betrag (EUR)</label>
                <input id="templateAmountInput" type="text" inputMode="decimal" placeholder="0,00" />
              </div>
              <div className="control">
                <label htmlFor="templateProviderInput">Provider</label>
                <input id="templateProviderInput" type="text" defaultValue="shopify" />
              </div>
              <div className="control">
                <label htmlFor="templateCounterpartyInput">Gegenpartei</label>
                <input id="templateCounterpartyInput" type="text" placeholder="z. B. OpenAI" />
              </div>
              <div className="control">
                <label htmlFor="templateScheduleInput">Intervall</label>
                <select id="templateScheduleInput" defaultValue="monthly">
                  <option value="monthly">monthly</option>
                  <option value="quarterly">quarterly</option>
                  <option value="yearly">yearly</option>
                </select>
              </div>
              <div className="control">
                <label htmlFor="templateStartDateInput">Abo Startdatum</label>
                <input id="templateStartDateInput" type="date" />
              </div>
              <div className="control">
                <label htmlFor="templateDayInput">Tag im Monat</label>
                <input id="templateDayInput" type="number" min="1" max="31" placeholder="3" />
              </div>
              <div className="control">
                <label htmlFor="templateAccountInput">Konto (optional)</label>
                <select id="templateAccountInput" />
              </div>
              <div className="control" style={{ gridColumn: "1 / -1" }}>
                <label htmlFor="templateNotesInput">Notiz (optional)</label>
                <input id="templateNotesInput" type="text" placeholder="Wird in generierte Buchung uebernommen" />
              </div>
            </div>
            <div className="bookings-form-actions">
              <button id="createTemplateBtn" className="btn-inline secondary" type="button">Template anlegen</button>
            </div>
          </div>

          <div id="bookingsTemplatesReactRoot" />
        </div>

        <div id="bookingsAccountsPanel" className={classNames("bookings-subpanel", accountsActive && "active")}>
          <div id="bookingsAccountTools" className="bookings-tools">
            <div className="bookings-form-grid">
              <div className="control">
                <label htmlFor="accountNameInput">Name</label>
                <input id="accountNameInput" type="text" placeholder="Bank DE" />
              </div>
              <div className="control">
                <label htmlFor="accountProviderInput">Provider</label>
                <input id="accountProviderInput" type="text" placeholder="bank" />
              </div>
              <div className="control">
                <label htmlFor="accountActiveInput">Aktiv</label>
                <select id="accountActiveInput" defaultValue="true">
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              </div>
            </div>
            <div className="bookings-form-actions">
              <button id="createAccountBtn" className="btn-inline secondary" type="button">Konto anlegen</button>
            </div>
          </div>

          <div id="bookingsAccountsReactRoot" />
        </div>

        <div id="bookingsDocumentsPanel" className={classNames("bookings-subpanel", documentsActive && "active")}>
          <div id="bookingsDocumentTools" className="bookings-tools">
            <div className="bookings-form-grid">
              <div className="control" style={{ gridColumn: "1 / -1" }}>
                <label htmlFor="bookingDocumentFileInput">Datei</label>
                <input id="bookingDocumentFileInput" type="file" accept=".pdf,.png,.jpg,.jpeg,.webp,.txt,.csv,.zip,.doc,.docx" />
              </div>
              <div className="control">
                <label htmlFor="bookingDocumentTxInput">Transaktion (optional)</label>
                <select id="bookingDocumentTxInput" />
              </div>
              <div className="control">
                <label htmlFor="bookingDocumentNotesInput">Notiz (optional)</label>
                <input id="bookingDocumentNotesInput" type="text" placeholder="Lieferantenrechnung" />
              </div>
            </div>
            <div className="bookings-form-actions">
              <button id="uploadBookingDocumentBtn" className="btn-inline secondary" type="button">Beleg hochladen</button>
            </div>
          </div>

          <div id="bookingsDocumentsReactRoot" />
        </div>
      </section>
    </div>
  );
}
