# Buchhaltungs-Deep-Dive: Brutto/Netto-Umstellung, USt-Pflicht ab 100.000 €, Vorsteuer

**Datum:** 2026-08-21
**Typ:** Read-only Analyse (keine Code-/Datenänderungen; SQLite ausschließlich im `mode=ro` geöffnet, ein reiner Funktionstest ohne Seiteneffekte)
**Umfang:** Komplette Buchhaltungslogik – Importer, Combined Orders, Analytics, Buchhaltungs-Sync, USt-Report, Amazon-FIFO/Finance-Layer, Sammelrechnungen

---

## 1. Ausgangslage

Der Betreiber war Kleinunternehmer (§19 UStG): Einkäufe und Verkäufe effektiv brutto = netto,
keine USt. Im Juni 2026 wurde die 100.000-€-Grenze des laufenden Jahres überschritten → ab dem
überschreitenden Auftrag Umsatzsteuerpflicht (Regelbesteuerung). Ab dann gilt:

- **Verkäufe**: weiterhin brutto, aber mit ausgewiesener/geschuldeter Output-USt.
- **Einkäufe**: netto betrachten. AliExpress-Einkäufe haben **keine ziehbare Vorsteuer**
  (keine Rechnung mit ausgewiesener USt) ⇒ Brutto = Vollkosten = netto. EU-Einkäufe mit
  ordentlicher Rechnung: Brutto + USt-Anteil + Vorsteuerziehbarkeit erfassen.
- **Gebühren** (Amazon, Kaufland, Google Ads): deren ausgewiesene USt ist ziehbare Vorsteuer.

Aufgabe der Analyse: prüfen, wie weit das System diese Umstellung bereits abbildet, wo
Diskrepanzen existieren (Storno/Refund-Behandlung, Gebühren-VAT, Bücher vs. Dashboard) und was
zu fixen ist.

## 2. Projektkarte & Datenfluss

```
Importer                     Quell-DBs                          Runtime-DBs
shopify_live.py       ──►   data/sources/shopify/shopify_data.sqlite3 ─┐
kaufland_live.py      ──►   data/sources/kaufland/kaufland_data.sqlite3 ─┼─► populate_combined_orders()
amazon_sp_api.py      ──►   data/sources/amazon_fba/amazon_fba.sqlite3 ──┘        [app/db.py:384]
                                                                                   │
                                                              data/combined.sqlite3 (combined_orders)
                                                                                   │
        ┌──────────────────────────────────────────────────────────────────────────┤
        ▼                                                                          ▼
Orders / Analytics / USt-Report                                  sync_combined_orders_into_bookkeeping()
[orders.py] [analytics.py] [tax_reporting.py]                    [bookings.py:636]
                                                                 ▼
                                              data/sources/bookkeeping/dashboard.sqlite3
                                              (transactions/orders/documents/monthly_invoices)

Amazon-Finanzlayer separat:
  Financial Events mit lifecycle_id-Dedup [amazon_sp_api.py:2090-2198]
  Canonical-Representative-Predicate      [amazon_fba.py:22-95]
  FIFO-Lots + Supplier-Invoices           [amazon_fba.py:797-1206]

Serverstart ruft reconcile_runtime_state() [runtime_reconcile.py:36] auf,
welches AUTOMATISCH in die Buchhaltungs-DB schreibt (Order-Sync + Google-Ads-Sync).
```

**USt-Report:** `GET /api/invoices/tax-report` [routers/invoices.py:73-84] → ruft
`populate_combined_orders()` (schreibt!) → `list_all_orders_without_pagination()` (mit
`hide_canceled=False`, ohne Statusfilter) → `build_vat_report()` [tax_reporting.py:171].

## 3. Antworten auf die konkreten Fragen

### 3.1 „Zählt bei zurückerstatteten Orders immer noch der Kaufpreis?" — JA (Analytics)

`_normalize_order_metrics` [analytics.py:132-165]: Bei Return-artigen Status
(cancel/refund/return/void/…) werden Umsatz, Gebühren, Gewinn, Versand **genullt — aber
`purchase` nicht** (Zeilen 150-155). Der AliExpress-Einkauf eines stornierten Auftrags läuft
also weiter in `purchase_total_cents`, die Monatsbuckets und den Marketplace-KPIs.

Im Gegensatz dazu löscht die **Buchhaltungs-Sync** dieselben Aufträge komplett
(`_normalized_sync_amounts` [bookings.py:223-240]: Return-artig && nicht partial-refund ⇒
alle Beträge 0 ⇒ `_upsert_synced_transaction` löscht Sale/Fee/COGS [bookings.py:489-492]).

⇒ Dashboard-Analytics und Bücher widersprechen sich systematisch bei Retouren.
⇒ **Rückzahlungen von AliExpress an den Betreiber (Supplier-Refunds) sind nirgends
modellierbar** — kein Feld, kein Importer, kein Transaktionstyp dafür.

### 3.2 „Werden gecancelte Orders richtig rausgefiltert?" — marketplace-abhängig

| Marketplace | Verhalten | Evidenz |
|---|---|---|
| Kaufland | ✅ korrekt: SQL filtert `status NOT IN ('cancelled','canceled')` aus allen Geldsummen | db.py:455, 465, 475, 479, 485, 495, 503 |
| Shopify | ❌ voll `refunded`/`voided` Orders zählen mit vollem `total_price`; Refund-Abzug nur bei `partially_refunded` | order_summaries.py:272-279 |
| Amazon | ⚠️ aktuell ok (6 Canceled ohne Finance-Events = 0 €), aber Fallback auf `item_sales_cents` wenn keine Finance-Events vorhanden ist latent riskant | amazon_fba.py:111 |

**Gemessenes Shopify-Leck:** 16 Orders mit refund-artigem Status tragen **2.983,48 €** Umsatz in
den Combined-Orders-/Threshold-/USt-Pfad (davon 2026: #1097, #1108, #1199–#1203).

**Auswirkung auf die Threshold-Ermittlung 2026** (identischer Walk wie
`build_threshold_candidate` [tax_reporting.py:145-168], der selbst NICHT filtert und vom
Aufrufer alle Orders erhält [invoices.py:77-83, orders.py:1146]):

- inkl. refundierter Orders (wie codiert): Überschreitung am **29.06.2026**
  (Kaufland MEART75, kumuliert 100.082,04 €)
- exklusive Return-artiger Orders: **30.06.2026** (kumuliert 100.004,07 €)

Der Stichtag hängt also direkt am Shopify-Storno-Bug.

Kleinigkeiten: `line_items_count` bei Kaufland zählt stornierte Units mit (db.py:512-514);
`build_vat_report` gibt für Refund/Cancel-Status nur eine Warnung aus, rechnet die volle
Output-VAT aber trotzdem an [tax_reporting.py:187-198].

### 3.3 Ist die Brutto/Netto-Umstellung vorbereitet? — Struktur ja, Zustand nein

**Vorhanden (gutes Fundament):**
- Combined Orders: `sales_gross/net/vat_cents`, `purchase_cost/vat_cents`,
  `purchase_is_vat_deductible`, `purchase_supplier/currency` [db.py:588-646]
- Annotierung pro Order: `vat_applicable`, `output_vat_cents`,
  `deductible_purchase_vat_cents`, `purchase_effective_cost_cents`,
  `vat_due_before_fee_invoices_cents` [tax_reporting.py:51-79]
- Verkäuferprofil mit `tax_mode` (`small_business`|`regular`) + `vat_effective_from`
  [invoices.py:20, tax_reporting.py:14-39]; Sales-Invoices warnen bei
  Kleinunternehmer-Profil trotz Steuerwerten in Quelldaten [invoices.py:818-819]
- Amazon: Fee-Splits gross/base/tax werden erhalten, NICHT per /1,19 geschätzt
  [amazon_sp_api.py:124-186]; Lifecycle-Dedup Deferred→Released über `lifecycle_id`
  [amazon_sp_api.py:2114]; Canonical-Representative-Predicate verhindert Doppelzählung
  [amazon_fba.py:22-95]; FIFO-Einheitkosten aus **Netto**-Rechnungszeile
  [amazon_fba.py:883-884]; Supplier-Invoices mit gross/net/vat +
  `input_vat_status` [amazon_fba.py:1126-1143] — entspricht dem Design-Doc
  `docs/superpowers/specs/2026-08-21-amazon-financial-lifecycle-and-fee-accounting-design.md`

**Zustand (Daten/Konfiguration heute):**
- `seller_profiles`: **`tax_mode='small_business'`, `vat_effective_from=''`** — das System weiß
  nichts von der Überschreitung; jeder USt-Report liefert aktuell 0 € Output-VAT plus Warnung
  [tax_reporting.py:225-228].
- Buchhaltungs-DB: **1.628 api-syncte Transaktionen haben `vat_amount=NULL`,
  `amount_net=NULL`, `is_vat_deductible=0`** — der Sync insertet hartcodiert NULLs
  [bookings.py:537, 596-598]. Nur 7 manuelle Buchungen, davon 1 mit VAT. Die Bücher kennen
  keine Steuerinformationen.
- Amazon: `inventory_lots=0`, `fifo_allocations=0`, `supplier_invoices=0` ⇒ Amazon-COGS = 0 €
  in allen Sichten; zusätzlich `purchase_is_vat_deductible` für Amazon hartcodiert `False`
  [db.py:574-577, amazon_fba.py:145].

## 4. Bestätigte Bugs (priorisiert, mit Evidenz)

### 🔴 B1 — Kaufland sales-VAT ×100 zu hoch (kritisch)

`to_kaufland_cents()` [order_summaries.py:71-104] interpretiert nicht-ganzzahlige Floats als
Euro und multipliziert ×100. Die SQL-Aggregate `units_vat_sum`/`shipping_vat_sum` in
db.py:458-467/488-497 (`SUM(price * vat/(100+vat))`) sind REAL-Werte, praktisch nie ganzzahlig
— jedes nicht-ganzzahlige Ergebnis wird ×100 aufgeblasen.

Direktnachweis gegen den echten Code:
```
to_kaufland_cents(3990.0)             -> 3990      # ganzzahlig: ok
to_kaufland_cents(1419.4117647058824) -> 141941    # ×100 INFLATED (echter DB-Wert M16YXQ5)
to_kaufland_cents(3032.016806722689)  -> 303202    # ×100 INFLATED
```

Folgen in combined.sqlite3 (kaufland, 604 Orders):
- `sales_vat` summiert **1.365.415,75 €** statt ~15.530 € (~×100 zu hoch)
- `sales_net_cents = max(gross − inflated_vat, 0) = 0` bei **459 von 604 Orders**
  (Beispiel MK557K5: gross 14990 ct, vat 239336 ct, net 0)

Solange `tax_mode=small_business` ist, maskiert `vat_applicable=False` das Problem im Report;
nach der Umstellung wäre jeder Kaufland-Wert im USt-Report Müll, und Netto-Umsätze für
Kaufland sind überall im Dashboard falsch (0).

**Fix-Richtung:** VAT-Beträge bereits im SQL auf ganze Cent runden (`ROUND(...)`) oder die
Cent/Euro-Heuristik für diese Spalten entfernen (die Werte sind per Konstruktion Cent).

### 🔴 B2 — Shopify: voll erstattete/stornierte Orders zählen voll (kritisch)

`shopify_summary_from_row` [order_summaries.py:265-280]: Refund-Abzug nur bei
`partially_refunded`; `refunded`/`voided` behalten volles `total_price`. Keine Extraktion von
`cancelled_at`/`cancel_reason`. Betrifft Orders-, Analytics-, Threshold- UND USt-Pfad.
Gemessen: 16 Orders / 2.983,48 €; verschiebt die Threshold-Überschreitung von 29.06. auf
30.06.2026. Die Buchhaltungs-Sync nullt dieselben Orders (bookings.py:237) ⇒ Bücher vs.
Dashboard inkonsistent.

**Fix-Richtung:** bei `refunded`/`voided` (und `cancel`-Tokens) total/vat/net/fees auf 0
setzen bzw. `gross − refund_sum` analog zum Partial-Fall; konsistent mit
`_is_return_like_status`.

### 🔴 B3 — Shopify hat durchgehend `total_tax=0` — nach Umstellung fehlt jede Output-VAT-Quelle (kritisch)

Alle Shopify-Rows: `sales_vat_cents=0` (historisch korrekt als Kleinunternehmer — keine USt
ausgewiesen). Nach dem Stichtag braucht der Kanal aber USt. Es existiert **kein Mechanismus**,
der Output-VAT aus dem Brutto ableitet (÷1,19), falls Shopify weiter keine Steuer ausweist.
Ohne Aktion: 0 € Umsatzsteuer auf den kompletten Shopify-Kanal im Monatsreport.

**Fix-Richtung:** Entweder Shopify-Tax-Einstellungen aktivieren (dann liefert `total_tax`) und/
oder im Reporting eine explizite, gekennzeichnete Schätzung aus Brutto rechnen
(„ausgewiesene USt fehlt") statt stiller 0.

### 🟠 B4 — Refund-Currency-Bug Shopify (hoch)

`refund_amount_sum` summiert Shopify-Refund-Transaktionen ohne Währungsprüfung [db.py:421-429].
Gemessen: #1200/#1203/#1204 haben Refund-Transaktionen in **SEK** (presentment currency):
2.183,00 / 2.071,00 / 3.558,00 SEK gegenüber Auftragswerten von 197,87 / 187,69 / 320,23 EUR.
Die Summe wird als EUR-Cent interpretiert (Faktor ~10–11 über Ziel). Bei
`partially_refunded` würde `max(gross − refund, 0)` den Umsatz auf 0 drücken; bei voll
refundierten maskiert es sich.

**Fix-Richtung:** Währung je Transaktion prüfen (`$.currency`), Fremdwährungen ausschließen
oder umrechnen; ggf. `shop_money`/`presentment_money` sauber trennen.

### 🟠 B5 — Amazon-Einkauf komplett ungebucht (hoch)

FIFO-Stack ist implementiert, aber leer: `inventory_lots=0`, `fifo_allocations=0`,
`supplier_invoices=0` ⇒ `fifo_cogs_cents`=0 ⇒ Amazon-Kaufpreis in combined_orders = 0 €
(gemessen: amazon purch=0,00 € über alle 54 Finance-Orders). Zusätzlich ist
`purchase_is_vat_deductible` für Amazon hartcodiert `False` [db.py:577]. Damit sind
Amazon-Einkaufskosten und potenzielle Vorsteuer aus Lieferantenrechnungen derzeit
unsichtbar, obwohl genau dafür der Netto-FIFO-Workflow gebaut wurde.

**Fix-Richtung:** Beschaffungsstrecke tatsächlich nutzen (procurement batch → supplier
invoice → confirm → FIFO), oder alternativ das AliExpress-Enrichment auch für Amazon-Orders
zulassen; Deductible-Flag aus `input_vat_status` ableiten statt hartcodieren.

### 🟠 B6 — Gebühren-Vorsteuer erreicht den USt-Report nicht (hoch)

- Amazon parst Fee-gross/net/VAT korrekt und aggregiert sie sogar pro Event
  (`fees_net_cents`, `fees_vat_cents` in `get_amazon_finance_overview` [amazon_fba.py:1060-1073]) —
  aber `build_vat_report` liest ausschließlich die manuelle Tabelle `monthly_invoices`
  [tax_reporting.py:95-115] plus manuelle `transactions`. Die geparste Fee-VAT fließt nirgends ein.
- Kaufland-Gebühren: `fees_cents = price − revenue_gross` [order_summaries.py:372-375] ohne
  VAT-Split; Kauflands Gebührenabrechnung enthält USt — nicht erfasst.
- Google Ads: alle 4 vorhandenen Sammelrechnungen haben `vat_amount_cents=0`, Status
  `mismatch` — Ads-Rechnungen enthalten regulär 19 % USt als ziehbare Vorsteuer.

**Fix-Richtung:** Input-VAT-Quellen im Report vereinigen: (a) Order-gebundene
Purchase-VAT (vorhanden), (b) Amazon-Fee-VAT aus Finance-Layer, (c) Kaufland-Gebühren-
Sammelrechnung, (d) Google Ads Sammelrechnung (VAT pflegen), (e) manuelle Buchungen (vorhanden).

### 🟡 B7 — Threshold-Walk filtert nicht (mittel)

`build_threshold_candidate` summiert alle `sales_gross_cents` inkl. Return-artiger Orders
[tax_reporting.py:145-168]; Aufruf übergibt ungefiltert alle Orders [orders.py:1131-1158].
Anforderung des Betreibers war „nach den gecancelten Orders". Konsistenz herstellen (gleiche
Zeroing-Regel wie Analytics/Buchhaltung), dann ergibt sich der Stichtag automatisch korrekt.

### 🟡 B8 — Kosmetik/Kleinigkeiten (niedrig)

- Kaufland `line_items_count` zählt stornierte Units [db.py:512-514].
- Amazon `_order_summary` Fallback-Kette `financial_sales or order_total or item_sales`
  kann bei stornierten Orders ohne Finance-Events Item-Umsatz zeigen [amazon_fba.py:111].
- `annotate_order_tax_fields` clamped `sales_net`/`purchase_effective_cost` stillschweigend
  auf ≥0 — verschleiert negative Datenprobleme [tax_reporting.py:54-63].

## 5. Gemessene Ist-Zahlen (combined.sqlite3 / bookkeeping / amazon_fba, Stand 21.08.2026)

**Combined Orders nach Marketplace × Status:**

| Marketplace | Status | n | Umsatz € | Einkauf € | VAT € |
|---|---|---|---|---|---|
| amazon | released | 23 | 2.489,88 | 0,00 | 242,81 |
| amazon | deferred | 25 | 1.890,70 | 0,00 | 187,13 |
| amazon | pending | 6 | 0,00 | 0,00 | 0,00 |
| kaufland | – | 604 | 97.258,75 | 17.953,00 | **1.365.415,75 (Bug B1)** |
| shopify | paid | 188 | 42.234,73 | 5.977,41 | 0,00 |
| shopify | refunded | 15 | 2.823,58 (Bug B2) | 0,00 | 0,00 |
| shopify | partially_refunded | 1 | 159,90 | 0,00 | 0,00 |

**Monatsverlauf 2026 (rev/purchase):** Jan 24.417/0 · Feb 15.797/1.124 · Mär 11.540/8.036 ·
Apr 18.374/13.257 · Mai 17.437/1.513 · Jun 13.561/0 · Jul 17.781/0 · Aug 11.336/0.
Auffällig: ab Juni keine Einkaufskosten mehr erfasst (Enrichment-Pflege?) — für eine
Gewinn-/Steuerbetrachtung nach der Umstellung dringend zu schließen.

**Threshold 2026:** Überschreitung lt. aktueller Codelogik 29.06.2026 (100.082,04 €);
ohne Return-artige Orders 30.06.2026 (100.004,07 €).

**Amazon Finance Events (modern):** released 28 (Sales 2.489,88 / Fees 465,08),
deferred 29 (2.231,17 / 360,11), pending 22 (2.149,41 / 428,78); 25 Lifecycles mit mehreren
Raw-Events (Dedup greift); 497 Fee-Komponenten vorhanden.

**Buchhaltungs-DB:** 1.635 Transaktionen (SALE 139.653,38 € IN · COGS 22.949,94 € OUT ·
FEE 18.286,81 € OUT · SUBSCRIPTION 83,95 € · EXPENSE 12,34 €); VAT-Felder fast komplett leer
(s. B6-Kontext); `monthly_invoices` nur google_ads Nov 2025–Feb 2026, alle `mismatch`, VAT 0.

**Kaufland-Quelle:** 502 offene Units (94.715,00 €) vs. 106 stornierte (19.430,34 €, korrekt
exkludiert); `order_unit_refunds` leer (0 Rows).

## 6. Steuerrechtliche Einordnung (Hinweise, keine Rechtsberatung)

- **Modell passt grundsätzlich zu §19 UStG n.F. (seit 2025):** Grenzen 25.000 € (Vorjahr) /
  100.000 € (laufendes Jahr); Überschreiten im Jahreslauf ⇒ USt entsteht ab dem überschreitenden
  Auftrag. Ein einzelner Stichtag via `vat_effective_from` + `is_vat_applicable_for_order`
  [tax_reporting.py:42-48] ist eine brauchbare Approximation.
- **Aber:** (a) Die Grenze wird brutto gerechnet — vertretbar („Umsätze zzgl. Steuer"), aber mit
  Steuerberater bestätigen; (b) der Walk schließt Storni nicht aus (B7); (c) der Fall
  „Vorjahr > 25.000 € ⇒ ab 01.01. pflichtig" fehlt im Modell (hier 2025 vermutlich unkritisch,
  da Shop-Start erst Ende Nov 2025).
- **AliExpress:** keine Rechnung mit ausgewiesener USt ⇒ **kein Vorsteuerabzug**; Kaufpreis =
  Vollkosten. Default `purchase_is_vat_deductible=0` ist hier korrekt. Für EU-Einkäufe mit
  ausgewiesener USt: über Enrichment `purchase_cost_cents` (brutto lt. Rechnung) +
  `purchase_vat_cents` + Deductible-Flag pflegen — Felder vorhanden.
- **Marktplatz-Gebühren & Ads:** ausgewiesene USt = ziehbare Vorsteuer; aktuell nur der
  manuelle Weg wirksam (praktisch ungenutzt) — siehe B6.
- **Vorsteuerabzug allgemein:** setzt ordnungsgemäße Rechnung (§14 UStG) voraus; das
  Beleg-/Document-Modell (uploads, document_id an COGS/EXPENSE) ist dafür die richtige Basis.

## 7. Priorisierte Maßnahmenliste

**P0 — Datenintegrität (vor jedem Stichtag!)**
1. B1 fixen (Kaufland-VAT ×100), danach `populate_combined_orders()` neu laufen lassen.
2. B2 fixen (Shopify refunded/voided → 0 bzw. netto abzgl. Refund) — bestimmt den finalen
   Stichtag (29.06. vs. 30.06.2026).
3. B4 fixen (Refund-Währung guarden).
4. Danach: `seller_profiles.tax_mode='regular'` setzen und `vat_effective_from` auf die
   bestätigte Auftragszeit des Überschreitens setzen (aktuell bewusst offen gelassen).

**P1 — USt-Report vollständig machen**
5. B6: Amazon-Fee-VAT (liegt schon strukturiert vor) in den Report einspeisen; Kaufland-
   Gebührenrechnungen und Google-Ads-VAT erfassen (Sammelrechnungen mit `vat_amount_cents`
   pflegen oder automatisch ableiten).
6. B3 klären: Shopify-USt-Ausweisung aktivieren ODER dokumentierte Brutto-Herleitung im Report.
7. B7: Threshold-Konsistenz (Storni exkludieren) + optional „Vorjahr > 25k"-Prüfung als Warnung.
8. B5: Amazon-Einkaufsstrecke befüllen (FIFO/Lieferantenrechnungen) oder Alternative definieren;
   `purchase_is_vat_deductible` aus Daten ableiten statt hardcodiert.

**P2 — Konsistenz & Absicherung**
9. Kaufpreis-Behandlung bei Retouren festlegen (nullen? Ware weg? Supplier-Refund als eigener
   Transaktionstyp `REFUND`/IN vom Lieferanten) — Analytics ↔ Buchhaltung angleichen.
10. Regressionstests: Threshold-Walk, `build_vat_report` mit Storni/Partial-Refunds/FX,
    `to_kaufland_cents`-Semantik, Sync-Zeroing. Aktuell deckt nur `test_amazon_fba.py`
    VAT-Themen streiflichtartig ab.

## 8. Validierungskommandos

```bash
# Read-only DB-Inspektion (immer mode=ro!)
sqlite3 "file:data/combined.sqlite3?mode=ro" \
  "SELECT marketplace, SUM(sales_gross_cents)/100.0, SUM(sales_vat_cents)/100.0, SUM(sales_net_cents)/100.0 FROM combined_orders GROUP BY marketplace"

# Repro B1 (pure function):
cd ecommerce-dashboard && python3 -c "
from app.services.order_summaries import to_kaufland_cents
print(to_kaufland_cents(1419.4117647058824))  # -> 141941 (BUG, erwartet 1419)"

# Test suite (read-only bzgl. Prod-Daten, nutzt Fixtures):
pytest ecommerce-dashboard/tests -q
```

## 9. Offene Rückfragen an den Betreiber

1. Stichtag: 29.06./30.06.2026 — welcher Auftrag ist laut Unterlagen der überschreitende?
2. Shopify: sollen neue Aufträge USt ausweisen (Shopify-Tax aktivieren) oder bleibt der Kanal
   bis zur Konfiguration ohne Ausweis (dann Herleitung im Report)?
3. Werden die Einkäufe ab Juni 2026 (Enrichment leer) nachgetragen, oder fehlt eine Quelle?
4. Amazon-FBA-Einkäufe: soll die beschaffungsstrecke (procurement batches + supplier invoices)
   genutzt werden? Dann Eingangsrechnungen mit Brutto/Netto/VAT je Zeile erfassen.
5. Google Ads / Kaufland-Gebühren: liegen dir die Sammelrechnungen mit USt-Ausweis vor, damit
   die Vorsteuer gepflegt werden kann?

---

## Addendum (21.08.2026): Unabhängige Verifikation + Detail-Fixplan B1–B3

### B1 Kaufland-VAT ×100 — VERIFIZIERT (459/461 exakt ×100)

Root-Cause-Kette: Importer speichert Unit-Preise als Cent-TEXT („32990"), vat=19.0 REAL →
SQL-Aggregat `SUM(price*vat/(100+vat))` [db.py:458-467 **und dupliziert** orders.py:~186/~970]
liefert nicht-ganzzahlige CENT-Beträge → `to_kaufland_cents()` [order_summaries.py:71-104]
interpretiert Nicht-Ganzzahlen als EURO (×100). Manuelle Nachrechnung aller 461 Orders aus
Roh-Units: 459× Ratio exakt 100.0, 2× Ratio 1.0 (ganzzahliger Zufall).

Impact-Grenzen: NICHT betroffen sind Analytics-Umsatz, Buchhaltungs-Sync und Verkaufsrechnungen
(invoices.py:705-708 rechnet pro Unit aus Rohdaten). Betroffen: combined_orders vat/net,
USt-Report, Dashboard-Nettospalten. Eingeführt mit Commit 57d0019.

Fix: (1) ROUND() um beide VAT-Aggregate in db.py UND orders.py (4 SQL-Stellen),
(2) Aggregate in kaufland_summary_from_row nicht mehr durch die Heuristik schicken,
(3) Regressionstest mit nicht-ganzzahliger Summe, (4) populate_combined_orders() neu.

### B2 Shopify refunded zählen voll — VERIFIZIERT (15 Orders, alle echt voll erstattet)

financial_status kommt unverändert aus der API (raw_json-Crosscheck 204/204 konsistent).
12/15 Refund-TX-Summen == Auftragstotal; #1200/#1203/#1204 ebenfalls voll erstattet, aber in
SEK (zugleich Beleg für B4). Threshold: wie codiert 29.06.2026, exklusive return-like
30.06.2026. Nach B3-Fix würden die 15 Orders ~476 € Phantom-USt erzeugen → B2 zuerst.

Fix: (1) shopify_summary_from_row: refunded/voided/cancel → alles 0 (partial bleibt Abzug),
(2) Defense-in-depth: build_threshold_candidate + build_vat_report normalisieren selbst,
(3) Fixtures refunded/voided/partial, (4) Rebuild. Limitierung dokumentieren: behaltene
Shopify-Gebühren bei Erstattung fallen unter den Tisch (wie im Buchhaltungssync bisher).

### B3 Shopify total_tax=0 — REKLASSIFIZIERT: kein Code-Bug, Konfigurations-/Design-Gap

Alle 204 Orders: Spalte=0 UND raw_json total_tax="0.00" UND tax_lines=[] UND taxes_included=true.
Shopify hat nie Steuer geliefert (Store ohne Steuerraten konfiguriert, Kleinunternehmer-Setup);
der Importer verliert nichts. Kritisch bleibt es trotzdem: ohne Aktion 0 € Output-USt auf dem
Shopify-Kanal (~42 k € Umsatz ⇒ ~6,7 k € USt p.a. unsichtbar).

Fix: (A) Betreiber: Shopify-Steuern (19 % DE, Preise inklusiv) ab Stichtag aktivieren — der
Ingestion-Pfad für total_tax ist verifiziert intakt. (B) System-Fallback: bei vat_applicable &&
gross>0 && vat==0 → hergeleitete USt (gross − gross/1,19) MIT explizitem Flag
(`sales_vat_source='derived'`) und Warning-Liste im Report; niemals still ableiten.
Altorders vor Stichtag bleiben vat_applicable=false (als KU korrekt).

### Ausführungsreihenfolge

B1 + B2 (Code) → Rebuild combined_orders → Stichtag final bestätigen → B3 (Shopify-Config +
derived-Fallback) → seller_profiles auf regular/vat_effective_from setzen.

---

## Addendum 2 (21.08.2026): Implementierung B1 + B2 + Jahresgrenzen-Fix

**Umgesetzt (Code):**
- `order_summaries.py`: neuer deterministischer Konverter `_cents_from_real_aggregate()`
  (round half away from zero, SQLite-ROUND-kompatibel); `kaufland_summary_from_row` nutzt ihn
  für alle 6 SQL-Geld-Aggregate → B1 behoben an einer zentralen Stelle (deckt db.py-Populate,
  Orders-Liste und Order-Detail ab, da alle durch dieselbe Funktion laufen).
- `order_summaries.py`: `_is_fully_return_like_status()` + Zeroing in
  `shopify_summary_from_row` — refunded/voided/cancelled ⇒ total/gross/vat/net/fees/
  after_fees/shipping = 0; Status bleibt für die UI sichtbar; partially_refunded unverändert.
- `tax_reporting.py`: `is_fully_return_like_order()` als eigener Guard in
  `build_threshold_candidate` (Skip) und `build_vat_report` (Skip + Warnung) — Defense-in-depth,
  konsistent mit bookings/analytics.
- **Neuer Bug B10 gefunden & gefixt:** `build_threshold_candidate` summierte jahresübergreifend;
  Nov/Dez-2025-Umsatz (16.635 €) floss in die 100k-Grenze 2026 ein. §19 UStG gilt pro
  Kalenderjahr → Kumulation resettet jetzt per Jahr (`year` im Candidate ergänzt).

**Tests:** `tests/test_accounting_fixes.py` mit 12 Regressionstests (VAT-Inflation, Integer-
Aggregate, Refund-Skalierung Kaufland; Shopify refunded/voided/cancelled/partial/paid;
Threshold-Guard inkl. Jahresreset; Guard-Semantik). Gesamtsuite: **136 passed**.

**Rebuild + Gegenprobe (combined.sqlite3, Backup: /tmp/opencode/combined.pre_fix_backup.sqlite3):**
- Kaufland VAT: 1.365.415,75 € → **13.732,98 €** (plausibel lt. Einzelnachrechnung; Abweichung
  von gross/1,19≈15,5 k € erklärt sich durch vat=0-Units und proportionale Refund-Kürzung);
  `sales_net=0`-Fälle: 459 → **0**.
- Shopify refunded: 15 Orders, Leck **0,00 €** (vorher 2.983,48 €).
- Threshold 2026: **30.06.2026 17:21 UTC**, Kaufland MXZ3G75, kumuliert 100.024,07 €.
- Guard-Wirkung aktuell rein präventiv: alle 127 geskippten Orders tragen 0 € (kaufland 106
  cancelled, shopify 15 refunded, amazon 6 Canceled via raw_status erkannt).

**Offen (Betreiber):** `seller_profiles` auf `tax_mode='regular'` + `vat_effective_from`
2026-06-30 setzen (bewusst dem Betreiber vorbehalten). Buchhaltungs-DB unverändert betroffen:
Sync hatte voll erstattete Orders bereits gelöscht; kein Re-Mirror nötig.

---

## Addendum 3 (21.08.2026): Unabhängige Verifikation B4–B6

### B4 Refund-Währung — CONFIRMED, aktuell durch B2 ohne falschen Betrag

Subagent und unabhängige Rohdatenprüfung bestätigen die Fehlerkette:

1. Shopify speichert beim Multi-Currency-Checkout auf der Order `currency='EUR'` und
   `presentment_currency='SEK'`.
2. Die eingebetteten `refunds[].transactions[]` enthalten dann z.B. `amount='2183.00'`,
   `currency='SEK'`, während `amount_set` leer ist.
3. `shopify_live.py:820-844` speichert die Refund-Transaktionen unverändert in
   `transactions_json`.
4. `db.py:421-429` sowie die Duplikate in `orders.py:142-150` und `orders.py:875-885`
   summieren nur `amount` mit `kind='refund'` und `status='success'`, aber prüfen die
   Währung nicht.
5. `shopify_summary_from_row` interpretiert die Summe anschließend als EUR.

Rohdatenbefund: genau drei EUR-Orders enthalten erfolgreiche SEK-Refunds:

| Order | Orderwert EUR | Refund laut JSON | Faktor |
|---|---:|---:|---:|
| #1200 | 197,87 | 2.183,00 SEK | 11,03 |
| #1203 | 187,69 | 2.071,00 SEK | 11,03 |
| #1204 | 320,23 | 3.558,00 SEK | 11,11 |

Die einzige `partially_refunded`-Order #1108 hat hingegen eine korrekte EUR-Erstattung von
10,00 €. Die drei SEK-Fälle sind aktuell vollständig `refunded` und werden durch den B2-Fix
komplett genullt; der aktuelle falsche Geldbetrag ist daher **0 €**. Der Bug bleibt eine
produktive Landmine für die nächste Teil-Erstattung in Fremdwährung.

Sicherheitsfix: Währung in allen drei SQL-Kopien gegen `orders.currency` prüfen. Sauberer,
aber aufwendiger wäre die Normalisierung auf Shop-Money im Importer; bei `amount_set=null`
ist dafür die Ratio aus `total_price_set.shop_money` und `presentment_money` nötig. Die
gleichartige fehlende Währungsprüfung der `fee_total`/`net_total`-Subqueries sollte ebenfalls
defensiv geprüft werden.

### B5 Amazon-COGS — CONFIRMED, mit Transportkosten-Nuance

Read-only-Zählung in `amazon_fba.sqlite3`:

| Tabelle | Rows |
|---|---:|
| `procurement_batches` / `_lines` | 0 / 0 |
| `supplier_invoices` / `_lines` | 0 / 0 |
| `inventory_lots` / `fifo_allocations` | 0 / 0 |
| `amazon_inbound_invoices` / `_lines` | 0 / 0 |
| `amazon_inbound_cost_allocations` | 0 |
| `amazon_inbound_costs` | 6 |

Die 6 vorhandenen `amazon_inbound_costs` sind ausschließlich FBA-Transportgebühren:
44,00 € `FBAInboundTransportationFee` + 3,25 € Program Fee, mehrfach als
`superseded`/`actual`; sie sind keine Produkt-Einkaufskosten. 48 Orders haben positive
Amazon-Finance-Sales (8.321,91 €), aber FIFO-COGS und `combined_orders.purchase_cost_cents`
bleiben 0 €.

Die Beschaffungsstrecke ist backendseitig vorhanden (`amazon_fba.py:797-1206`), aber die
klassische Batch/Invoice/Lot/FIFO-Kette ist nur über Admin-API-Endpunkte in
`routers/amazon.py:360-393` erreichbar. Im Frontend existieren keine Calls für
`/procurement/*` oder `/fifo`; `allocate_order_fifo` wird weder von Sync noch Auto-Refresh
automatisch aufgerufen. Manuelles Amazon-Enrichment ist absichtlich blockiert
(`routers/orders.py:208-209`), und der allgemeine Buchhaltungs-Sync überspringt Amazon
(`bookings.py:702-706`). Damit gibt es keinen alternativen COGS-Pfad.

`purchase_is_vat_deductible=False` ist unbedingt hartcodiert in
`amazon_fba.py:144-145`, `db.py:574-577` und `orders.py:280`. Die Supplier-Invoice-Daten
enthalten zwar gross/net/VAT und im alten Pfad `input_vat_status`, der aktive Shipment-Pfad
hat aber kein eigenes `input_vat_status`; die Ableitung muss daher datenbasiert aus einer
geprüften Rechnung erfolgen. Der FIFO-Lot selbst soll gemäß Design-Doc den **Netto**-Kostenwert
führen.

### B6 Gebühren-Vorsteuer — CONFIRMED, Quellen unterscheiden

`build_vat_report` (tax_reporting.py:218, Summenbildung :273-275) kennt nur:

1. Order-Purchase-VAT,
2. `_load_monthly_fee_vat` aus `monthly_invoices` (tax_reporting.py:95-115),
3. manuelle `transactions` mit `direction='OUT'`, `is_vat_deductible=1` und VAT > 0
   (tax_reporting.py:118-142).

**Amazon:** `extract_modern_financial_breakdown` parst Base/Tax korrekt
(`amazon_sp_api.py:124-186`), und `get_amazon_finance_overview` stellt
`fees_vat_cents` bereit (`amazon_fba.py:1060-1073`). Es gibt aber keinen Import in
`tax_reporting.py` und keinen automatischen Write in `monthly_invoices`/`transactions`.
Unabhängige Nachrechnung aus allen Raw Events:

| Posted-Monat | Amazon-Fee-VAT |
|---|---:|
| 2026-07 | 57,74 € |
| 2026-08 | 139,49 € |

72 ModernTransaction-Events enthalten Fees mit echten Base/Tax-Splits; diese Vorsteuer ist
damit bereits im System vorhanden, aber im USt-Report unsichtbar.

**Kaufland:** `fees_cents = units_price_sum - revenue_gross_sum` ist nur ein Residuum ohne
Fee-VAT-Split (`order_summaries.py:429-435`). Es gibt keinen Import von Kaufland-
Abrechnungs-/Rechnungsdokumenten. Die Monatsabrechnung muss derzeit als `monthly_invoices`
mit Provider `kaufland`, Brutto und ausgewiesener VAT erfasst werden; der vorhandene Loader
würde sie dann automatisch berücksichtigen.

**Google Ads:** Der CSV-Importer (`google_ads.py:136-339`) liest keine Steuerfelder. Die vier
vorhandenen `google_ads`-Sammelrechnungen haben VAT=0 und `mismatch`. Das kann falsch sein,
falls deutsche USt ausgewiesen ist, aber korrekt bei Reverse Charge (§13b UStG). Rechnung
zuerst prüfen; die UI und API akzeptieren `vat_amount_cents` bereits.

Minimaler Amazon-Code-Fix: monatlicher Loader in `tax_reporting.py`, der den vorhandenen
Canonical-Lifecycle-Predicate nutzt, nur EUR und den zutreffenden `posted_date`-Monat
berücksichtigt, Fee-VAT summiert und als separaten Reportposten ausgibt. Für Kaufland und
Google Ads ist zunächst Beleg-/Steuerbehandlung zu klären; kein blindes Ableiten aus dem
Gebührenresiduum.
