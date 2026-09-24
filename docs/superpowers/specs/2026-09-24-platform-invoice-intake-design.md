# Design: Eingangsrechnungen — Upload, Parsen, Abgleich, Vorsteuer

Stand: 2026-09-24 · Status: freigegeben · Baut auf: `2026-08-21-amazon-financial-lifecycle-and-fee-accounting-design.md`

## 1. Problem

Jeden Monatsende kommen Rechnungen rein, die Gebühren abdecken, die vorher schon
teilweise automatisch erfasst wurden: Kaufland-Sammelrechnungen, Amazon-Gebühren,
separat Kaufland-Einzelbelege für Storno-Gebühren. Dazu irgendwann Wareneinkauf
(AliExpress).

Heute gibt es dafür **zwei getrennte Systeme** mit drei verschiedenen Quellen für
die Vorsteuer. Das Ergebnis ist, dass der USt-Report nicht dasselbe zeigt wie die
Buchungen. Ziel: eine Quelle, ein Upload-Pfad, ein Abgleich.

## 2. Ausgangslage (geprüft an der Live-Instanz)

Produktivsystem `192.168.178.197:8012`, Stand älter als Dev:

| Was | Befund |
|---|---|
| `GET /api/ust-report/documents` | **404** — USt-Report dort nie deployed |
| `GET /api/bookings/documents` | 200 — **321 Belege**, darunter die 10 Kaufland-Rechnungen (`R0226`…`R0826`, `C0326`, `C0726`) |
| `GET /api/bookings/monthly-invoices` | 200 — **4 Sammelrechnungen** (Google Ads, Nov 25 – Feb 26) mit `calculated_sum_cents`, `difference_cents`, Status `mismatch` |
| `vat_amount_cents` | bei allen Sammelrechnungen **0** — die Vorsteuer fehlt überall |

Zwei Schlussfolgerungen daraus:

1. Der Abgleich-Mechanismus (`calculated_sum_cents` vs. `invoice_amount_cents`)
   **existiert im Produktivsystem und funktioniert**. Wir erweitern ihn, statt ihn
   neu zu erfinden.
2. `input_vat_invoices` gehört zum nie-deployten USt-Report-Neubau und existiert
   live **nicht**. Es wird **gar nicht erst angelegt**.

## 3. Harte Regel: nur ergänzen, nie ersetzen

Diese Regel ist bindend für jeden Teil dieser Arbeit und steht über allen anderen
Entscheidungen hier:

- Migration ausschließlich über `CREATE TABLE IF NOT EXISTS` und `_ensure_column()`
  (Projektstandard aus `AGENTS.md`). **Kein** `DROP`, `DELETE`, `RENAME`, kein
  Überschreiben bestehender Zeilen.
- Bestehende `documents` und `transactions` bleiben unangetastet. Neue Daten hängen
  sich an (`document_id`-Verknüpfung), sie ersetzen nichts.
- Alte Backups bleiben ladbar. Fehlende Spalten in alten Ständen sind `NULL` und
  werden vom Lesefluss toleriert. Umgekehrt gilt: eine neuere Datenbank in eine
  ältere Version zu laden ist wie bei jedem Schema-Zuwachs nicht möglich.
- **`input_vat_invoices` / `input_vat_invoice_lines` werden nicht angelegt.** Die im
  Dev-Stand vorhandenen 10 Zeilen sind Test-/Arbeitsdaten ohne Produktivbezug und
  werden beim Umbau verworfen (Dev-DB, kein Backup-Fall).
- Bis zur geplanten Versionsübergabe wird **kein Kommando gegen
  `192.168.178.197:8012` gerichtet**. Entwicklung läuft ausschließlich lokal.

## 4. Ziele

1. Ein Upload-Pfad für alle Eingangsbelege (Plattformgebühren heute, Wareneinkauf
   anschließend im selben Modul).
2. Automatisches Auslesen der Belege mit **Vorbefüllung**; nichts wird gebucht, bevor
   es freigegeben wurde.
3. Abgleich der Rechnung gegen das, was automatisch gebucht wurde; Abweichungen
   werden übernommen, aber als markierte Korrekturbuchung mit separater Freigabe.
4. Der USt-Report wird zum Spiegel der Buchungen — er erzeugt keine eigene Wahrheit
   mehr.
5. Parsing-Regeln werden dokumentiert, damit ein KI-Agent denselben Stand hat.

## 5. Nicht-Ziele

- VIDR (`GET_VAT_TRANSACTION_DATA`), OSS-Verwaltung, Schwellenmonitor (bewusst
  außen vor, unverändert).
- Shopify (inaktiv).
- Automatische Steuerklassifikation ohne Beleg/Freigabe.
- Umstellung des Kundenrechnungs-Tab (`/invoices`) — der bleibt wie er ist.
- Amazon-Kundenbelege (Retail-Rechnungen/Gutschriften) werden **erfasst**, aber nicht
  in die Gebühren-Logik gezwungen; sie sind Umsatzbelege, keine Eingangsrechnungen.

## 6. Datenmodell (rein additiv)

### 6.1 `monthly_invoices` — die Eingangsrechnung

Bestehende Spalten bleiben unverändert (`provider`, `period_from`, `period_to`,
`invoice_amount_cents`, `vat_amount_cents`, `currency`, `calculated_sum_cents`,
`difference_cents`, `document_id`, `notes`, `status`, `created_at`, `updated_at`).

Neu per `_ensure_column()`:

| Spalte | Typ | Zweck |
|---|---|---|
| `invoice_number` | TEXT | `R0726-24282540`, `DE-AEU-2026-1781260`, `C0726-91224`. UNIQUE(provider, invoice_number) als Index, kein harzer Constraint auf alten Zeilen |
| `invoice_date` | TEXT | Rechnungsdatum (ISO) |
| `doc_kind` | TEXT | `consolidated` (Sammelrechnung) · `single` (Einzelbeleg) |
| `original_invoice_number` | TEXT | Gutschrift → Original (`DE-CN-…` → `DE-AEU-2026-1781262`) |
| `lines_json` | TEXT | Snapshot der geparsten Positionen (JSON-Array) |
| `parse_confidence` | REAL | 0–1, Selbstbewusstsein des Parsers |
| `needs_review_reasons` | TEXT | JSON-Array konkreter Warnungen |
| `fx_rate` | TEXT | Amazon-Abos laufen in GBP; hier der aufgedruckte Kurs |
| `vat_cents_eur` | INTEGER | nur die USt in EUR (entscheidend für die USt-VA bei GBP-Belegen) |

`lines_json` bewusst als Blob statt eigener Positionstabelle: die PDF-Positionen
sind ein **Abbild des Belegs**, nicht das Journal. Das Journal sind `transactions`.

Status-Werte (vorher `draft`/`matched`/`mismatch`):
`draft` → `needs_review` → `approved`; daneben weiter `matched` / `mismatch` aus dem
Abgleich. Alte Zeilen mit `draft` bleiben gültig.

### 6.2 `transactions` — das Journal

Schema unverändert. Neue Befüllung über `category` (die Spalte existiert bereits):

| Vorgang | `type` | `category` | USt |
|---|---|---|---|
| Verkaufsprovision je Bestellung | `FEE` | `provision` | 19 %, **neu gefüllt** (heute `NULL`) |
| Storno Provision | `FEE` | `provision` (negativ) | 19 % |
| Monatliche Grundgebühr | `SUBSCRIPTION` | `base_fee` | 19 % |
| Sponsored Product Ads | `FEE` | `advertising` | 19 % |
| Fee for cancelled orders (C-Beleg) | `EXPENSE` | `cancellation_fee` | **0 %**, `is_vat_deductible=0` |
| Abweichung Rechnung ↔ gebucht | `ADJUSTMENT` | `invoice_variance` | je Fall |

### 6.3 `documents`

Unverändert. Nimmt PDF **und** (bei Amazon) die begleitende CSV auf. Verlinkung über
bestehendes `document_id`.

## 7. Parsing-Schicht

Neuer Service `app/services/invoice_parser.py`. Liest einen Beleg und gibt
strukturierte Felder + Positionen + `confidence` + `needs_review_reasons` zurück.
Der ausführliche Regelkatalog steht in `docs/platform-invoice-parsing.md` (wird im
Rahmen dieser Arbeit geschrieben). Zusammenfassung der Templates:

### 7.1 `KAUFLAND_SAMMELRECHNUNG` (`R…`)

Erkennung: `Rechnungs Nr.` + Spaltenkopf `Netto (EUR) … Brutto (EUR)`.

Zeilenformat (durch `pdftotext -layout` stabil tabellarisch):
`DD.MM.YYYY  <Bezeichnung>  <Netto>  <MwSt>  19%  <Brutto>`

Positionskategorien über die Bezeichnung:

| Marker | `position_key` |
|---|---|
| `Provision zu Bestell-Nr.` | `provision` |
| `Storno Provision zu Bestell-Nr.` | `provision_storno` |
| `Monatliche Grundgebühr` | `base_fee` |
| `Sponsored Product Ads` | `advertising` |

Kopf: `Neckarsulm, DD.MM.YYYY` → `invoice_date`. Kontrollsumme: `Summe 19%  <net> <vat> <brutto>`.

**Geprüft gegen 8 echte Rechnungen / 777 Einzelpositionen: 8/8 Kontrollsummen exakt.**
Sonderfall: `Fees for cancelled orders` taucht **nicht** in `R…` auf, sondern in `C…`.

### 7.2 `KAUFLAND_EINZELBELEG` (`C…`)

Erkennung: `Abrechnungsbeleg Nr.` + Spaltenkopf `Zahlbetrag (EUR)`.
Genau eine Position, Kategorie `cancellation_fee`. Marker im Fuß:
`Es handelt sich um einen nicht steuerbaren Schadensersatz.` → `vat_rate = 0`,
`is_vat_deductible = 0`. Richtung: **Ausgabe** (Fußzeile „mit dem Saldo Ihres
Kreditorenkontos verrechnet" ist dieselbe Formulierung wie auf den `R…`-Rechnungen,
die eindeutig bezahlt werden).

### 7.3 `AMAZON_GEBUEHR` (`DE-AEU-…`, EUR, deutsch)

Erkennung: `Rechnungsnummer: DE-AEU-…` + `Rechnungszeitraum:`.
Kopf: `Rechnungsdatum: DD/MM/YYYY`, Zeitraum `… to …`.
Positionen aus der Tabelle `Dienstleistung | Preis (Ohne USt.) | USt.% | USt. | Gesamtsumme`.
**Zusätzlich die begleitende CSV bevorzugt einlesen** (`DE-AEU-….csv`, Spalten
`Transaction Date, Transaction ID, Order ID, Fees Invoice Number, Marketplace, Fee ID,
Total Fees (VAT-Inclusive)`) — sie liefert die Einzelpositionen zuverlässiger als das
PDF-Layout mit umbrechenden Beschreibungen.

### 7.4 `AMAZON_ABO` (`DE-AEU-…`, GBP, englisch)

Erkennung: `Invoice Number:` + `Invoice Period:` (englisch).
Beträge in **GBP** (`Selling on Amazon Fees GBP 25.00 19.00% GBP 4.75 GBP 29.75`).
Der **eine EUR-Betrag im Dokument ist die USt in EUR** (`EUR 5.51`) — nur der ist für
die USt-VA relevant. `Exchange Rate: [1.160390 EUR / 1 GBP]` → `fx_rate`.

### 7.5 `AMAZON_STEUERGUTSCHRIFT` (`DE-CN-…`)

Erkennung: `STEUERGUTSCHRIFT` + `Gutschriftennummer:`.
Position `Erstattung von Verkäufergebühren` (negativ), 19 %.
`Ursprüngliche Rechnungsnummer` → `original_invoice_number` (Verknüpfung zwingend,
sonst `needs_review`).

### 7.6 `AMAZON_RETAIL_GUTSCHRIFT` (`DE-…-…-N`)

Erkennung: `GUTSCHRIFT` + Spaltenkopf `Bestellnummer | Lieferdatum | Menge | ASIN`.
**Umsatzbeleg, keine Eingangsrechnung** — wird erfasst und gekennzeichnet, fließt
nicht in die Vorsteuer. Wird der Reihe nach als eigener `doc_kind` geführt.

### 7.7 Selbstvalidierung (Schutz gegen „wackelig")

Jedes Template hat eine Kontrollsumme. Der Parser vergleicht
`sum(lines.net + lines.vat)` gegen die ausgewiesene Rechnungssumme.

- Abweichung > 2 Cent → `parse_confidence` sinkt, `needs_review_reasons` enthält
  `"kontrollsumme_verfehlt"`, Status `needs_review`.
- Positionen ohne erkannte Kategorie → `"unbekannte_position"`, einzeln gelistet.
- Kein Template erkannt → `doc_kind = "unknown"`, `category = "other"`,
  `parse_confidence = 0`, Status `needs_review`, **es wird nichts gebucht** — der
  Beleg liegt dann nur als Datei mit Warnung vor und muss manuell erfasst werden.

Der Parser weiß also selbst, wann er versagt. Das ist der eigentliche Schutz gegen
Wackeligkeit — nicht die Trefferquote.

## 8. Buchungsquellen

| Position | Quelle | Zeitpunkt |
|---|---|---|
| Verkaufsprovision inkl. Storno | `order_units`: `price − revenue_gross` (Brutto), `netto = round(brutto/1.19)`, Vorzeichen aus `status = 'cancelled'` | laufend, beim Order-Sync |
| Grundgebühr | Rechnungsimport | beim Upload |
| Sponsored Ads | Rechnungsimport | beim Upload |
| Fees for cancelled orders | Rechnungsimport | beim Upload |

**Begründung für Provision aus `order_units`:** gegen 6 echte Fälle geprüft —
`price − revenue_gross` stimmt centgenau mit dem PDF-Bruttobetrag überein
(8,15 / 13,76 / 23,19 / 10,12 / 8,57 / 18,09). Damit ist die Provision vor dem Upload
bekannt und dient als Erwartungswert im Abgleich.

Kaufland stellt **keine** Gebühren-API bereit (geprüft gegen die komplette
Marketplace-Seller-API-Doku, `sellerapi.kaufland.com`): `Commission Rates` liefert
ausdrücklich nur ein *estimate* („the commission actually charged is determined at
invoice time and may differ"), `Reports` liefert Umsatzreports. Weder Grundgebühr
noch Werbekosten sind abfragbar. Deshalb: Provision automatisch, Rest beim Import.

## 9. Abgleich, Korrektur, Freigabe

### 9.1 Ablauf

1. Upload (UI oder `POST /api/platform-invoices`, PDF + optional CSV).
2. Parser läuft, liefert Vorbefüllung + `confidence` + Warnungen.
3. Vorschau-Abgleich: `calculated_sum_cents` = Summe der im Zeitraum bereits
   gebuchten Provisionen (plus ggf. schon gebuchter Grundgebühr/Ads) gegen
   `invoice_amount_cents` aus dem Beleg.
4. **Freigabe nötig.** Ohne Freigabe passiert nichts — kein Statuswechsel, keine
   Buchung.
5. Bei Freigabe entstehen die Buchungen aus Abschnitt 6.2.

### 9.2 Umgang mit Abweichungen (freigegebene Entscheidung)

Ergebnis vorab: **am Ende gilt der Rechnungsbetrag.** Der Weg dorthin ist bewusst
nicht „überschreiben", sondern „ausgleichen":

- Die automatisch gebuchten Provision-Transaktionen werden **nicht verändert** — sie
  behalten ihren Order-Bezug und bleiben nachvollziehbar.
- Die Differenz zwischen gebuchter Summe und Rechnungssumme wird als **eigene
  `ADJUSTMENT`-Transaktion** gebucht, `category = invoice_variance`,
  `reference = "Abweichung Sammelrechnung <Nr.>"`, `notes` nennt PDF-Wert und
  gebuchten Wert.
- Diese Korrekturbuchung hat **ihr eigenes Freigabe-Gate** und erscheint als Warnung,
  nicht als „passt".
- Danach ist `sum(gebuchte Transaktionen im Zeitraum) == invoice_amount_cents`, und
  zugleich ist sichtbar, dass und um wie viel vorher abgewichen wurde.

Keine bestehende Transaktion wird also jemals überschrieben — das ist die Umsetzung
von Abschnitt 3 für den Abgleichsfall.

## 10. UI

Neuer bzw. umgebauter Bereich unter **Buchungen → Transaktionen → Rechnungen**
(heute „Sammelrechnungen"). Ein Upload, eine Liste, für Plattformgebühren und später
Wareneinkauf.

| Element | Verhalten |
|---|---|
| Upload | `sammel-file-*` (Design-Langugage!), PDF + optionale CSV |
| Formular | vorausgefüllt aus dem Parser; alle Felder editierbar |
| Positionstabelle | je Positionstyp Netto / USt / Brutto / Anzahl |
| Abgleichskarte | gebucht vs. Rechnung, Differenz, ggf. Warnung |
| Aktion | `Freigeben` (primär) — bis dahin Status `needs_review` |
| Status | `Entwurf` · `Zu prüfen` · `Freigegeben` · `Stimmt` / `Abweichung` |
| KI-Agent | `POST /api/platform-invoices` → Prefill-Response, `PATCH …/approve` |

Kein Raw-`<input type="file">` (Design-Langugage). Layout nach
`frontend/DESIGN-LANGUAGE.md` (Kacheln, `btn-inline`, `detail-card`, `kpi-grid`).

**Tab-Aufräumung (ohne Löschen):** die Sub-Reiter `templates` und `accounts` werden
aus der Buchungs-Sub-Navigation **entfernt**. Ihre Routen, API-Endpunkte und Tabellen
bleiben unverändert bestehen und sind weiter direkt über die API erreichbar — es
verschwindet nur die Menüeinblendung. Ein erneutes Anblenden ist jederzeit ein
reiner Frontend-Eingriff.

**USt-Report:** der Tab „Eingangsrechnungen" entfällt. Seine Inhalte stehen unter
Buchungen. Der Report selbst liest künftig `transactions` + `monthly_invoices` und
zeigt Zahllast, Ausgangs-USt, Vorsteuer — als Spiegel, ohne eigene Belegverwaltung.

## 11. Tests

- Parser: je Template ein Test mit dem echten Beleg-Text (Fixture aus den vorhandenen
  PDFs abgeleitet, **keine echten Rechnungsnummern** — siehe `e2c57ce`). Enthält den
  Kontrollsummen-Test und die Fehlerfälle (kein Template, Kontrollsumme verfehlt,
  unbekannte Position).
- Provision: Regression gegen die 6 geprüften Fälle inkl. Storno-Vorzeichen.
- Abgleich: Übereinstimmung, Abweichung, Abweichung + Korrekturbuchung + Freigabe.
- Migrations-Test: alte Tabellenform laden, ohne neue Spalten — muss funktionieren.
- API: `POST /api/platform-invoices` (Prefill), `PATCH …/approve`, `GET`-Liste.

## 12. Deployment

Einmalige bewusste Versionsübergabe auf das Produktivsystem. Davor:

1. `scripts/dashboard-api/`, `docs/dashboard-backend-api.md` und
   `.opencode/skills/dashboard-backend-api/SKILL.md` im **selben Commit**
   aktualisieren (Projektregel bei API-Änderungen).
2. Konfigurationscheck vor dem Neustart (der bei der Versionsübergabe ohnehin
   erfolgt); Fehlkonfiguration darf nicht unbemerkt in den Betrieb gehen.
3. Expliziter Handgriff: Belege und Transaktionen des Produktivsystems vor dem
   Upgrade einmal exportieren (Sicherungskopie, nicht Teil dieser Arbeit).
4. Nach dem Upgrade: nur additive Spalten, 321 Belege und alle Transaktionen müssen
   unverändert zählbar sein (Verifikation: `GET /api/bookings/documents` → `total`
   weiterhin 321).

## 13. API (rückwärtskompatibel)

Die bestehenden Buchungs-Endpunkte bleiben **unverändert gültig** — das
Produktivsystem ruft sie auf. Neu kommt nur dazu:

| Endpunkt | Zweck |
|---|---|
| `POST /api/bookings/monthly-invoices/parse` | PDF (+CSV) hochladen, **ohne anzulegen** → Prefill, `confidence`, `needs_review_reasons`, Abgleich-Vorschau |
| `PATCH /api/bookings/monthly-invoices/{id}/approve` | Freigabe; legt die `transactions` an bzw. die Korrekturbuchung |
| `GET /api/bookings/monthly-invoices` | bleibt, um die neuen Felder erweitert |
| `POST /api/bookings/monthly-invoices` | bleibt, akzeptiert die neuen Felder **optional** (alte Clients funktionieren weiter) |

Die neuen Felder sind in allen Responses optional — ein Client, der sie nicht kennt,
liest sie stillschweigend nicht. Umgekehrt darf kein Endpunkt ein Feld verlangen,
das alte Clients nicht senden.

## 14. Bauabschnitte

1. Parser + Fixtures + `docs/platform-invoice-parsing.md` (ohne UI, ohne Buchung).
2. Additive Spalten + `parse`-Endpunkt + Freigabe/`approve` + Abgleich.
3. UI: Upload, Vorbefüllung, Positionstabelle, Abgleichskarte, Freigabe-Gate.
4. USt-Report stellt auf `transactions` + `monthly_invoices` um; Tab
   „Eingangsrechnungen" entfällt.
5. Tab-Aufräumung, API-Doku-Update, Verifikationsschritte für die Übergabe.

Jeder Abschnitt ist für sich lauffähig und testbar. Abschnitt 1 und 2 ändern nichts
am UI des Produktivsystems.

## 15. Offene Punkte

- **Wareneinkauf (AliExpress):** im Modul vorgesehen, aber nicht Teil des ersten
  Bauabschnitts. Gleicher Upload-Pfad, `doc_kind = "single"`, `category = "purchase"`.
  Der USt-Report-Behälter „Wareneinkauf" bleibt bestehen und wird dann gespeist.
- **Amazon-Retail-Belege** (`DE-855130503-2026-1` und Nachfolger): erfasst, aber als
  Umsatzbeleg gekennzeichnet. Offen bleibt die Klärung mit Kaufland/Amazon, warum die
  5 Juli-MCF-Orders als `SHIPMENT` gebucht sind, obwohl ein Beleg „Gutschrift" heißt.
- **Amazon-Gebührenrechnungen vor Juli 2026:** fehlen im Belegbestand (nur Abo Juni
  vorhanden). Vor der ersten USt-Abgabe gegenprüfen.
