# Parsing-Regeln für Eingangsrechnungen

Kurzfassung für Menschen und KI-Agenten: woran ein Beleg zu erkennen ist, was
daraus gelesen wird, und woran der Parser merkt, dass er unsicher ist.
Implementierung: `ecommerce-dashboard/app/services/invoice_parser.py`.
Tests: `ecommerce-dashboard/tests/test_invoice_parser.py`.

## Grundhaltung

Der Parser **darf nie stillschweigend etwas falsch machen**. Jedes Template hat
eine eingebaute Kontrollsumme; wird sie verfehlt, sinkt `parse_confidence` und die
Rechnung bleibt mit `needs_review_reasons` stehen. Gebucht wird erst nach
ausdrücklicher Freigabe.

## Erkennungsreihenfolge

Die Templates überlappen sich in ihren Kopfzeilen, deshalb in dieser Reihenfolge
prüfen (so implementiert in `detect_template`):

1. `Abrechnungsbeleg Nr.` → `KAUFLAND_EINZELBELEG`
2. `STEUERGUTSCHRIFT` oder `Gutschriftennummer:` → `AMAZON_STEUERGUTSCHRIFT`
3. `Rechnungs Nr.` **und** `Netto (EUR)` → `KAUFLAND_SAMMELRECHNUNG`
4. `Invoice Period:` **und** `Invoice Number:` → `AMAZON_ABO` (englisch, GBP)
5. `Rechnungsnummer:` **und** `Dienstleistung` → `AMAZON_GEBUEHR` (deutsch, EUR)
6. `GUTSCHRIFT` **und** `Bestellnummer` → `AMAZON_RETAIL_GUTSCHRIFT`
7. sonst → `UNKNOWN` (nichts wird gebucht)

Wichtig: `Rechnungszeitraum` ist **nicht** Teil der Template-Erkennung. Fehlt er,
bleibt das Template gültig und meldet ihn als `leistungszeitraum_fehlt`.

## Positionskategorien

Über den Bezeichner der Zeile (`classify_position`):

| Marker im Text | `position_key` | Herkunft |
|---|---|---|
| `Storno Provision zu Bestell-Nr.` | `provision_storno` | Kaufland |
| `Provision zu Bestell-Nr.` | `provision` | Kaufland |
| `Referral Fee` | `provision` | Amazon |
| `Monatliche Grundgebühr` | `base_fee` | Kaufland |
| `Sponsored Product Ads` / `Click Costs` | `advertising` | Kaufland |
| `Selling on Amazon Fees` / `Subscription` | `subscription` | Amazon (Abo) |
| `Pick & Pack` / `Inbound` / `Inventory Storage` | `fulfillment` | Amazon |
| `Refund Administration` | `refund_admin` | Amazon |
| `Erstattung` / `Credit` | `fee_refund` | Amazon-Gutschrift |
| `cancelled orders` | `cancellation_fee` | Kaufland-Einzelbeleg |
| alles andere | `other` → Warnung | — |

Reihenfolge zählt: `Storno Provision` vor `Provision`, `cancelled orders` vor
allem anderen (steht sonst im Weg).

## Template 1 — `KAUFLAND_SAMMELRECHNUNG` (`R…`)

Monatliche Sammelrechnung mit vielen Positionen.

- Kopf: `Rechnungs Nr.: R0726-24282540` · Datum in `Neckarsulm, DD.MM.JJJJ`
- Zeilenformat (nach `pdftotext -layout`, stabil tabellarisch):
  `DD.MM.JJJJ  <Bezeichnung>  <Netto>  <MwSt>  19%  <Brutto>`
  Zwischen Bezeichnung und Betrag stehen **zwei oder mehr** Leerzeichen.
  Fortsetzungszeilen mit der Bestellnummer tragen kein Datum und sind keine Position.
- Kontrollsumme: `Summe 19%  <netto>  <mwst>  <brutto>`
- Leistungszeitraum = frühestes bis spätestes Positionsdatum
- Beträge in deutscher Schreibweise (`1.403,37`)
- Steuer steht auf der Rechnung — **nicht** aus dem Betrag ableiten, sondern lesen

Bekannte Sonderformen: `Zwischensumme` je Seitenabschnitt (ignorieren), negative
`Storno Provision`-Zeilen.

## Template 2 — `KAUFLAND_EINZELBELEG` (`C…`)

Einzelbeleg mit genau einer Position, ohne Steuer.

- Kopf: `Abrechnungsbeleg Nr.: C0726-91224` · Datum in `Neckarsulm, DD.MM.JJJJ`
- Spaltenkopf: `Zahlbetrag (EUR)` — **nur ein Betrag**, kein Netto/USt/Brutto
- Zeile: `DD.MM.JJJJ  <Bezeichnung>  <Zahlbetrag>`
- Fuß: `Es handelt sich um einen nicht steuerbaren Schadensersatz.`
  → `vat_rate = None`, `is_vat_deductible = False`
- Kontrollsumme: `Summe  <betrag>` (ohne Doppelpunkt)

**Richtung: Ausgabe.** Die Fußzeile „mit dem Saldo Ihres Kreditorenkontos
verrechnet" klingt neutral, steht aber wörtlich genauso auf den `R…`-Rechnungen,
die eindeutig bezahlt werden. Beide Male zieht Kaufland vom Konto des Verkäufers ab.

## Template 3 — `AMAZON_GEBUEHR` (`DE-AEU-…`, EUR, deutsch)

- Kopf: `Rechnungsnummer: DE-AEU-2026-1781260` · `Rechnungsdatum: DD/MM/JJJJ`
  · `Rechnungszeitraum: DD/MM/JJJJ to DD/MM/JJJJ`
- Spaltenkopf `Dienstleistung` ist Teil der Erkennung.
- Positionen: `<Bezeichnung>   EUR <netto>   <satz>%   EUR <ust>   EUR <brutto>`
  **Die Beschriftung bricht mehrzeilig um.** Steht die Betragszeile allein, wird
  die davor stehende Zeile als Bezeichnung übernommen; der Nachlauf danach geht in
  die nächste Position. Dadurch kann die Kategorie ungenau werden — die CSV ist
  die sichere Quelle.
- Kontrollsumme: `Gesamtsumme  EUR <netto>  EUR <ust>  EUR <brutto>`
  **ohne Prozentsatz** — ein eigener Regex, sonst findet man nichts.

## Template 4 — `AMAZON_ABO` (`DE-AEU-…`, GBP, englisch)

- Kopf: `Invoice Number:` · `Invoice Date: DD/MM/JJJJ` · `Invoice Period: … to …`
- Eine Position: `Selling on Amazon Fees  GBP <netto>  <satz>%  GBP <ust>  GBP <brutto>`
- Kontrollsumme: `Total  GBP <netto>  GBP <ust>  GBP <brutto>` (ohne Satz)
- **Währung ist GBP.** Aufgedruckt ist zusätzlich nur die **Vorsteuer in EUR**
  (isolierte Zeile `EUR 5,51`) — nur die ist für die USt-Voranmeldung maßgeblich.
- `Exchange Rate: [1.160390 EUR / 1 GBP]` → `fx_rate`
- Datumsschreibweise Tag/Monat/Jahr wie in Template 3.

## Template 5 — `AMAZON_STEUERGUTSCHRIFT` (`DE-CN-…`)

- Kopf: `Gutschriftennummer:` · `Ausstellungsdatum der Gutschrift: DD/MM/JJJJ`
  · `Zeitraum der Gutschrift: … to …`
- Position: `Erstattung von Verkäufergebühren  -EUR <netto>  <satz>%  -EUR <ust>  -EUR <brutto>`
  Das Minus steht **vor** dem Währungssymbol.
- Kontrollsumme: `Gesamtsumme  -EUR …` (ohne Satz). Muss vor den Positionszeilen
  geprüft werden, sonst wandert sie als Position ins Ledger.
- `Ursprüngliche Rechnungsnummer` (mit Umlaut) → alle folgenden Rechnungsnummern
  in `original_invoice_numbers`; erster Bezug weiterhin `original_invoice_number`.
  Ohne Bezug ist die Gutschrift unvollständig → `originalrechnung_fehlt`.

## Template 6 — `AMAZON_RETAIL_GUTSCHRIFT` (`DE-…-…-N`)

**Umsatzbeleg, keine Eingangsrechnung.** Wird erfasst und als `doc_kind = "sales"`
gekennzeichnet, fließt aber nicht in die Vorsteuer.

- Kopf: `Rechnungsnummer: DE-855130503-2026-1` · `Rechnungsdatum: DD.MM.JJJJ`
- Spaltenkopf: `Bestellnummer | Lieferdatum | Menge | ASIN-Beschreibung`
- Die USt-Summenzeile hat **vier** Spalten:
  `EUR <zwischensumme> 19,00 EUR <ust> EUR <ust gesamt>`.
  Die vierte Spale ist die **USt gesamt**, nicht das Brutto.
- Das Brutto steht allein in `GESAMT: EUR <brutto>` (in der Positionstabelle).
  Die zweite `GESAMT`-Zeile bei der USt ist die USt — deshalb die **erste**
  `GESAMT`-Zeile nehmen.

## Amazon-Fee-CSV (bevorzugt neben dem PDF)

Amazon legt zur Gebührenrechnung eine CSV bei. Sie ist verlässlicher als das
PDF-Layout, deshalb **bei Amazon immer zusätzlich einlesen**:

```
"Transaction Date","Transaction ID","Order ID","Fees Invoice Number",
 "Marketplace","Fee ID","Total Fees (VAT-Inclusive)"
```

- `Transaction Date` ist **Monat/Tag/Jahr** (US-Format), nicht wie im PDF.
- `Fee ID` ist die sichere Kategoriequelle (`Referral Fee`,
  `MCF FBA Pick & Pack Fee`, `Refund Administration Fee`, …).
- `Total Fees (VAT-Inclusive)` ist **brutto**. Netto/USt werden je Zeile über
  `brutto / 1,19` zerlegt (Kaufland und Amazon runden beide je Position).
- Kontrollsumme gegen die PDF-Gesamtsumme über das **Brutto** — das ist robust
  gegen die Rundungsreste der Nettowerte.

### Shipping Chargeback und Inventory Removals

Recherche am 07.10.2026, Amazon-Hilfe als Primärquelle:

- [Shipping chargeback fee](https://sellercentral.amazon.com/help/hub/reference/external/GCKEJYYF9LHR2KBY):
  Amazon erhält die vom Kunden bezahlten Versandkosten zurück, da Amazon den
  Versand übernimmt. Keine Remissionsgebühr. Kategorie `shipping_chargeback`,
  bestellbezogene Abgrenzung wie andere Ordergebühren; eine Erstattung erbt den
  Ursprungsbezug. Kundenversandumsatz nicht aus der Ausgangs-USt entfernen.
- [FBA removal order fees](https://sellercentral.amazon.com/help/hub/reference/external/G200685050)
  und [Lagerbestand entfernen](https://sellercentral.amazon.com/help/hub/reference/external/G200280650):
  Gebühr je aus dem Logistikzentrum entfernter Einheit, etwa Rücksendung an eine
  angegebene Adresse. Kategorie `inventory_removal`, Abgrenzung nach belegtem
  Leistungsdatum. Die Fee-CSV allein belegt nicht, ob eine konkrete Entfernung
  Rücksendung oder Entsorgung war; dafür ist der Remissionsbericht maßgeblich.

Die USt folgt dem Originalbeleg; aus der amerikanischen Hilfeseite werden keine
Steuersätze oder US-Gebührentarife übernommen.

## Selbstvalidierung

`_finalize()` prüft und setzt `needs_review_reasons`:

| Kürzel | Bedeutung |
|---|---|
| `kein_template_erkannt` | nichts erkannt, `parse_confidence = 0`, es wird nichts gebucht |
| `kontrollsumme_netto_verfehlt` | Summe der Positionen ≠ ausgewiesenes Netto |
| `kontrollsumme_brutto_verfehlt` | Summe der Positionen ≠ ausgewiesenes Brutto |
| `unbekannte_position:<text>:<anzahl>` | Bezeichner nicht einordenbar, gruppiert je Gebührenart |
| `keine_positionen_gefunden` | leerer Beleg |
| `rechnungsnummer_fehlt` | kein Pflichtfeld |
| `originalrechnung_fehlt` | Gutschrift ohne Bezug |
| `leistungszeitraum_fehlt` | Sammelrechnung ohne Periode |

Toleranz: 2 Cent brutto, 1 Cent je Position netto (Amazon rundet je Position —
deshalb weicht die Nettosumme um Rundungsreste ab, z. B. 22,31 statt 22,30).

## Belegter Stand

Gegen 21 echte PDFs geprüft (`/mnt/sharedclaw/Kaufland_Rechnungen`,
`/mnt/sharedclaw/Amazon_Invoices`):

- 8 Kaufland-Sammelrechnungen, **777 Einzelpositionen**, Kontrollsummen 8/8 exakt
- 2 Kaufland-Einzelbelege
- 3 Amazon-Abos inkl. FX-Kurs und EUR-Vorsteuer
- 1 Amazon-Steuergutschrift inkl. Originalbezug
- 1 Amazon-Retail-Gutschrift (Umsatzbeleg)

## Hinweise für KI-Agenten

- Upload über `POST /api/bookings/monthly-invoices/parse` mit PDF und optional
  CSV. Der Aufruf **legt nichts an** — er liefert nur die Vorbefüllung.
- Response enthält `parse_confidence` und `needs_review_reasons`. Bei
  `parse_confidence < 1` oder nicht-leeren Gründen: dem Menschen zur Freigabe
  vorlegen, nicht selbst freigeben.
- `POST …/{id}/approve` ist der einzige Weg, der etwas ins Ledger schreibt.
- Amazon-Rechnungen immer **mit** CSV einreichen, wenn verfügbar.
