# Design-Konsistenz-Scan — 2026-09-23

Referenz: `frontend/DESIGN-LANGUAGE.md`
Basis: `ecommerce-dashboard/app/static/css/main.css`, `frontend/src/features/*/`

---

## Zusammenfassung

| Kategorie | Verstoesse | Prioritaet |
|---|---|---|
| `className="button"` (kein CSS) | 37 Stellen in 3 Dateien | hoch |
| Hardcode-Farben | 2 Stellen in 1 Datei | hoch |
| `<input type="month">` raw | 1 Stelle | mittel |
| Inline `fontSize` px | 5 Stellen | mittel |
| Inline `fontSize` rem-Override | 6 Stellen | niedrig |
| KPI-Tile ohne `.card` | 4 Tiles | niedrig |
| Padding/Margin px-vs-rem gemischt | ~15 Stellen | niedrig |
| Raw `<input type="file">` | ~5 Stellen | mittel |
| Page-Root inkonsistent | gemischt | niedrig |

---

## Detail-Funde

### 1. `className="button"` — kein CSS definiert (hoch)

Rendert als nackter Browser-Button (nur `font: inherit`). Muss `btn-inline` (+Modifier) werden.

| Datei | Zeilen | Anzahl | Soll |
|---|---|---|---|
| `features/amazon/amazon-page.tsx` | 573, 637, 640, 692, 701 | 5 | `btn-inline primary` / `btn-inline ghost` / `btn-inline danger` |
| `features/amazon/amazon-pool-page.tsx` | 134, 137, 153, 154, 155, 158, 163, 164, 167, 168, 170, 177, 180, 181, 183 | 15 | `btn-inline primary` (Submit) / `btn-inline ghost` (neutral) |
| `features/amazon/amazon-inventory-page.tsx` | 195 | 1 | `btn-inline secondary` |

**Mapping:**
- Submit/Erstellen/Bestaetigen → `btn-inline primary`
- Entfernen/Loeschen → `btn-inline danger`
- Anzeigen/Download/Vorschau → `btn-inline ghost`
- Neutral (Zuordnen, Uebernehmen) → `btn-inline secondary`

### 2. Hardcode-Farben (hoch)

| Datei:Zeile | Hardcode | Soll |
|---|---|---|
| `invoices-page.tsx:455` | `background: "rgba(41, 94, 174, 0.10)"` | `background: "color-mix(in srgb, var(--th-accent) 10%, transparent)"` oder CSS-Klasse |
| `invoices-page.tsx:774` | `background: "#fff"`, `border: "1px solid rgba(148, 163, 184, 0.25)"` | `var(--th-surface-white)`, `1px solid var(--th-line)` |

### 3. Datepicker raw (mittel)

| Datei:Zeile | Aktuell | Soll |
|---|---|---|
| `invoices-page.tsx:593` | `<input type="month">` + `settings-inline-input` | Monatswähler im Picker-Look (Referenz: `tax-report-month-picker.tsx`) |
| `amazon-pool-page.tsx:158,163,164,179` | `<input type="date">` raw | `.control > label + input` Wrapper |

### 4. Inline `fontSize` in px (mittel)

| Datei:Zeilen | Wert | Soll |
|---|---|---|
| `support-page.tsx:497,512,556,586,611` | `fontSize: 15` (px!) | `fontSize: "0.98rem"` oder Standard `table-title` |

### 5. Inline `fontSize` rem-Override (niedrig)

| Datei:Zeilen | Wert | Soll |
|---|---|---|
| `bookings-page.tsx:2090,2182,2231,2307,2350,2379` | `fontSize: "0.98rem"` auf `table-title` | Override streichen (Standard ist `1.08rem`) |

### 6. KPI-Tile ohne `.card` (niedrig)

| Datei | Aktuell | Soll |
|---|---|---|
| `amazon-page.tsx:458-465` | `article.kpi` | `article.card.kpi` (Schatten/Radius wie andere Seiten) |

### 7. Padding/Margin gemischt px-vs-rem (niedrig)

| Datei | Form |
|---|---|
| `orders-page.tsx:538` | `padding: 16, marginTop: 12` (px) |
| `ebay-page.tsx:233,441` | `padding: 16` (px) |
| `amazon-page.tsx:563,579` | `padding: "1.25rem"` (rem) |
| `invoices-page.tsx:419,483,582,603` | `padding: 16` / `padding: 12` (px) |
| `google-ads-page.tsx:838` | `padding: 16` (px) |

**Regel:** px fuer Layout (`12`, `16`) — rem-Werte in diesen Inline-Styles angleichen.

### 8. Raw `<input type="file">` ohne Styling (mittel)

| Datei:Zeilen | Soll |
|---|---|
| `amazon-pool-page.tsx:155,170` | `sammel-file-field` / `sammel-file-btn` / `sammel-file-name` |
| `amazon-pool-page.tsx:158` (legacy) | gleiches Pattern |

### 9. Page-Root (niedrig)

| Pattern | Seiten |
|---|---|
| `section.page` | amazon, tax-report, support |
| `div.tab-panel.active` | orders, ebay, analytics, invoices |
| `section.amazon-pool` | amazon-pool (eigen) |
| `section.analytics-react-state` | analytics (intern) |

**Soll:** `section.page` als Standard fuer Feature-Seiten. Bestehende `tab-panel`-Seiten koennen bleiben (kein visueller Unterschied), neue Seiten nutzen `section.page`.

### 10. Amazon-Pool Layout (mittel)

`amazon-pool-page.tsx` rendert alles in einem dichten `<section className="amazon-pool">` ohne `card`-Kachelstruktur. Tabellen nutzen `.pool-scroll` statt `.table-wrap`.

**Soll:** Formulare in `detail-card`-Kacheln, Tabellen in `card table-card`, Upload im `sammel-file-*`-Pattern.

---

## Nicht betroffen (konform)

- `analytics-page.tsx` — Referenz
- `orders-page.tsx` — Referenz (nach VST-Entfernung)
- `tax-report-page.tsx` — konform (nach Redesign)
- `customers-page.tsx` — konform (bis auf KPI `card kpi` OK)
- `google-ads-page.tsx` — konform
- `ebay-page.tsx` — konform (bis auf px-padding)
- `support-page.tsx` — bis auf fontSize-Overrides konform
- `bookings-page.tsx` — bis auf fontSize-Overrides konform
