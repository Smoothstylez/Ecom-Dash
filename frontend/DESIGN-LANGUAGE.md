# Design Language — Ecom-Dash

Verbindliche Referenz für alle Oberflaechen. Neue Features **muessen** diesen Regeln folgen.
Ausnahmen nur nach ausdruecklicher Freigabe.

**Referenzseiten (so soll es aussehen):**
- `features/analytics/analytics-page.tsx` — Charts, KPI-Kacheln, Datepicker-Nutzung
- `features/orders/orders-page.tsx` — Tabellen, Badges, Upload, Detail-Karten
- `features/tax-report/tax-report-page.tsx` — Monats-Picker, Kacheln, Tabs, Klartext-Labels

**Stylesheet:** `ecommerce-dashboard/app/static/css/main.css` (einzige CSS-Quelle)
**Theme-Tokens:** `ecommerce-dashboard/app/static/css/themes.css` (`--th-*`-Variablen)
**Theme-System:** `frontend/src/shared/theme/` (`ThemeProvider`, `data-theme`, Custom-Editor)

---

## 1. Theme & Farben

- **Alle Farben** ueber `--th-*`-CSS-Variablen. **Nie** Hex/RGB/hsl hardcoden.
- `data-theme` auf `<html>` steuert das aktive Theme; `ThemeProvider` verwaltet Wechsel + Persistenz.
- Custom-Theme-Editor (`DashboardThemeModal`) schreibt dieselben `--th-*`-Variablen.
- Ausnahmen fuer Betriebszustande: `var(--danger)`, `var(--success)`, `var(--warning)`, `--info` (in `main.css` definiert).
- **Verboten:** `style={{ color: "#..." }}`, `background: "rgba(...)"`, `background: "#fff"`.

### Marketplace-Farben (verbindlich)

| Marketplace | Token | Farbe (warm-light) | Badge-Klasse | Row-Klasse |
|---|---|---|---|---|
| Shopify | `--th-marketplace-shopify` | Gruen `#1f8b5f` | `badge-shopify` | `order-row-shopify` |
| Kaufland | `--th-marketplace-kaufland` | Rot `#d85048` | `badge-kaufland` | `order-row-kaufland` |
| Amazon | `--th-marketplace-amazon` | Orange `#e8923a` | `badge-amazon` | `order-row-amazon` |

**Regeln:**
- In **Vergleichsdarstellungen** (Charts, Tabellen, Karten) immer diese drei Marketplace-Farben verwenden.
- `--th-donut-shopify` / `--th-donut-kaufland` / `--th-donut-amazon` fuer Donut-Charts.
- Green bleibt Shopify vorbehalten. Kaufland = Rot, Amazon = Orange.
- Custom-Theme-Editor: Gruppe „Marketplace" fuer alle Marketplace-Tokens.

Erlaubt (Theme-konform):
```tsx
<div className="table-meta" style={{ color: "var(--th-ink-4)" }} />
<div style={{ color: "var(--danger)" }}>Fehler</div>
```

---

## 2. Groessen-Skala

### Abstaende (px, Inline nur wenn nötig)
| Zweck | Wert | CSS-Klasse bevorzugt |
|---|---|---|
| Kompakt (Tabellenzellen-Abstand) | `8px` | — |
| Standard (Karten-Innenabstand) | `10px` / `14px` | `.table-card { padding: 14px }`, `.detail-card { padding: 10px }` |
| Karten-Luecke (grid gap) | `10px` | `.kpi-grid`, `.detail-grid` |
| Sections-Luecke | `12px` / `1rem` | inline `marginTop: 12` |
| Grosszuegig (Modal, Hero) | `16px` / `1.25rem` | inline `padding: 16` |

**Regel:** Entweder durchgehend px (`12`, `16`) **oder** durchgehend rem (`1rem`, `1.25rem`) pro Seite. Nicht mischen. Empfehlung: **px** fuer Layout-Abstaende (wie `main.css`), **rem** nur fuer Schriftgroessen.

### Schriftgroessen (rem)
| Element | Klasse | Groesse | Gewicht |
|---|---|---|---|
| KPI-Label | `.kpi-name` | `0.75rem` | 600, uppercase, letter-spacing |
| KPI-Wert | `.kpi-value` | `1.35rem` | 700 |
| KPI-Untertitel | `.kpi-sub` | `0.8rem` | 400 |
| Tabellen-Header | `thead th` | `0.75rem` | 600, uppercase |
| Tabellen-Zelle | `tbody td` | `0.84rem` | 400 |
| Sektions-Titel | `.table-title` / `.chart-title` | `1.08rem` | 600 |
| Meta-Text | `.table-meta` | `0.82rem` | 400 |
| Detail-Label | `.detail-row span` | `0.78rem` | 400 |
| Detail-Wert | `.detail-row strong` | `0.82rem` | 400 |
| Button (klein) | `.btn-inline` | `0.78rem` | 600 |
| Formular-Label | `.control label` | `0.78rem` | 600, uppercase |

**Verboten:** Inline-`fontSize` in px (`fontSize: 15`). Wenn abweichend: rem-Wert aus der Skala.

---

## 3. Komponenten

### KPI-Kacheln
```tsx
<div className="kpi-grid">
  <article className="card kpi">
    <div className="kpi-name">Label</div>
    <div className="kpi-value">{value}</div>
    <div className="kpi-sub">Untertitel</div>
  </article>
  {/* max. 6 pro Reihe */}
</div>
```
- Immer `card kpi` (mit `.card` fuer Schatten/Radius).
- Optional: `.kpi-trend` (+`.trend-up`/`.trend-down`/`.trend-flat`) zwischen value und sub.

### Detail-Kacheln (Zusammenfassung / Key-Value)
```tsx
<section className="detail-grid">
  <article className="detail-card">
    <h3>Titel</h3>
    <div className="detail-kv">
      <div className="detail-row"><span>Label</span><strong>Wert</strong></div>
    </div>
  </article>
</section>
```
- 3 Spalten (`.detail-grid`), `.detail-card-wide` fuer volle Breite.
- Tabellen innerhalb: `section.detail-table-wrap > table`.

### Tabellen
```tsx
<section className="card table-card">
  <div className="table-head">
    <h2 className="table-title">Titel</h2>
    <div className="table-meta">12 Zeilen</div>
  </div>
  <div className="table-wrap">
    <table>
      <thead><tr><th>A</th></tr></thead>
      <tbody>
        {rows.length ? rows.map(...) : <tr><td colSpan={1}>Keine Daten.</td></tr>}
      </tbody>
    </table>
  </div>
</section>
```
- Klassen: `table-head` (nicht `table-header`!), `table-title`, `table-meta`, `table-wrap`.
- `<th>`/`<td>` ohne eigene Klasse (globale CSS-Regeln).
- Empty-State: `<tr><td colSpan={N}>Keine ...</td></tr>`.

### Buttons
| Klasse | Einsatz |
|---|---|
| `btn-inline primary` | Hauptaktion (Speichern, Anlegen, Abschicken) |
| `btn-inline secondary` | Nebenaktion |
| `btn-inline ghost` | Neutral (Preview, Paginierung, Abbrechen) |
| `btn-inline danger` | Loeschen, Verwerfen |
| `btn-inline` | Standard (Schliessen) |

**Verboten:** `className="button"`, `"button button-primary"` — kein CSS definiert, rendert ungestylt.

### Tabs / Segmented Controls
```tsx
<div className="trend-granularity" role="tablist">
  <button className="segmented-btn active" type="button">Tab A</button>
  <button className="segmented-btn" type="button">Tab B</button>
</div>
```
Amazon-Pool-Sub-Tabs: `nav.amazon-pool-tabs > button.segmented-btn`.

### Badges
```tsx
<span className={`badge ${badgeClass}`}>{label}</span>
```
Varianten: `.badge-sale`, `.badge-fee`, `.badge-cogs`, `.badge-invoice`, `.badge-refund`, `.badge-default`.

### Formulare
```tsx
<div className="control">
  <label htmlFor="fieldId">Label</label>
  <input id="fieldId" ... />
</div>
```
- Grid: `.bookings-form-grid` (5-spaltig) oder `.detail-form-grid`.
- Actions: `.bookings-form-actions` (rechtsbuendig).
- Datepicker/Select-Trigger: `.control-menu-trigger` + `.control-menu` (Sidebar-Look).

### Datepicker
- **Globaler Zeitraum:** Sidebar-Trigger `.sidebar-control-btn` + Popup `.control-menu.date-range-menu` mit `.date-menu-layout`, `.date-calendar-pane`, `.date-days-grid`, `.date-day`.
- **Monatsauswahl (eigene Seite):** gleiche Klassen, eine Kalenderscheibe mit Monatsnamen in `.date-days-grid`, Auswahl via `.date-day.range-edge`. Referenz: `features/tax-report/tax-report-month-picker.tsx`.
- **Verboten:** nacktes `<input type="month">` oder `<input type="date">` ohne `.control`-Wrapper als sichtbarer Picker.

### Datei-Upload
```tsx
<div className="sammel-file-field">
  <label className="sammel-file-btn" htmlFor="fileInput">Datei waehlen</label>
  <input id="fileInput" className="sammel-file-input" type="file" />
  <div className="sammel-file-name">Optional</div>
</div>
```
Alternativ (Tabellenzeilen): `label.file-picker-label` + `input.invoice-file-input` (hidden).
**Verboten:** nacktes `<input type="file">` ohne Styling.

### Beleg-Aktionen (Preview + Download)
```tsx
<span className="doc-actions">
  <button className="btn-inline ghost" ...>Preview</button>
  <a href={url} target="_blank" rel="noreferrer">Download</a>
</span>
```

### Status-Hinweise
```tsx
<div className="status status-ok">Gespeichert.</div>
<div className="status status-error">Fehler.</div>
<div className="status status-info">Hinweis.</div>
```

### Page-Root
```tsx
<section className="page" aria-label="Seitenname">
  {/* KPIs, Tabs, Kacheln, Tabellen */}
</section>
```

---

## 4. Sprache & Beschriftung

- **Klartext statt API-Names.** Keine internen Bezeichner wie `de_b2c`, `eu_b2b_intra_community_supply`, `input_vat_status`, `KAUFLAND_RATE_NEEDS_OVERRIDE` in der UI.
- **Glossar** (verbindlich):
  | intern | Anzeige |
  |---|---|
  | `de_b2c` | Deutschland-Veraeufe (19 %) |
  | `eu_b2b_intra_community_supply` | Innergemeinschaftliche Lieferungen (0 %) |
  | `eu_b2c_home_rate` | EU-Veraeufe mit deutscher USt |
  | `unresolved` | Zu pruefen |
  | `deemed_supplier` | Amazon als Steuerschuldner |
  | `export` | Ausfuhr ausserhalb EU |
  | `returns` | Retouren & Erstattungen |
  | `purchases_cents` | Wareneinkauf |
  | `amazon_fees_cents` | Amazon-Gebuehren |
  | `kaufland_fees_cents` | Kaufland-Gebuehren |
  | `nontaxable_cents` | Nicht steuerbar (Schadensersatz) |
  | `input_vat_status: confirmed` | Freigegeben |
  | `input_vat_status: review_required` | Zu pruefen |
  | `input_vat_status: non_deductible` | Ohne Vorsteuer |
- Einheiten: „Stueck", „Zeilen", „Positionen".
- Buttons: Verben im Infinitiv („Speichern", „Anlegen", „Hochladen") oder Klartext („Alle auf 19 % setzen").

---

## 5. Neue Seite — Checkliste

- [ ] Page-Root: `section.page`
- [ ] KPI-Leiste (falls vorhanden): `kpi-grid` + `card kpi`
- [ ] Tabs: `trend-granularity` + `segmented-btn`
- [ ] Kacheln: `detail-grid` / `detail-card` / `detail-row`
- [ ] Tabellen: `card table-card` / `table-head` / `table-title` / `table-wrap`
- [ ] Buttons: ausschliesslich `btn-inline`-Familie
- [ ] Formulare: `.control > label + input`
- [ ] Upload: `sammel-file-*` oder `file-picker-label`
- [ ] Datepicker: Picker-Look (Monatswähler / Datumsfeld in `.control`)
- [ ] Farben: nur `--th-*` / `--danger` / `--success` / `--warning` / `--info`
- [ ] Groessen: px fuer Layout, rem fuer Schrift (Skala aus Abschnitt 2)
- [ ] Labels: Klartext aus Glossar, keine internen API-Names
- [ ] Empty-State: `<td colSpan={N}>Keine ...</td></tr>`
- [ ] Theme-Wechsel: keine Hardcodes, Responsive bei 1120px / 760px beachten

---

## 6. Bekannte Altlasten (Stand 2026-09-23)

Diese Stellen weichen ab und werden schrittweise bereinigt (siehe
`docs/superpowers/reports/2026-09-23-design-consistency-scan.md`):

| Datei | Abweichung |
|---|---|
| `amazon-page.tsx`, `amazon-pool-page.tsx`, `amazon-inventory-page.tsx` | `className="button"` statt `btn-inline` |
| `invoices-page.tsx:455` | Hardcode `rgba(41, 94, 174, 0.10)` |
| `invoices-page.tsx:774` | Hardcode `#fff` + `rgba(148, 163, 184, 0.25)` |
| `invoices-page.tsx:593` | `<input type="month">` statt Picker |
| `bookings-page.tsx` (6x) | Inline `fontSize: "0.98rem"` auf `table-title` |
| `support-page.tsx` (5x) | `fontSize: 15` (px) |
| `amazon-page.tsx` | `article.kpi` ohne `.card` |
| `amazon-pool-page.tsx` | kein `card`-/`detail-grid`-Layout, raw Upload |
