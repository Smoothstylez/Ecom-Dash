# AGENTS.md — Ecom-Dash

Projekt: E-Commerce-Dashboard (FastAPI + React + SQLite) für Amazon FBA, Kaufland, eBay, Google Ads.

## Design Language (verbindlich fuer alle UI-Aenderungen)

**Vor jeder Frontend-/UI-Aenderung:** `frontend/DESIGN-LANGUAGE.md` lesen.

Enthaelt: Theme-Tokens (`--th-*`), Groessen-Skala, Komponenten-Muster
(KPI-Kacheln, Detail-Karten, Tabellen, Buttons, Tabs, Datepicker, Upload),
Klartext-Glossar fuer Steuerklassen, Checkliste fuer neue Seiten.

**Referenzseiten:** Analytics (Charts/Picker), Orders (Tabellen/Upload),
Tax-Report (Monats-Picker/Kacheln/Tabs).

**Kernregeln:**
- Farben nur ueber `--th-*` / `--danger` / `--success` / `--warning` / `--info`
- Buttons nur `btn-inline` (+`.primary/.secondary/.ghost/.danger`) — nie `className="button"`
- Layout-Abstaende in px (`8`/`10`/`12`/`14`/`16`), Schrift in rem (Skala beachten)
- Keine Inline-`fontSize`-Overrides auf `table-title`
- Upload: `sammel-file-*` oder `file-picker-label` — nie raw `<input type="file">`
- Datepicker: Picker-Look (Monatswähler/Datumsfeld in `.control`) — nie raw `<input type="month">`
- UI-Labels: Klartext aus Glossar, keine internen API-Names (`de_b2c`, `input_vat_status`, etc.)
- KPI-Tiles: `article.card.kpi` (mit `.card`)

## Backend-Konventionen

- Python 3 + FastAPI + raw `sqlite3` (kein ORM/Alembic)
- Geldbeträge: Integer-Cents (`*_cents`)
- Datum/Zeit: ISO-8601-UTC (`...Z`)
- Migrationen: `CREATE TABLE IF NOT EXISTS` + `_ensure_column()` (kein Alembic)
- Services in `app/services/`, Routen in `app/routers/`, Tests in `tests/` und `ecommerce-dashboard/tests/`
- Admin-Mutationen: `Depends(require_admin_access)`

## API-Dokumentation

Bei jeder API-Aenderung: `docs/dashboard-backend-api.md`,
`.opencode/skills/dashboard-backend-api/SKILL.md` und `scripts/dashboard-api/`
**im selben Commit** aktualisieren.

## Wichtige Verweise

| Thema | Datei |
|---|---|
| Design Language | `frontend/DESIGN-LANGUAGE.md` |
| API-Referenz | `docs/dashboard-backend-api.md` |
| Konsistenz-Scan (2026-09) | `docs/superpowers/reports/2026-09-23-design-consistency-scan.md` |
| Steuer-Logik (USt-Report) | `docs/superpowers/specs/2026-09-23-ust-report-design.md` |
| Einkaufspool | `docs/dashboard-backend-api.md` (Abschnitt "Amazon Einkaufspool") |
