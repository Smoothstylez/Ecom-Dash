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

## Release- und Docker-Datensicherheit

- Vor jedem Push/Release `git status --short --branch`, `git diff --stat`,
  `git diff --check` und die **einzelnen** zu stageenden Pfade prüfen. Nie
  `git add .` für einen Release verwenden.
- `ecommerce-dashboard/data/` und `ecommerce-dashboard/storage/` enthalten
  betriebliche SQLite-Daten, Bestellungen, Kunden-/Supportdaten, Rechnungen,
  Belegbilder, Uploads und auch bereits versionierte Seed-/Referenzinhalte.
  Bestehende, absichtlich versionierte Dateien und deren Docker-Einbindung
  unverändert lassen. Geänderte/untracked Runtime-Dateien nicht versehentlich
  stagen; neue Testdaten nur als ausdrücklich synthetische Fixtures hinzufügen.
- Das unversionierte Arbeitsverzeichnis kann Runtime-Dateien enthalten, die
  `.gitignore` ignoriert, aber Docker trotzdem sieht. Die `Dockerfile` kopiert
  `data/` und `storage/` als Seeds; `.dockerignore` schließt diese Pfade nicht
  aus. Deshalb niemals zusätzliche pauschale `data/`-/`storage/`-Ausschlüsse
  oder Änderungen an den `COPY`-Pfaden ohne ausdrückliche Anforderung einführen.
  Für einen lokalen Release-Build ausschließlich bereits freigegebene
  versionierte Seeds beibehalten und unbeabsichtigte, ignorierte Runtime-Dateien
  sowie persönliche Uploads gemäß der bestehenden Release-Praxis ausschließen.
- `run.sh` kopiert Seeds beim ersten Start in `/data/db` und markiert das mit
  `.seeded`; bei bereits initialisierten HA-Installationen wird der Seed bei
  Container-Updates nicht erneut kopiert. App-Code und Datenbankschema laufen
  trotzdem auf vorhandenen persistenten Daten: additive SQLite-Migrationen und
  Backup-/Rollback-Pfad vor Deploy prüfen.
- Add-on-Releases erfordern dieselbe neue Version in
  `ecommerce-dashboard/config.yaml` (`version`), `ecommerce-dashboard/Dockerfile`
  (`io.hass.version`) und `ecommerce-dashboard/app/config.py` (`APP_VERSION`).
  Ohne Versionsbump erkennt Supervisor keinen neuen Add-on-Release; `APP_VERSION`
  steuert zusätzlich Frontend-Cache-Busting. Keine Version nur für reine
  Dokumentänderungen ohne Release erhöhen.
- Git-Tags gibt es nicht in jedem bisherigen Release; Commit/Tag/Remote-Stand
  separat feststellen und nicht aus `APP_VERSION` ableiten.

## Wichtige Verweise

| Thema | Datei |
|---|---|
| Design Language | `frontend/DESIGN-LANGUAGE.md` |
| API-Referenz | `docs/dashboard-backend-api.md` |
| Konsistenz-Scan (2026-09) | `docs/superpowers/reports/2026-09-23-design-consistency-scan.md` |
| Steuer-Logik (USt-Report) | `docs/superpowers/specs/2026-09-23-ust-report-design.md` |
| Einkaufspool | `docs/dashboard-backend-api.md` (Abschnitt "Amazon Einkaufspool") |
