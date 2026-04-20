# Frontend Migration Plan

## Ziel

Das Backend bleibt vorerst API-seitig unveraendert. Das bestehende statische Frontend wird in kleinen, testbaren Schritten nach `React 19 + Vite + TypeScript + TanStack Query + TanStack Router + shadcn/ui + Tailwind CSS v4` migriert.

## Invarianten

- Das Legacy-Dashboard unter `/`, `/analytics`, `/orders`, `/customers`, `/bookings`, `/bookings/full`, `/google-ads` und `/ebay` bleibt funktionsfaehig, bis die jeweilige Route migriert ist.
- Neue React-Arbeit laeuft zunaechst separat unter `/app-preview`.
- Backend-Endpunkte bleiben stabil; neue Frontend-Komponenten konsumieren die bestehenden `/api/*`-Routen.
- Jede migrierte Route bekommt vor dem Umschalten mindestens Smoke- und Render-Regressionen.

## Migrationsreihenfolge

1. Fundament: Frontend-Workspace, Router, Query-Client, UI-Primitives, Testbasis
2. Analytics: Referenz fuer Layout, Karten, Tabellen, Spacing und Interaktionen
3. Orders: Tabellen, Filter, Mutationen, Uploads
4. eBay: read-only, geringe Komplexitaet
5. Google Ads: Uploads, KPIs, Tabellen, Charts
6. Customers: KPIs, Tabellen, Map/Globe als spezialisierte Visuals
7. Bookings: zuletzt, wegen der meisten Flows und Detail-Interaktionen

## Teststrategie

### Backend

- API-Smoke-Tests fuer die wichtigsten GET-Endpunkte
- Route-Smoke-Tests fuer Legacy-HTML-Aliase und `/app-preview`
- Bestehende Datenintegritaets-Skripte bleiben Teil der Regression

### Frontend

- Vitest fuer UI-Primitives, Hilfsfunktionen und Route-Shells
- Danach pro migrierter Route weitere Komponenten- und Hook-Tests
- E2E-Suite folgt nach der ersten echten Produktionsroute in React

## UI-Standardisierung

- Analytics ist die visuelle Vorlage fuer neue Komponenten.
- Ziel ist ein gemeinsames System fuer:
  - `Button`
  - `Card`
  - `KpiCard`
  - `SectionHeader`
  - `DataTableShell`
  - `SegmentedControl`
  - `Dialog` / `Drawer` / `Popover`
- Inline-Styles aus `dashboard.html` werden routeweise abgebaut und in wiederverwendbare Komponenten ueberfuehrt.

## Aktuelle Phase

Phase 1 ist aktiv:

1. React-Vite-Workspace anlegen
2. Preview-Route neben dem Legacy-Dashboard anbieten
3. Erste Analytics-Ansicht mit echten API-Daten aufbauen
4. Erste automatisierte Regressionen einziehen
