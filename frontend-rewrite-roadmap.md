# Frontend Rewrite Roadmap

## Ziel

Das Ziel ist ein legacy-freies React/TypeScript-Frontend.

Eine Route gilt nur dann als migriert, wenn:
1. React die Daten selbst laedt
2. React den State selbst besitzt
3. React Mutationen selbst ausfuehrt
4. React Modale und Details selbst steuert
5. kein `window.__ECOM_DASH_REACT_*`- oder `ecomdash:*`-Bridge-Code mehr noetig ist
6. kein Rendern in Legacy-Hosts per Portal mehr noetig ist
7. route-spezifischer Legacy-Code danach geloescht werden kann

## Arbeitsregeln

1. Keine weitere Route gilt als "fertig", nur weil React etwas rendert.
2. Prioritaet ist Ownership-Migration, nicht nur Rendering-Migration.
3. Pro Route arbeiten wir in dieser Reihenfolge:
   - Daten
   - State
   - Mutationen
   - Modale und Interaktionen
   - Rendering
   - Bridge entfernen
   - Legacy-Code loeschen
4. Eine Route ist erst fertig, wenn ihre Bridge entfernt ist.
5. `bookings` bleibt zuletzt.
6. Shared Legacy-Loader und Template werden erst ganz am Ende geloescht.

## Reihenfolge

1. Plattform / Shell
2. Analytics
3. eBay
4. Google Ads
5. Orders
6. Customers
7. Bookings
8. Legacy Delete

## Statusmodell

Zulaessige Stati:
- `Not Started`
- `In Progress`
- `Hybrid`
- `React Complete`
- `Deleted`

## Aktueller Status

| Area | State | React UI | React Data | React Mutations | Legacy Runtime Remaining | Notes |
|---|---|---:|---:|---:|---|---|
| Shell | React Complete | Yes | n/a | n/a | none | Die aktive App bootet nur noch React plus CSS; Filter- und Refresh-State liegen jetzt komplett im `DashboardShellStateProvider` |
| Analytics | React Complete | Yes | Yes | n/a | none | kein route-spezifischer Bridge-Code und kein `LegacyDashboardHost` mehr; visuelle Paritaet gegen `bea1038` verifiziert |
| eBay | React Complete | Yes | Yes | n/a | none | `/ebay` rendert ohne `LegacyDashboardHost`; Daten, Filter und explizite Refreshes laufen direkt ueber React |
| Google Ads | React Complete | Yes | Yes | Yes | none | `/google-ads` rendert ohne `LegacyDashboardHost`; Daten, Upload, Reset und Produktdetails laufen direkt ueber React |
| Orders | React Complete | Yes | Yes | Yes | none | Route-DoD erreicht; `orders.js`, `react-orders-bridge.js` und globale Detail-Bridge-Reste sind entfernt, Shared-Details laufen ueber `DashboardRuntimeProvider` |
| Customers | React Complete | Yes | Yes | n/a | none | Route-DoD erreicht; `customers.js` und `react-customers-bridge.js` sind geloescht, Leaflet/topojson/globe.gl bleiben normale Drittanbieter-Libs innerhalb des React-Boots |
| Bookings | React Complete | Yes | Yes | Yes | none | `/bookings` rendert Panel, Tabellen, CRUD, Details und Preview selbst; Bookings-/Orders-/Modal-Interop laeuft nur noch ueber `DashboardRuntimeProvider` |
| Legacy Delete | Deleted | n/a | n/a | n/a | none | Die aktive App bootet ohne Legacy-JS-Bundle, Bridge-Dateien oder React-only Kompatibilitaets-Globals/-Events |

## Phase 0: React-Plattform zuerst

### Ziel

Die React-App braucht erst eine eigene Shell und gemeinsame Infrastruktur, damit die Bereiche nicht weiter ueber Legacy gekoppelt bleiben.

### Arbeitspakete

1. React `AppShell` bauen
2. Sidebar und Top-Level-Navigation aus Legacy loesen
3. globalen Filter-State in React ziehen
   - Zeitraum
   - Marketplace
   - Suche
   - Bookings-Subtab
4. React-Modal-System bauen
5. gemeinsame API-Schicht unter `frontend/src/shared/` ausbauen
6. Theme-Anbindung React-native halten, ohne Legacy-Shell-Abhaengigkeit

### Definition of Done

- keine Route braucht fuer Shell und Navigation `LegacyDashboardHost`
- React besitzt Navigation und globalen UI-State
- neue Routen koennen ohne Template-Injektion gerendert werden

### M1: Aktueller Milestone

#### Ziel

React soll zuerst die aeussere Shell-Struktur besitzen, auch wenn Legacy noch Panels und Modale liefert.

#### Erledigt

- React rendert die aeussere Dashboard-Shell selbst:
  - `.page`
  - `.page-layout`
  - Sidebar
  - `dateRangeMenu`
  - `channelMenu`
  - `.main-content-wrapper`
- Legacy injiziert nicht mehr die aeussere Shell, sondern nur noch:
  - Route-Panels
  - verbleibende Legacy-Modals
  - Sidebar-Panels

#### Verifiziert

- `npm run typecheck` grün
- `npm run build` grün
- `python3 -m unittest tests/test_app_routes.py` grün
- `npm run test:e2e` grün
- Playwright: `26 passed`
- isolierter Screenshot-Vergleich gegen `bea1038` fuer Desktop und Mobile mit stubbed API-Antworten: identische Shell-/`/analytics`-Layout-Metriken

#### Zusaetzlich erledigt

- React besitzt jetzt den globalen Shell-UI-State fuer:
  - Zeitraum
  - Marketplace
  - Suche
  - Bookings-Subtab
- React rendert und steuert das Search-Modal selbst; das Legacy-Template liefert dieses Modal nicht mehr.
- React rendert jetzt auch die shared DOM-Roots fuer `#detailsModal` und `#previewModal`; Legacy- und React-Interop laufen weiter ueber dieselben IDs.
- Legacy `init.js` bindet die React-owned Shell-Controls nicht mehr direkt, sondern konsumiert den React-Shell-Snapshot fuer Hybrid-Refreshes.
- Sidebar-Toggle und Top-Level-Navigation werden von React gesteuert.

#### Seit letztem Update erledigt

- `analytics` laeuft auf der React-Shell ohne `react-analytics-bridge.js`.
- `analytics` rendert ohne `LegacyDashboardHost`.
- Route-spezifisches Analytics-Panel und Trend-Granularitaet werden von React gerendert und gesteuert.
- Visuelle Paritaet gegen `bea1038` ist fuer Desktop und Mobile mit stubbed API-Antworten verifiziert.
- `eBay` laeuft ohne `react-ebay-bridge.js`.
- `/ebay` rendert ohne `LegacyDashboardHost` und ohne Portal-Rendern in ein Legacy-Panel.
- eBay-Daten und Filter laufen fuer die Route direkt ueber React-API-Fetching.
- Der `/ebay`-Pfad laedt `ebay.js` nicht mehr aktiv; Legacy-Refreshes skippen `loadEbay()` fuer React-owned eBay.
- `Google Ads` laeuft ohne `react-google-ads-bridge.js`.
- `/google-ads` rendert ohne `LegacyDashboardHost` und ohne Portal-Rendern in ein Legacy-Panel.
- Google-Ads-Daten, Upload, Reset und Produktdetail laufen fuer die Route direkt ueber React-API-Fetching und React-Charts.
- Der `/google-ads`-Pfad laedt `google-ads.js` nicht mehr aktiv; Legacy-Refreshes skippen `loadGoogleAds()` fuer React-owned Google Ads.

#### Naechste Schritte in M1

1. Orders als naechste Route von Bridge, Legacy-Tabellenlogik und Detailmodal-Kopplung loesen
2. Shared-Legacy-Runtime weiter verkleinern, sobald route-spezifische Restkopplungen verschwinden

## Phase 1: Route-Migrationen

### Analytics

#### Ziel

`analytics` wird die erste vollstaendig React-owned Route.

#### Definition of Done

- kein `react-analytics-bridge.js`
- keine Legacy-Filter-Bridge
- Route rendert ohne Legacy-Host
- Daten und UI-State kommen nur aus React

#### Stand

- `react-analytics-bridge.js` ist entfernt.
- Die Route rendert ohne `LegacyDashboardHost`.
- React besitzt Daten, Trend-Granularitaet und die Shell-Filter-Integration fuer `/analytics`.
- Kein aktiver Legacy-JS-Bootpfad mehr; offen bleiben nur temporaere Shell- und Cross-Feature-Kompatibilitaets-APIs.
- Visuelle Paritaet gegen `bea1038` ist fuer Desktop und Mobile mit stubbed API-Antworten verifiziert.

### eBay

#### Ziel

Kleinste read-heavy Route als naechster Voll-Auszug.

#### Definition of Done

- kein `react-ebay-bridge.js`
- kein aktiver Runtime-Bedarf an `ebay.js`
- Filter und Daten in React

#### Stand

- `react-ebay-bridge.js` ist entfernt.
- Die Route rendert ohne `LegacyDashboardHost`.
- React rendert das echte `#ebayPanel` selbst und laedt Summary/Orders direkt ueber `/api/ebay/summary` und `/api/ebay/orders`.
- Der `/ebay`-Pfad laedt `ebay.js` nicht mehr aktiv; `init.js` skippt `loadEbay()` fuer React-owned eBay.
- `ebay.js` ist entfernt; es bleibt kein aktiver Legacy- oder Kompatibilitaetspfad fuer die Route.

### Google Ads

#### Ziel

Aktionen und Detail-Logik aus Legacy loesen.

#### Definition of Done

- kein `react-google-ads-bridge.js`
- Upload, Reset und Produktdetail in React
- kein globaler Legacy-Aktionspfad mehr

#### Stand

- `react-google-ads-bridge.js` ist entfernt.
- Die Route rendert ohne `LegacyDashboardHost`.
- React rendert das echte `#googleAdsPanel` selbst und laedt Analytics und Produktdetails direkt ueber `/api/google-ads/analytics` und `/api/google-ads/product-detail`.
- Upload und Reset laufen direkt aus React ueber `/api/google-ads/upload` und `/api/google-ads/reset`.
- Der `/google-ads`-Pfad laedt `google-ads.js` nicht mehr aktiv; `init.js` skippt `loadGoogleAds()` fuer React-owned Google Ads.
- `google-ads.js` ist entfernt; es bleibt kein aktiver Legacy- oder Kompatibilitaetspfad fuer die Route.

### Orders

#### Ziel

Filter, Tabellenlogik, Mutationen und Detailmodal voll nach React holen.

#### Aktueller Stand

`/orders` rendert ohne `LegacyDashboardHost`; `orders.js`, `react-orders-bridge.js` und der alte `/static/js`-Bootpfad sind geloescht. Die shared Order-Details und Modal-Oeffnung laufen jetzt ueber `frontend/src/app/dashboard-runtime.tsx` statt ueber globale Kompatibilitaets-APIs.

#### Definition of Done

- kein `react-orders-bridge.js`
- kein aktiver Runtime-Bedarf an `orders.js`
- Tabelle, Filter, Kaufpreis, Rechnung, Details in React

### Customers

#### Ziel

Customer-Geo als letzten grossen Read-Block in React uebernehmen.

#### Aktueller Stand

`/customers` rendert ohne `LegacyDashboardHost`; `customers.js`, `react-customers-bridge.js` und der alte `/static/js`-Bootpfad sind geloescht. KPI, Liste, Map/Globe und das Geo-Status-Panel laufen direkt in React; die verbleibenden Leaflet/topojson/globe.gl-Abhaengigkeiten sind normale Drittanbieter-Libs und kein Legacy-Runtimepfad.

#### Definition of Done

- kein `react-customers-bridge.js`
- kein aktiver Runtime-Bedarf an `customers.js`
- KPI, Liste, Map/Globe in React

### Bookings

#### Ziel

Den komplexesten Legacy-Block zuletzt migrieren.

#### Definition of Done

- kein `react-bookings-bridge.js`
- kein aktiver Runtime-Bedarf an `bookings.js`
- Subtabs, CRUD, Details, Preview, Dokumente, Templates, Konten in React

#### Aktueller Stand

`react-bookings-bridge.js` ist entfernt. React laedt Transactions, Orders, Ledger Orders, Templates, Konten, Dokumente und Monthly Invoices direkt ueber `frontend/src/features/bookings/api.ts`.

Die sichtbaren Mutationen fuer Transaction create/save, Template create/save/run/backfill, Account create/save, Document upload sowie Monthly-Invoice create/delete/upload-invoice-doc laufen direkt aus React. Auch Transactions-Class-Bar, Unified-New-Button, Tool-Open/Close-Ownership, der Sammel-Month-Picker und die Fee-Preview fuer Monthly Invoices werden in `frontend/src/features/bookings/bookings-page.tsx` gehalten.

Die shared Modal-Roots fuer Details und Preview werden von React in `frontend/src/app/dashboard-shared-modals.tsx` gerendert. `frontend/src/app/dashboard-runtime.tsx` ist jetzt die einzige app-weite Runtime-Schnittstelle fuer shared Modal-/Preview-/Order-/Bookings-Interop sowie Bookings-Refresh/UI-State. `frontend/src/features/orders/order-detail-runtime.tsx` und `frontend/src/features/bookings/bookings-global-runtime.tsx` registrieren ihre APIs dort; route-lokale Doppellogik und React-only `window.__ECOM_DASH_REACT_*`- oder `ecomdash:*`-Kompatibilitaetslayer sind entfernt.

`/bookings` rendert das Route-Panel vollstaendig aus React: `frontend/src/features/bookings/bookings-panel.tsx` liefert das echte `#bookingsPanel`, `frontend/src/features/bookings/bookings-shell.tsx` mountet direkt `BookingsPanel` plus `BookingsPage`, und der fruehere versteckte `LegacyDashboardHost` ist nicht mehr Teil des aktiven App-Boots. Der alte Legacy-Bootpfad laedt `/static/js/bookings.js` auf keiner Route mehr; `ecommerce-dashboard/app/static/js/bookings.js` ist entfernt.

Der verifizierte Gruen-Checkpoint basiert auf sequentieller Pruefung (`npm --prefix frontend run typecheck` -> `npm --prefix frontend run build` -> `python3 -m unittest tests/test_app_routes.py` -> `npm --prefix frontend run test:e2e`), damit Playwright nicht gegen ein unvollstaendig neu gebautes `frontend/dist` laeuft.

## Phase 2: Legacy Delete

### Repo Exit Criteria

- keine Nutzung von `window.__ECOM_DASH_REACT_*`
- keine Nutzung von `ecomdash:*`-Kompatibilitaets-Events
- keine `react-*-bridge.js` mehr aktiv
- kein Portal-Rendern in Legacy-Hosts mehr noetig
- keine Route wird ueber `LegacyDashboardHost` gebootet
- `legacy-dashboard.ts`, `legacy-dashboard-template.ts` und `LegacyDashboardHost` sind entfernt
- die aktive App bootet ohne `/static/js/init.js` als Frontend-Runtime
- das alte `/static/js`-Bundle und die ungenutzten `frontend/src/template/*-markup.ts` Fragmente sind entfernt

## Verbindliche Entscheidung

Ab jetzt bauen wir nicht weiter an einem Hybrid als Zielarchitektur.

Der Hybrid ist nur noch Uebergangszustand.

Abschlusskriterium pro Route ist echte React-Ownership, nicht React-Rendering.
