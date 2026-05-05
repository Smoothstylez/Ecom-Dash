# Frontend Runtime Notes

## Ziel

Das Dashboard wird ueber Vite als einzige Frontend-Shell ausgeliefert. Optik und Verhalten bleiben auf dem etablierten alten Layout, waehrend Backend-APIs und bestehende JS-Module weiter genutzt werden.

## Invarianten

- Die produktiven Routen `/`, `/analytics`, `/orders`, `/customers`, `/bookings`, `/bookings/full`, `/google-ads` und `/ebay` liefern dieselbe Vite-App aus.
- Backend-Endpunkte unter `/api/*` bleiben stabil.
- Die Layout- und Interaktionslogik lebt aktuell in den vorhandenen Dateien unter `ecommerce-dashboard/app/static/js` und `ecommerce-dashboard/app/static/css`.
- `/app-preview/*` existiert nur noch als Redirect fuer alte Links.

## Aktuelle Vereinfachung

1. Die alte React-Preview-Struktur wurde entfernt.
2. Die Frontend-Auslieferung ist auf eine einzige Vite-Shell reduziert.
3. Alte Preview-Routen leiten auf die Hauptpfade weiter.
4. Weitere Vereinfachungen sollten schrittweise die Legacy-JS-Module in kleinere, besser wartbare Frontend-Module ueberfuehren.

## Teststrategie

### Backend

- API-Smoke-Tests fuer die wichtigsten GET-Endpunkte
- Route-Smoke-Tests fuer die produktiven Dashboard-Pfade
- Redirect-Tests fuer alte `/app-preview/*`-Links
- Bestehende Datenintegritaets-Skripte bleiben Teil der Regression

## Naechster technischer Schritt

- Das verbleibende Legacy-Markup und die JS-Module sollten schrittweise in kleinere Frontend-Module zerlegt werden.
- Ziel ist weniger globaler DOM-Zustand, weniger implizite Abhaengigkeiten und klarere Testbarkeit.
