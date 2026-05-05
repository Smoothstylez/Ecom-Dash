# Frontend Shell

## Starten

1. Backend starten:
   - `cd ecommerce-dashboard`
   - `python -m uvicorn app.main:app --host 0.0.0.0 --port 8012`
2. Frontend starten:
   - `cd frontend`
   - `npm install`
   - `npm run dev`

## Build fuer FastAPI

- `npm run build`
- Danach ist die gebaute App unter `http://localhost:8012/analytics` verfuegbar.

## Aktueller Stand

- Vite liefert die Haupt-App unter den produktiven Dashboard-Routen aus.
- Das alte Dashboard-Layout bleibt bewusst erhalten.
- Die aktive App bootet ohne das alte Legacy-JS-Bundle unter `/static/js`.
- `/app-preview/*` wird auf die Hauptpfade weitergeleitet und behaelt Query-Strings bei.
- API-Credentials werden nicht mehr im Repo oder Browser gespeichert; lokal/HA laufen sie ueber Umgebungsvariablen bzw. Add-on-Optionen.
