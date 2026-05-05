# Frontend Shell

## Starten

1. Backend starten:
   - `cd /home/luis/projects/Ecom-Dash/ecommerce-dashboard`
   - `python -m uvicorn app.main:app --host 0.0.0.0 --port 8012`
2. Frontend starten:
   - `cd /home/luis/projects/Ecom-Dash/frontend`
   - `npm install`
   - `npm run dev`

## Build fuer FastAPI

- `npm run build`
- Danach ist die gebaute App unter `http://localhost:8012/analytics` verfuegbar.

## Aktueller Stand

- Vite liefert die Haupt-App unter den produktiven Dashboard-Routen aus.
- Das alte Dashboard-Layout bleibt bewusst erhalten.
- Die Interaktionen laufen weiterhin ueber die bestehenden Legacy-JS-Module unter `/static/js`.
- `/app-preview/*` wird nur noch auf die Hauptpfade weitergeleitet.
