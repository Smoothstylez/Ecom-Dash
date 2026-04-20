# Frontend Preview

## Starten

1. Backend starten:
   - `cd /home/luis/projects/Ecom-Dash/ecommerce-dashboard`
   - `python -m uvicorn app.main:app --host 0.0.0.0 --port 8012`
2. Frontend starten:
   - `cd /home/luis/projects/Ecom-Dash/frontend`
   - `npm install`
   - `npm run dev`

## Build fuer FastAPI Preview

- `npm run build`
- Danach ist die gebaute App unter `http://localhost:8012/app-preview` verfuegbar.

## Scope von Phase 1

- gemeinsamer Router- und Query-Stack
- Analytics-Preview mit echten API-Daten
- Platzhalter-Routen fuer die restlichen Bereiche
- Vitest-Basis fuer neue Komponenten
