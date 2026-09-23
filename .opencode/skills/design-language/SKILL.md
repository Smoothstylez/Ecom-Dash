---
name: design-language
description: Use when creating or editing any frontend UI, React component, page layout, or dashboard visual element. Covers buttons, cards, tables, KPI tiles, date pickers, file uploads, theme tokens, size scale, and German labeling. TRIGGER: any task that touches frontend/src/ or adds/changes visible UI.
---

# Design Language

Read `frontend/DESIGN-LANGUAGE.md` before any UI work. It is the single
source of truth for the dashboard visual language.

## Quick Reference

**Colors:** only `--th-*` CSS variables + `--danger`/`--success`/`--warning`/`--info`.
Never hardcode hex/rgb/rgba.

**Buttons:** `btn-inline` with modifier `.primary` (submit), `.secondary` (neutral),
`.ghost` (preview/cancel), `.danger` (delete). Never `className="button"`.

**Size scale:**
- Layout gaps: `8` / `10` / `12` / `14` / `16` (px)
- Font sizes: `0.75` / `0.78` / `0.82` / `0.84` / `0.92` / `1.08` / `1.35` (rem)
- No inline `fontSize` on `table-title`

**Components:**
- KPI: `article.card.kpi` > `kpi-name` / `kpi-value` / `kpi-sub`
- Detail cards: `detail-grid` > `detail-card` > `detail-kv` > `detail-row`
- Tables: `card table-card` > `table-head` > `table-title` + `table-wrap`
- Tabs: `trend-granularity` > `segmented-btn`
- Upload: `sammel-file-field` / `sammel-file-btn` / `sammel-file-name`
- Datepicker: Picker-Look via `control-menu` / `date-days-grid` / `date-day`
- Badges: `badge badge-sale|fee|cogs|invoice|refund|default`

**Labels:** German plain language. No internal API names (`de_b2c`, `input_vat_status`).
See glossary in `frontend/DESIGN-LANGUAGE.md` section 4.

**New page checklist:** `frontend/DESIGN-LANGUAGE.md` section 5.

## Reference Pages

- `frontend/src/features/analytics/analytics-page.tsx` — charts, KPI, date range
- `frontend/src/features/orders/orders-page.tsx` — tables, badges, upload
- `frontend/src/features/tax-report/tax-report-page.tsx` — month picker, tiles, tabs
