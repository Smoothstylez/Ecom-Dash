# Coding Conventions: E-Commerce Dashboard

## Python Standards
- **Style**: Follow PEP 8, use `from __future__ import annotations` for all modules
- **Types**: Use type hints for function signatures, `Optional[T]` for nullable, `dict[str, Any]` for dynamic payloads
- **Imports**: Standard library → third-party → local (`app.*`), alphabetically sorted
- **Strings**: Use double quotes for strings, single quotes only for dict keys or nested quotes
- **Functions**: Private functions prefixed with `_`, use explicit keyword-only args for public APIs
- **Error Handling**: Raise `HTTPException` in routers, `BookkeepingServiceError` in bookkeeping service, use specific status codes
- **Database**: Always use `row_factory = sqlite3.Row`, enable `PRAGMA foreign_keys = ON`, use context managers for connections
- **No Comments**: Code should be self-documenting, avoid inline comments unless absolutely necessary

## FastAPI Patterns
- Router prefix pattern: `/api/{domain}` (e.g., `/api/orders`, `/api/analytics`)
- Query parameters with `Optional[str] = Query(default=None)` pattern
- Response dicts: always include `{"ok": True}` or error details
- Use Pydantic `BaseModel` for request bodies only (responses are plain dicts)

## JavaScript Standards
- **Style**: Strict mode (`"use strict"`), ES6+ syntax, no TypeScript
- **State Management**: Single global `state` object in `core.js`, no frameworks
- **DOM References**: Central `els` object with `document.getElementById` calls
- **API Calls**: Use `async/await` with `fetchJson()` helper, handle errors with `setStatus(message, "error")`
- **Formatting**: German locale (`de-DE`), EUR currency, `MONEY_FMT`/`DATE_FMT` formatters
- **No Comments**: Self-documenting variable names, no inline comments
- **HTML Generation**: Use template literals, escape user input with `escapeHtml()`

## Database Conventions
- **Timestamps**: ISO 8601 with Z suffix (e.g., `2026-03-15T12:00:00Z`), stored as TEXT
- **Cents**: All monetary values stored as integer cents (multiply by 100)
- **Currency**: 3-letter uppercase codes (EUR, USD), default EUR
- **IDs**: UUID v4 for all primary keys except composite keys (marketplace, order_id)
- **Paths**: Store paths relative to PROJECT_ROOT, use POSIX format (`storage/invoices/...`)

## Naming Conventions
- **Files**: Snake_case for Python (`orders.py`, `bookkeeping_full.py`), kebab-case for JS (`google-ads.js`)
- **Functions**: Snake_case for Python, camelCase for JavaScript
- **Constants**: UPPER_SNAKE_CASE for true constants, camelCase for config-like values
- **Database Tables**: Snake_case singular (`transactions`, `orders`, `documents`)
- **API Endpoints**: kebab-case URLs (`/api/orders/{marketplace}/{order_id}/invoice`)

## Anti-Patterns to Avoid
- **NO**: Using ORM or migration tools (plain SQLite only)
- **NO**: Adding comments that explain what code does (code should be self-explanatory)
- **NO**: Creating new JavaScript modules for minor features (extend existing modules)
- **NO**: Using `any` type or skipping type hints in Python
- **NO**: Hardcoding absolute paths (use `config.py` path resolution)
- **NO**: Mixing German and English in variable names (use English for code)
- **NO**: Creating new CSS files (extend `main.css` or `themes.css`)
- **NO**: Using jQuery or other DOM libraries (vanilla JS only)
- **NO**: Polling in frontend without `changestamp` mechanism
- **NO**: Committing secrets or credentials to repository

## UI/UX Conventions
- German language labels, tooltips use `data-tooltip` attribute
- Status indicators: `status-error`, `status-ok`, `status-info` classes
- Modal dialogs: `.modal` class, `aria-hidden` attribute toggled
- Tables: `.table-wrap` for scrollable containers, sticky headers
- Charts: Chart.js with German number formatting
- Maps: Leaflet for 2D, Globe.gl for 3D hexbin visualization

## Testing
- Test files located in `/home/luis/projects/Ecom-Dash/tests/` directory
- Run tests before committing changes (command TBD based on test framework)
