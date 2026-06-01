# Clients API package

Routes are split by concern; `__init__.py` aggregates sub-routers into a single `router` mounted at `/clients` in `app/main.py`.

## Registration order (required)

Register **static and collection paths before** `/{client_id}` routes so FastAPI does not treat path segments like `terminal-summary` as client IDs.

Current order in `__init__.py`:

1. `terminal` — `/calendar/monthly-coaching-metrics`, `/terminal/monthly-trends`, `/terminal-summary`
2. `insights` — `/health-scores`, `/call-insight-tags`, `/{client_id}/call-insights`, …
3. `automation` — `/automation/process`
4. `checkins` — `/check-ins/sync`, `/{client_id}/check-ins`, …
5. `payments` — `/{client_id}/payments`, manual payment CRUD
6. `crud` — `/merge`, `/{client_id}` (get, patch, delete). List/create are on the package router via `add_api_route("", …)` (FastAPI cannot nest empty paths on sub-routers).

`parse_client_uuid()` in `helpers.py` returns 404 for non-UUID path segments so static paths misrouted to `/{client_id}` never hit Postgres with invalid UUID casts.

## Shared helpers

`helpers.py` holds org scoping, check-in sync worker glue, Brevo stats, email normalization, and merge metadata helpers. Terminal trend math lives in `app/services/terminal_metrics_service.py` (not in route modules).
