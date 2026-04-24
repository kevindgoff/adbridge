# Project Structure & Conventions

## Directory layout
```
├── app/
│   ├── main.py              # FastAPI app, startup hook, router registration
│   ├── config.py            # Reads config.yml to toggle platforms on/off
│   ├── database.py          # Schema DDL, seed functions, DB connection (get_db)
│   ├── helpers.py           # Shared pagination (cursor-based) and response formatting
│   └── routes/
│       ├── basis.py         # /basis/v1 endpoints
│       ├── dv360.py         # /dv360/v4 endpoints
│       ├── triton.py        # /triton endpoints (metrics)
│       ├── triton_booking.py# /triton-booking endpoints (TAP)
│       ├── hivestack.py     # /hivestack endpoints (DOOH)
│       └── adswizz.py       # /adswizz/v8 endpoints
├── tests/
│   ├── test_all_routes.py   # Static analysis: checks psycopg2 patterns across all routes
│   └── test_basis.py        # Basis-specific placeholder checks
├── config.yml               # Platform toggle flags
├── Dockerfile
├── docker-compose.yml        # Base (API only)
├── docker-compose.local.yml  # Local overlay (adds Postgres)
└── requirements.txt
```

## Architecture patterns

### One route file per platform
Each ad platform gets its own file in `app/routes/`. The router is conditionally included in `main.py` based on `config.yml` flags.

### Database access
- No ORM — raw SQL with psycopg2 and `%s` placeholders (never `?`)
- `get_db()` is a FastAPI dependency that yields a connection and closes it after the request
- `RealDictCursor` is used so all rows are dicts
- Schema is defined as a single `SCHEMA` string in `database.py`, split and executed statement-by-statement

### Seed data
- `init_db()` creates tables then checks if `users` is empty to decide whether to seed
- Each platform has its own `_seed_<platform>(cur, now)` function
- Core shared entities (users, agencies, clients, brands, campaigns) are seeded by `_seed_core(cur)`

### Response envelope
- List endpoints return `{"metadata": {...}, "data": [...]}`  via `list_response()`
- Single-item endpoints return `{"data": {...}}` via `single_response()`
- Hivestack uses OData-style `{"value": [...]}` with `$top/$skip/$count` via `_odata()` helper

### Pagination
- Basis/DV360/Triton/AdsWizz use cursor-based pagination (`paginate()` in `helpers.py`)
- Hivestack uses offset-based OData pagination (`_odata()` in `hivestack.py`)

### Route-level helpers
Each route file defines a local `_q(conn, sql, params)` or similar helper to execute queries via cursor. This avoids calling `conn.execute()` directly (psycopg2 connections don't support it).

## Code style
- Module docstrings at the top of each route file describing the platform and endpoints
- Section headers using comment blocks (`# ═══...` or `# ───...`)
- `Optional` from `typing` for query params
- `HTTPException(404, ...)` for not-found errors
- UUIDs as string primary keys for most entities (some platforms use integer serial IDs)
- Timestamps stored as ISO 8601 strings with `Z` suffix

## Adding a new platform
Follow the established pattern — see the "Adding a New Platform" section in `README.md` for a step-by-step checklist covering route file, database schema/seed, config toggle, app registration, and config.yml entry.
