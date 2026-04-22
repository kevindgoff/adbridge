# AdBridge – Local Ad Platform API Mock

A mock API layer for local integration testing against ad-platform APIs. Built with **FastAPI** and backed by **PostgreSQL**, it seeds realistic synthetic data on first startup so you can develop and test without needing live credentials.

## Supported Platforms

| Prefix | Platform |
|---|---|
| `/basis` | Basis Technologies |
| `/dv360` | Google Display & Video 360 |
| `/triton` | Triton Digital Metrics |
| `/triton-booking` | Triton Digital Booking (TAP) |
| `/hivestack` | Hivestack OpenRTB 2.5 DOOH |

Each platform can be toggled on or off in `config.yml`:

```yaml
apis:
  basis: true
  dv360: true
  triton: true
  hivestack: true
```

## Quick Start

### 1. Environment

Copy the example env file and adjust values as needed:

```bash
cp .env.example .env
```

### 2. Run with Docker Compose

Two modes are available — pick one.

**Local Postgres** (spins up a Postgres container, no cloud credentials needed):

```bash
cp .env.local.example .env
docker compose -f docker-compose.yml -f docker-compose.local.yml up --build
```

**Cloud SQL** (connects via the GCP Cloud SQL Auth Proxy — requires the service account JSON):

```bash
cp .env.cloudsql.example .env
# fill in POSTGRES_PASSWORD in .env
docker compose -f docker-compose.yml -f docker-compose.cloudsql.yml up --build
```

Both start the API on **http://localhost:8000**.

### 3. Run Locally (without Docker)

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Make sure the Postgres connection variables in `.env` point to a running instance.

## API Key Authentication

Authentication is opt-in. To enable it, set the `API_KEY` environment variable:

```
API_KEY=my-secret-key
```

When set, every request must include the key in the `X-API-Key` header:

```bash
curl -H "X-API-Key: my-secret-key" http://localhost:8000/health
```

If `API_KEY` is not set or is empty, all requests are allowed without authentication.

## Interactive Docs

FastAPI auto-generates interactive API documentation:

- Swagger UI: **http://localhost:8000/docs**
- ReDoc: **http://localhost:8000/redoc**

## Health Check

```
GET /health  →  {"status": "ok"}
```

## Project Structure

```
├── app/
│   ├── main.py          # FastAPI app, middleware, router registration
│   ├── config.py         # Reads config.yml to toggle platforms
│   ├── database.py       # Schema definitions, seed data, DB helpers
│   ├── helpers.py        # Pagination and response formatting utilities
│   └── routes/           # One module per platform
│       ├── basis.py
│       ├── dv360.py
│       ├── triton.py
│       ├── triton_booking.py
│       └── hivestack.py
├── tests/
├── config.yml            # Enable/disable individual platform APIs
├── .env.example          # Environment variable template
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `POSTGRES_HOST` | Yes | Database host |
| `POSTGRES_PORT` | No | Database port (default `5432`) |
| `POSTGRES_DB` | Yes | Database name |
| `POSTGRES_USER` | Yes | Database user |
| `POSTGRES_PASSWORD` | Yes | Database password |
| `API_KEY` | No | If set, requires `X-API-Key` header on all requests |
| `ADBRIDGE_CONFIG_PATH` | No | Path to config file (default `config.yml`) |


## Prompt Example for adding a new platform
Add a new ad platform mock called AudioWizz to the AdBridge application. Use the API reference at <PASTE_URL_HERE> to understand the platform's endpoints, entities, request/response shapes, pagination style, and authentication model.

Follow the exact patterns established by the existing platform integrations (Basis, DV360, Triton, Hivestack). Specifically:

1. Route file — audiowizz.py

    Create a new APIRouter with an appropriate prefix (e.g. /audiowizz or /audiowizz/v1).
    Implement mock endpoints for every major resource in the API reference (CRUD where the real API supports it, read-only otherwise).
    Use the same internal helper conventions: _q(conn, sql, params) for queries, Depends(get_db) for connections, HTTPException for errors.
    Match the real API's pagination style. If it uses cursor-based pagination, follow the Basis pattern. If offset-based, follow Triton Booking's start/limit/sort or DV360's pageToken/pageSize. If OData, follow Hivestack's $top/$skip/$count.
    Match the real API's response envelope exactly (e.g. {"data": [...]}, {"items": [...]}, {"results": [...]}, etc.).
    Where the real API nests objects (budgets, targeting, goals, etc.), write small _nest_* or _format_* helpers to reshape flat DB rows into the correct nested shape — same approach as DV360's _nest_budget, _nest_pacing, etc.
    Include a placeholder auth/token endpoint if the real API uses OAuth or token-based auth (like Basis's /oauth/token).

2. Database schema & seed data — database.py

    Add CREATE TABLE IF NOT EXISTS statements to the SCHEMA string for every AudioWizz entity. Prefix all table names with aw_ (matching the dv360_, tap_, hs_, etc. convention).
    Column types should mirror the real API's field types: TEXT for strings/IDs/timestamps, INTEGER/REAL/BIGINT for numbers, BOOLEAN for flags.
    Add synthetic seed data inside init_db() that creates a realistic-looking dataset — enough rows to exercise pagination and parent-child relationships. Follow the volume and style of the existing seeds (e.g. 3-5 top-level entities, 2-4 children each, etc.).

3. Config toggle — config.py

    Add "audiowizz" to the get_enabled_apis() dict with a default of True.

4. App registration — main.py

    Add an OpenAPI tag entry for AudioWizz in tags_metadata.
    Add a conditional include_router block gated on _enabled.get("audiowizz"), following the same pattern as the other platforms.

5. Config file — config.yml

    Add audiowizz: true under the apis: key.
    Do not modify any existing platform's routes, schema, or seed data. Do not add tests unless I ask. Keep the implementation minimal — only mock what the API reference documents.

---