# AdBridge – Local Ad Platform API Mock

A mock API layer for local integration testing against ad-platform APIs. Built with **FastAPI** and backed by **PostgreSQL**, it seeds realistic synthetic data on first startup so you can develop and test without needing live credentials.

## Supported Platforms

| Prefix | Platform |
|---|---|
| `/basis/v1` | Basis Technologies |
| `/dv360/v4` | Google Display & Video 360 |
| `/triton` | Triton Digital Metrics |
| `/triton-booking` | Triton Digital Booking (TAP) |
| `/hivestack` | Hivestack OpenRTB 2.5 DOOH |
| `/adswizz/v8` | AdsWizz Domain API v8 |

Each platform can be toggled on or off in `config.yml`:

```yaml
apis:
  basis: true
  dv360: true
  triton: true
  freewheel: true
  hivestack: true
  adswizz: true
```

---

## Local Setup (Docker Compose)

### Prerequisites

- **Docker Desktop** installed and running (verify with `docker --version`)
- **Port 8000** (API) and **5432** (Postgres) available on your machine

### Step-by-step

**1. Clone the repo and cd into it**

```bash
git clone <repo-url>
cd adbridge
```

**2. Create your `.env` file**

Copy the local example — it has the right defaults for Docker networking:

```bash
cp .env.local.example .env
```

This gives you:

```
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=adbridge
POSTGRES_USER=adbridge
POSTGRES_PASSWORD=adbridge
API_KEY=
```

> `POSTGRES_HOST=db` is required — it matches the Docker Compose service name.
> Leave `API_KEY` blank to skip authentication, or set it to any string to require `X-API-Key` on every request.

**3. Build and start the containers**

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml up --build -d
```

> **PowerShell users:** This command may show a red error exit code even when it succeeds. Docker Compose writes progress to stderr, and PowerShell treats that as an error. Ignore the exit code — check container status instead (next step).

**4. Verify both containers are running**

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml ps
```

You should see two containers with status `Up`:

```
NAME             IMAGE                SERVICE   STATUS
adbridge-api-1   adbridge-api         api       Up
adbridge-db-1    postgres:16-alpine   db        Up
```

**5. Wait a few seconds, then check the health endpoint**

The API waits for Postgres to be ready (retries up to 20 seconds), then creates tables and seeds data on first boot.

```bash
curl http://localhost:8000/health
```

Expected: `{"status":"ok"}`

> PowerShell equivalent: `Invoke-RestMethod http://localhost:8000/health`

**6. Open the interactive docs**

Browse to **http://localhost:8000/docs** — all platform endpoints are listed in Swagger UI.

---

## Troubleshooting

### "network not found" error

Docker has a stale network reference. Fix it:

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml down --remove-orphans
docker network prune -f
docker compose -f docker-compose.yml -f docker-compose.local.yml up --build -d
```

### API starts but returns 500 / connection refused

Postgres wasn't ready in time. The API retries automatically on startup, but if it still fails:

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml restart api
```

### Seed data is missing for a platform (empty responses)

The database only seeds once — when the `users` table is empty. If you added a new platform after the first boot, you need to wipe the volume and re-seed:

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml down -v
docker compose -f docker-compose.yml -f docker-compose.local.yml up --build -d
```

> The `-v` flag deletes the Postgres data volume. All data will be regenerated from scratch.

### Port already in use

Something else is using 8000 or 5432. Either stop the other process or change the port mapping in `docker-compose.yml` / `docker-compose.local.yml`:

```yaml
ports:
  - "9000:8000"   # maps localhost:9000 → container:8000
```

---

## Stopping the app

```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml down
```

Add `-v` to also delete the database volume (next startup will re-seed).

---

## Running without Docker

```bash
pip install -r requirements.txt
```

Make sure a Postgres instance is running and update `.env` to point at it:

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=adbridge
POSTGRES_USER=adbridge
POSTGRES_PASSWORD=adbridge
```

Then start the server:

```bash
uvicorn app.main:app --reload
```

---

## API Key Authentication

Optional. Set `API_KEY` in `.env` to any value to enable it:

```
API_KEY=my-secret-key
```

Then include the header on every request:

```bash
curl -H "X-API-Key: my-secret-key" http://localhost:8000/health
```

If `API_KEY` is blank or unset, all requests pass through without auth.

---

## Project Structure

```
├── app/
│   ├── main.py              # FastAPI app, startup, router registration
│   ├── config.py            # Reads config.yml to toggle platforms
│   ├── database.py          # Schema, seed data, DB connection
│   ├── helpers.py           # Pagination and response formatting
│   └── routes/
│       ├── basis.py
│       ├── dv360.py
│       ├── triton.py
│       ├── triton_booking.py
│       ├── hivestack.py
│       └── adswizz.py
├── tests/
├── config.yml               # Enable/disable platform APIs
├── .env.local.example       # Env template for local Docker
├── .env.cloudsql.example    # Env template for Cloud SQL
├── Dockerfile
├── docker-compose.yml       # Base compose (API service)
├── docker-compose.local.yml # Local Postgres overlay
└── requirements.txt
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `POSTGRES_HOST` | Yes | Database host (`db` for Docker, `localhost` for bare metal) |
| `POSTGRES_PORT` | No | Database port (default `5432`) |
| `POSTGRES_DB` | Yes | Database name |
| `POSTGRES_USER` | Yes | Database user |
| `POSTGRES_PASSWORD` | Yes | Database password |
| `API_KEY` | No | If set, requires `X-API-Key` header on all requests |
| `ADBRIDGE_CONFIG_PATH` | No | Path to config file (default `config.yml`) |


---

## Adding a New Platform

Use this prompt template to add any new ad platform mock. Replace the placeholder URL with the platform's API reference:

> Add a new ad platform mock called **[PlatformName]** to the AdBridge application. Use the API reference at `<PASTE_URL_HERE>` to understand the platform's endpoints, entities, request/response shapes, pagination style, and authentication model.
>
> Follow the exact patterns established by the existing platform integrations (Basis, DV360, Triton, Hivestack, AdsWizz). Specifically:
>
> 1. **Route file** — `app/routes/platformname.py`: Create an `APIRouter` with the appropriate prefix. Implement mock CRUD endpoints matching the real API's pagination style and response envelope. Use `_q()`, `Depends(get_db)`, `HTTPException` helpers.
>
> 2. **Database schema & seed data** — `app/database.py`: Add `CREATE TABLE IF NOT EXISTS` statements with a consistent table prefix (e.g. `pn_`). Add synthetic seed data in a `_seed_platformname()` function called from `init_db()`.
>
> 3. **Config toggle** — `app/config.py`: Add `"platformname"` to `get_enabled_apis()` with default `True`.
>
> 4. **App registration** — `app/main.py`: Add an OpenAPI tag and conditional `include_router` block.
>
> 5. **Config file** — `config.yml`: Add `platformname: true` under `apis:`.
>
> Do not modify existing platforms. Do not add tests unless asked.
