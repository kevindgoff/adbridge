# Tech Stack & Build

## Language & Runtime
- Python 3.12
- No type stubs or mypy configured

## Framework & Libraries
| Package | Version | Purpose |
|---|---|---|
| FastAPI | 0.115.6 | Web framework / API |
| Uvicorn | 0.34.0 | ASGI server |
| Pydantic | 2.10.4 | Data validation (used by FastAPI) |
| psycopg2-binary | 2.9.10 | PostgreSQL driver |
| python-dotenv | 1.0.1 | `.env` file loading |
| PyYAML | 6.0.2 | `config.yml` parsing |

## Database
- PostgreSQL 16 (Alpine image in Docker)
- Raw SQL via psycopg2 — no ORM
- `RealDictCursor` used everywhere so rows come back as dicts

## Containerization
- `Dockerfile`: Python 3.12-slim, installs from `requirements.txt`, runs uvicorn
- `docker-compose.yml`: Base compose (API service only)
- `docker-compose.local.yml`: Overlay that adds a local Postgres service + volume

## Common Commands

```bash
# Build and start locally (API + Postgres)
docker compose -f docker-compose.yml -f docker-compose.local.yml up --build -d

# Check container status
docker compose -f docker-compose.yml -f docker-compose.local.yml ps

# Stop
docker compose -f docker-compose.yml -f docker-compose.local.yml down

# Stop and wipe DB (forces re-seed on next start)
docker compose -f docker-compose.yml -f docker-compose.local.yml down -v

# Run without Docker (requires a running Postgres instance)
pip install -r requirements.txt
uvicorn app.main:app --reload

# Run tests
pytest
```

## Environment Variables
Configured via `.env` file (see `.env.local.example`):
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` — DB connection
- `API_KEY` — optional; if set, all requests require `X-API-Key` header
- `ADBRIDGE_CONFIG_PATH` — optional; path to config file (default `config.yml`)
