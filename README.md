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

```bash
docker compose up --build
```

This starts the PostgreSQL-compatible database proxy and the API service on **http://localhost:8000**.

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
