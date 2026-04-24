# AdBridge — Product Overview

AdBridge is a mock API layer for local integration testing against ad-platform APIs. It lets developers build and test ad-platform integrations without needing live credentials or network access.

## What it does
- Serves realistic mock endpoints that mirror real ad-platform APIs (response shapes, pagination, auth)
- Seeds synthetic data on first startup so endpoints return meaningful results immediately
- Runs locally via Docker Compose (FastAPI + PostgreSQL)

## Supported platforms
| Prefix | Platform |
|---|---|
| `/basis/v1` | Basis Technologies |
| `/dv360/v4` | Google Display & Video 360 |
| `/triton` | Triton Digital Metrics |
| `/triton-booking` | Triton Digital Booking (TAP) |
| `/hivestack` | Hivestack OpenRTB 2.5 DOOH |
| `/adswizz/v8` | AdsWizz Domain API v8 |

Each platform can be toggled on/off in `config.yml` under `apis:`.

## Key behaviors
- Optional API key auth via `X-API-Key` header (set `API_KEY` env var to enable)
- Database auto-seeds when the `users` table is empty; wipe the volume to re-seed
- Swagger UI available at `/docs`, health check at `/health`
