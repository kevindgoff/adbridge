
**Objective:** Develop a local integration environment prior to receiving official API credentials. This setup requires a functional mock API layer backed by a persistent local data store.  Iniitially it will work with th

**Core Requirements:**

- **Database Implementation:** Initialize a **POSTGRES database** containing synthetic test data. The specific schema design is flexible, provided the structure facilitates data retrieval for all specified endpoints.
    
- **API Architecture:** Construct a service mirroring the endpoints, request structures, and response formats defined in the official documentation: [https://api.basis.net/docs/](https://api.basis.net/docs/).   Prefix /basis in front of the endpoints so that we can expand to use more endpoints from other networks. 
    
- **Data Integration:** Ensure the API service utilizes the SQLite database as the single source of truth for all dynamic responses.
    
- **Security & Auth:** Bypass or implement placeholder logic for authentication protocols, as the current scope is limited to functional integration testing.
    
---
### Tips for this workflow:

- **Schema Mapping:** Since the schema "doesn't matter," you might find it easiest to have the AI generate a simple relational structure (e.g., `Users`, `Accounts`, `Transactions`) based on the most common objects in the Basis documentation.
    
- **Containerization:** If the engineering team needs to run this easily, consider asking the AI to "Provide a Dockerfile and docker-compose.yml to orchestrate the API and db instance."
---
### Prompt example for adding a new platform ###

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