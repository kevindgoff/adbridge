
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