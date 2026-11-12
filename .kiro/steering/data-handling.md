# Client Data Handling

## Rule: sanitize client data imported from live APIs
Any record pulled from a live ad-platform API is client data. It must be sanitized before it is stored in the AdBridge database, served by an endpoint, committed, or built into an image.

- **Replace:** names, descriptions, street addresses, people's names, codes, external IDs, logo URLs, domains, emails, phone numbers and UUIDs. Use generic labels built from the record's own numeric ID (for example `Sample Site 277426`), so the result is stable across imports and can't be reversed.
- **Coarsen:** venue coordinates, rounded to 2 decimals (about 1 km).
- **Keep:** numeric IDs and the references between them, organization and owner IDs, timestamps, flags, technical specs, and public geography such as country, region and city.
- **Fail closed:** an entity with no sanitization rule must be rejected on import, not loaded raw.
- **Raw captures stay local:** keep them in a git-ignored folder (`data/<platform>/`) that is also listed in `.dockerignore`. Never quote raw client values in code, tests, docs or commit messages. Refer to "the capture" or "the client" instead.
- **Test it:** every import needs a test asserting that no raw identifying value appears in API responses.

## Platforms currently affected
| Platform | Capture folder | Sanitizer |
|---|---|---|
| Hivestack | `data/hivestack/sample/` | `app/hivestack_sanitize.py`, applied in `_load_hivestack_capture` |

No other platform imports live data yet. A new importer must add a sanitizer and a row here.
