# Plan: AdsWizz order push endpoints in AdBridge

#fromKIRO

- Branch: `adswizz-order-push` (from `main` @ `70108af`)
- Module: `app/routes/adswizz.py` (prefix `/adswizz/v9`)
- Source of truth: AdsWizz Domain API v9 spec, `https://docs.adswizz.com/domain-api/v9/openapi.json` (listed in `platform_api_sources.yml`, re-read 2026-10-29)
- Status: implemented on this branch (see "Decisions" and "What was built" at the end)

## Goal

An integration client should be able to push a WideOrbit order into the AdBridge AdsWizz mock using the same calls, payloads, and response shapes it would use against `https://api.adswizz.com/domain`, and read the result back. Today the mock accepts most of the writes but returns the wrong response shapes and is missing the ad-targeting calls, so a client written against the real spec can't finish a push.

## Assumed push sequence

I could not inspect the client that will do the push, so this sequence is taken from the v9 spec. Open question 1 asks you to confirm it.

| # | Step | Endpoint (v9) | Mock today |
|---|---|---|---|
| 1 | Find or create advertiser | `GET /advertisers?name=`, `POST /advertisers` | Works |
| 2 | Create order | `POST /orders` | Accepts the request, but the response has the wrong shape (see F1) |
| 3 | Duplicate guard per line | `POST /campaigns/check-duplicates` | **Missing** (405) |
| 4 | Create campaign per order line | `POST /campaigns` | Accepts the request, but the response has the wrong shape and some fields are dropped |
| 5 | Upload creative | `POST /creatives` (base64 body), or `POST /creatives/uploads/presignedurl` + `/complete` | Works |
| 6 | Create ad with creative | `POST /campaigns/{campaignId}/ads` | Works, but `creativeIdentifier` and `cappingRules` are dropped |
| 7 | Attach inventory | `POST /campaigns/{campaignId}/ads/{adId}/zone-groups` | **Missing** (404) |
| 8 | Set targeting | `PUT /campaigns/{campaignId}/ads/{adId}/targeting-rules` | **Missing** (404) |
| 9 | Launch | `PATCH /campaigns/{campaignId}?action=launch` | Works (no preconditions checked) |
| 10 | Read back / sync | `GET /orders/{id}`, `GET /orders/{id}/campaigns`, `GET /orders?modifiedAfter=` | Wrong shape; `modifiedAfter` ignored |
| 11 | Updates / cancel | `PUT /orders/{id}`, `PUT /campaigns/{id}`, `PATCH ...?action=pause`, `PUT /orders/{id}/archive` | Wrong shape; archive returns `200 null` instead of `204` |

## Findings (from running the sequence against the current mock on SQLite)

- **F1. Responses come back in snake_case.** Orders and campaigns return raw DB rows (`advertiser_id`, `start_date`, `external_reference`, `deal_id`, `campaign_type`, `created_at`). The spec's `OrderDetailsDomain` and `SavedCampaign` use camelCase (`advertiserId`, `startDate`, `externalReference`, `dealId`, `campaignType`). A client that deserializes to spec models gets nulls.
- **F2. Booleans come back as `0`/`1` on SQLite** (`archived`, `includedInCampaignObjective`). On Postgres they are `true`/`false`.
- **F3. Request fields are dropped.** These are accepted and then lost: `cappingRules` on orders, campaigns, and ads; `categoryIds`, `adClashing`, and `inventoryPack.dealId` on campaigns; `creativeIdentifier` on ads; `objective.managedObjective` on orders.
- **F4. Bad references cause a 500.** `POST /orders` with an unknown `advertiserId` raises an unhandled `IntegrityError` (500). A missing required field (`name`, `startDate`) raises a `KeyError` (500). The real API returns 400 or 404 with an `ErrorResponse`.
- **F5. The error envelope is wrong.** Errors are wrapped as `{"detail": {...}}`. The spec returns `ErrorResponse` (`code`, `message`, `errors[]`) at the top level.
- **F6. Archive and unarchive return `200` with `null`.** The spec says `204 No Content` for both orders and campaigns.
- **F7. These spec endpoints are not mocked:** `check-duplicates`, ad `zone-groups` linking, ad `targeting-rules` (GET/PUT), `GET /ads/{adId}`, `GET /ads/{adId}/targeting-rules`, ad units, and campaign duplicate (`POST /campaigns/{campaignId}`).
- **F8. The `agency` header is optional in the mock.** The spec marks it required on every call.

## Work plan

### Phase 1: Response and validation fidelity for the push resources

Scope: orders, campaigns, and ads only. Agencies, publishers, and zones keep their current shapes unless you choose otherwise in open question 2.

1. **Formatters.** Rewrite `_format_order` to emit `OrderDetailsDomain`, and add `_format_order_light` for the list endpoint (`OrderLightDomainDto`, which has no `cappingRules`). Rewrite `_format_campaign` to emit `SavedCampaign`. Keep `_format_campaign_light` for `CampaignLightDomain`. Convert all booleans with `bool()`. Remove internal columns such as `created_at` from the output.
2. **Persist the dropped fields.** Store them as JSON text columns so the same SQL works on Postgres and SQLite:
   - `aw_orders`: `capping_rules TEXT`, `managed_objective BOOLEAN`, `updated_at TEXT`
   - `aw_campaigns`: `capping_rules TEXT`, `category_ids TEXT`, `ad_clashing BOOLEAN`, `inventory_deal_id TEXT`, `updated_at TEXT`
   - `aw_ads`: `capping_rules TEXT`, `creative_identifier TEXT`

   Assign `id` values to new `CappingRule` entries on save, because the spec says "`null` for a new rule". Following the spec, a `null` or empty array clears existing rules.
3. **Validation, returning 400 or 404 instead of 500:**
   - Required fields per spec: order `name`, `advertiserId`, `startDate`, `objective.currency`. Campaign `name`, `campaignType`, `advertiserId`, `campaignRevenue`, `billing`, `objective`, `campaignDeliveryPacing`.
   - Return 404 when `advertiserId` or `orderId` refers to a missing row. Return 400 when the campaign's `advertiserId` doesn't match the order's.
   - Enums: objective `type` (IMPRESSIONS, PRICE), `billing` (SOLD, UNSOLD), pacing `type` (EVENLY, AS_FAST_AS_POSSIBLE, PREDICTIVE), revenue `type` (CPM), capping `unit` (MINUTES, HOURS, DAYS, WEEKS, MONTHS, EPISODE).
   - Return 400 when `endDate` is before `startDate`.
   - Ads: return 404 when the campaign doesn't exist.
4. **Error envelope.** Give the AdsWizz router a custom `APIRoute` subclass (`route_class=`) that turns any `HTTPException` into a top-level `ErrorResponse`. This handles 404s, 405s, and FastAPI 422 validation errors (mapped to 400). The fix stays scoped to `/adswizz/v9`, so other platforms and `main.py` are untouched, and the test fixture's bare `FastAPI()` app gets it automatically.
5. **Status codes.** Order and campaign archive/unarchive return `Response(status_code=204)`.
6. **`GET /orders?modifiedAfter=`.** Filter on `updated_at`, which is set on every insert and update.

### Phase 2: Missing push endpoints

New tables:

```sql
CREATE TABLE IF NOT EXISTS aw_ad_zone_groups (
    ad_id INTEGER NOT NULL REFERENCES aw_ads(id),
    zone_group_id INTEGER NOT NULL REFERENCES aw_zone_groups(id),
    PRIMARY KEY (ad_id, zone_group_id)
);

CREATE TABLE IF NOT EXISTS aw_ad_targeting (
    ad_id INTEGER PRIMARY KEY REFERENCES aw_ads(id),
    zone_ids TEXT,                 -- JSON int array
    zone_group_ids TEXT,           -- JSON int array
    delivery_instructions TEXT,    -- JSON object
    rules TEXT,                    -- JSON array, stored as sent
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_ad_units (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES aw_campaigns(id),
    ad_ids TEXT NOT NULL,          -- JSON int array
    created_at TEXT NOT NULL
);
```

| Endpoint | Behaviour |
|---|---|
| `POST /campaigns/check-duplicates` | Takes `CampaignCheckForDuplicatesRequest` and returns `ValidationResult`. Returns `valid: false` with one `validationEvent` per overlapping campaign: same advertiser, overlapping dates, and matching `objectiveType`/`objectiveValue` when those are given. |
| `POST /campaigns/{campaignId}/ads/{adId}/zone-groups` | Body is an int array. Validates that the ad belongs to the campaign and that every zone group exists (404 otherwise). Replaces the ad's links and echoes the array back. |
| `GET /campaigns/{campaignId}/ads/{adId}/targeting-rules` | Returns `TargetingRulesContainer`. `zoneGroupIds` merges the stored value with the zone-group links. An ad with nothing set returns empty arrays. |
| `PUT /campaigns/{campaignId}/ads/{adId}/targeting-rules` | Validates that `zoneIds` and `zoneGroupIds` exist and that each rule has a `type` from the spec enum. Stores `rules` as sent and returns the saved container. |
| `GET /ads/{adId}` and `GET /ads/{adId}/targeting-rules` | Same data, looked up by ad only. |
| `GET/POST /campaigns/{campaignId}/ad-units`, `PUT/DELETE .../ad-units/{adUnitId}` | Stores `SavedAdUnit` (`adIds` needs at least 2 ads, all in the campaign) and updates `aw_ads.ad_unit_id`. Optional; see open question 4. |
| `POST /campaigns/{campaignId}` (duplicate) | Copies the campaign and its ads into a new campaign with `DRAFT` status and returns `201 SavedCampaign`. Optional; see open question 4. |

Small related fixes:
- `POST /creatives` returns an identifier with the `_ad_` prefix, matching the multipart flow, and stores it so a later `POST .../ads` that sends `creativeIdentifier` can be checked. Whether an unknown identifier should be rejected is open question 5.
- `GET /targeting-zones` honours `zoneGroupIds` and `creativeType`.

### Phase 3: Launch preconditions (optional, see open question 3)

`PATCH ?action=launch` would return 400 when the campaign has no ACTIVE ads or an ad has no zones or zone groups. It would also check status transitions: `launch` only from DRAFT/READY/PAUSED, and `pause` only from RUNNING. This makes the mock catch the mistakes the real API would reject, but it could break clients that launch an empty campaign on purpose in tests.

### Phase 4: `agency` header (optional, see open question 6)

Require the `agency` header on all AdsWizz routes (400 `ErrorResponse` if missing) and scope advertiser and order lookups to that agency. Off by default behind an env flag (`ADSWIZZ_REQUIRE_AGENCY=true`) so existing callers keep working.

### Phase 5: Tests and docs

- `tests/test_adswizz.py`:
  - An end-to-end push test that runs steps 1–11 and checks camelCase shapes, persisted `cappingRules`, `categoryIds`, and `creativeIdentifier`, targeting round-trip, launch, archive returning 204, and `modifiedAfter`.
  - Negative cases: unknown advertiser returns 404, missing required field returns 400, and the error body is a top-level `ErrorResponse`.
  - Add the new routes to the route-registration test.
  - Update existing assertions that depend on the old snake_case fields. `test_advertiser_domain_required` reads `agency_id`; it only needs to change if advertisers are converted too.
- `database.py`: add the DDL above, extend `_migrate_adswizz_v9` with the new columns so existing volumes upgrade without a wipe, and seed zone-group links and targeting for one seeded ad.
- `adswizz.py` module docstring, `README.md` AdsWizz section, and the `platform_api_sources.yml` `retrieved` date.
- `tests/test_all_routes.py` static checks must still pass (`%s` placeholders, no `conn.execute`).

## Verification

1. `pytest` passes. `pytest` and `httpx` are not in `requirements.txt`; I installed them into `.venv` to run the baseline, which is 14 passing tests.
2. Run the order-push sequence against both backends: SQLite through the test fixture, and Postgres through `docker compose -f docker-compose.yml -f docker-compose.local.yml up --build -d`.
3. Spot-check a few responses against the spec schemas (`OrderDetailsDomain`, `SavedCampaign`, `SavedAudioAd`, `TargetingRulesContainer`, `ValidationResult`) by loading the spec JSON and comparing property names.

## Out of scope

- Reporting (`/reports/*`), attribution, deals and deal library, supply packs, briefed or interactive creatives, rendering instructions, and geo/commons lookups. The targeting rule `values` endpoints are also out of scope; rules are stored as sent.
- Changing other platforms' error envelopes or response shapes.

## Open questions

1. **Which client does the push, and in what order?** Is it the WODH AdsWizz integration or a new UIL adapter? I tried to search the WO-FusionDigital repos for its AdsWizz calls and that search was declined, so the sequence above comes from the spec. If you can point me at the client's call list, or allow that search, I'll align the plan to it.
2. **Should all AdsWizz responses use camelCase, or only orders, campaigns, and ads?** Converting agencies, advertisers, publishers, and zones too would make the whole module match the spec. It would also change responses that existing consumers of the mock may rely on, such as `agency_id` on advertisers.
3. **Should launch enforce preconditions (Phase 3)?** Or should the mock stay permissive?
4. **Do you need ad units and campaign duplicate?** These are used for companion audio and display ads and for cloning lines. I'd skip them unless the push uses them.
5. **Should an ad with an unknown `creativeIdentifier` be rejected?** Or accepted as is?
6. **Should the `agency` header be required (Phase 4)?** It would sit behind an env flag, off by default.

## Decisions (2026-10-29)

1. **Client sequence:** not needed. The mock should work from the Swagger page, so the push endpoints ship with example request bodies that work against the seed data.
2. **Don't break existing users.** Response changes only add fields. Orders, campaigns, and ads keep their snake_case keys and also return the spec's camelCase keys. Error bodies keep `detail` and also return `code`, `message`, and `errors` at the top level. Archive/unarchive keep returning `200`, not the spec's `204`. Advertisers, agencies, publishers, and zones are unchanged. A full switch to the spec shape can come later.
3. **Launch stays permissive.** Phase 3 is dropped.
4. **Ad units and campaign duplicate are skipped.**
5. **No creative work.** `creativeIdentifier` is not stored or checked, and `POST /creatives` is unchanged.
6. **`agency` header:** stays optional, and no env flag was added. Requiring it would add a step to every Swagger call without catching any real push bugs. The flag can be added later if a client needs the check.

## What was built

- `app/routes/adswizz.py`
  - New endpoints: `POST /campaigns/check-duplicates`, `POST /campaigns/{id}/ads/{adId}/zone-groups`, `GET/PUT /campaigns/{id}/ads/{adId}/targeting-rules`, `GET /ads/{adId}`, `GET /ads/{adId}/targeting-rules`.
  - `GET /orders?modifiedAfter=` and `GET /targeting-zones?zoneGroupIds=&creativeType=` filters.
  - `cappingRules` on orders, campaigns, and ads. `categoryIds`, `adClashing`, and `inventoryPack.dealId` on campaigns. `objective.managedObjective` on orders.
  - Validation for required fields, enums, date order, and unknown ids. These return 400 or 404 instead of 500.
  - The error envelope is applied through a router-scoped `APIRoute` subclass, so other platforms are untouched.
- `app/database.py`
  - New tables `aw_capping_rules` and `aw_ad_targeting`, plus new columns on `aw_orders` and `aw_campaigns`.
  - `_migrate_adswizz_v9` adds the columns to existing databases and backfills `updated_at`.
  - The seed gives one ad targeting, and seeded pacing uses `AS_FAST_AS_POSSIBLE` instead of `ASAP`. Existing `ASAP` rows are still accepted on update.
- `tests/test_adswizz.py`: tests for an end-to-end push, validation errors, backward compatibility of seeded data, and the targeting-zone filters.

## Verification done

- `pytest`: 23 passed on SQLite.
- Upgrade path: a database seeded by `main` was opened with this branch, on both SQLite and Postgres 16 (throwaway container). The push tests pass on both.
- Swagger: `/docs` loads, the new routes appear, and every example body succeeds when run in push order.

## Found along the way (not fixed, separate from this work)

- On `main`, `init_db()` fails on a **fresh Postgres database**. `line_items` references `properties(id)` (and `vendors(id)`) before those tables are created in `SCHEMA`. SQLite doesn't enforce the order, which is why the tests pass. Existing Postgres volumes that were already initialised are not affected.
- `pytest` and `httpx` are needed to run the tests but are not in `requirements.txt`.
