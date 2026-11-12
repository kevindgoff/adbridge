# Plan: import Hivestack sample data into AdBridge
#fromKIRO
- Module: `app/routes/hivestack.py` (prefix `/hivestack`), tables `hs_*` in `app/database.py`
- Contract: `app/specs/hivestack.openapi.json`
- Source data: `data/hivestack/sample/`, copied from `OneDrive - WideOrbit\Desktop\hivestack\sample-data-hivestack` (captured 2026-11-10 19:53 UTC)
- Status: implemented (see "Decisions" and "What was built" at the end)

## Goal
On startup, AdBridge should serve the captured Hivestack records from `/hivestack/*` so that a GET returns the same JSON the live API returned. Resources the capture doesn't cover still get usable data.

## What the capture contains
Each file is `{"_meta": {...}, "records": [...]}`. The `records` are the live API objects. `_meta` describes the capture, and the loader can ignore it. `sample_summary.json` reports no dangling references.

| File | Records | ID range | Fits `hs_*` table | Matches spec model |
|---|---|---|---|---|
| `accounts.json` | 10 | 1046–1839 | yes, every key is a column | yes |
| `advertisers.json` | 10 | 2173–3742 | yes | yes |
| `networks.json` | 1 | 608303 | yes | yes |
| `sites.json` | 10 | 277426–277435 | yes | yes |
| `units.json` | 10 | 18597472–18597481 | yes | yes |
| `locations.json` | 11 | 10313–286680 | yes; `hierarchy`, `archived`, `version` not in file | yes |

I checked every record against the spec models and the table columns. There are no type errors, no unknown fields, and no missing required fields.

Other facts that shape the import:
- **One organization.** Every record belongs to one client organization/owner. The synthetic seed uses organization `5021` and owners `701`–`705`.
- **References resolve.** Units point at the 10 sites, the 1 network and 7 of the locations. Sites point at the same 7 locations. Locations form a Canada > Ontario/Quebec > city tree.
- **No ID collisions** with the synthetic seed for these six tables.
- **Media types are missing.** All units use `mediatype_id` 121 (`retail.mall`, parent 101). The synthetic seed already has a 101 with a different meaning (`outdoor.billboards`), so loading the real data next to the synthetic media types would give a wrong tree.
- **No buy-side activity.** There are no campaigns, line items, creatives, line-item creatives, unit packs, demographics, languages, unit languages, unit properties or reports. The accounts and advertisers have nothing attached to them.
- **It is real customer data:** client names, street addresses and logo URLs.

## Proposed design

### 1. Loader
Add `_load_hivestack_capture(cur, directory)` to `app/database.py`.
- It reads `<entity>.json` and inserts `records` in dependency order: locations, networks, sites, units, accounts, advertisers.
- Values are written with `_hs_insert`, using `ON CONFLICT (<pk>) DO UPDATE`, so running it again refreshes the rows.
- Nested and always-null fields are JSON-encoded with the same field lists the routes use. Today those lists are defined twice (seed and `_Resource` definitions in the routes). I'd move them into one small module, `app/hivestack_model.py`, that both files import. That avoids a circular import, since the routes already import `database`.
- Files it doesn't recognise are skipped with a log line. A later capture of campaigns or line items then only needs a mapping entry added.

### 2. Rows the loader derives
- **Media types:** taken from the `media_type` object embedded in each unit. For a parent that isn't in the data (101), insert a stub built from the code prefix (`retail` → "Retail", `parent_mediatype_id` null) so `/mediatypes` returns a proper tree. A real `/mediatypes` capture would replace this. The spec says the live list has 210 records.
- **Location extras:** copy `hierarchy`, `archived` and `version` from the location embedded in units (this covers 7 of the 11 rows). For the other 4, build `hierarchy` from the `region_id` / `country_id` chain and set `archived` false, `version` null.

### 3. Seed mode
Add a setting in `config.yml`:

```yaml
hivestack:
  seed: sample                    # sample | synthetic
  sample_dir: data/hivestack/sample
```

- `synthetic` keeps today's behaviour.
- `sample` does the following:
  - loads the capture;
  - skips the synthetic rows for the six covered resources and for media types;
  - keeps synthetic languages, demographics and RTB data, which don't conflict;
  - sets the organization used for new POST records to the captured organization.

### 4. Buy-side data in sample mode
Generate synthetic campaigns, line items, creatives and line-item creatives, plus a unit pack, that reference the real advertiser, account, unit and owner IDs. Then campaign `$expand`, `/lineitems/{id}/units` and the ad-serving endpoints all return real inventory. The existing `_seed_hivestack_v2` generator would take its parent IDs as parameters instead of hard-coding them.

### 5. Switching an existing database
Seeding only runs when `hs_accounts` is empty, so changing the mode does nothing to a database that is already seeded. Add a command:

```
python -m app.hivestack_import [--dir data/hivestack/sample] [--replace]
```

`--replace` empties the v2 `hs_*` tables (not the RTB tables) and reloads them in the configured mode. Without `--replace`, the command upserts.

### 6. Verification
- A pytest test loads the sample into a temporary SQLite database. For every record, it checks that `GET /hivestack/<entity>/<id>` returns exactly the source object, including key order.
- It also checks that list counts and `x-total-count` match the file counts, and that the synthetic buy-side rows only reference IDs that exist.
- I'd run the same test on Postgres once Docker is available.

## Open questions
1. **Commit the data?** The files contain the client's names, addresses and logo URLs. Options:
   - (a) commit as-is;
   - (b) add `data/hivestack/` to `.gitignore` and keep the files local;
   - (c) commit a scrubbed copy, with names and addresses replaced and IDs and structure kept.

   I'd pick (b) or (c) unless the team is already cleared to share this data.
2. **Default mode.** Should `sample` be the default, or opt-in?
3. **Buy side.** Is synthetic data on top of real IDs (section 4) acceptable, or will you capture `/campaigns`, `/lineitems`, `/creatives` and `/lineitemcreatives` next?
4. **Media types.** Can you capture `/mediatypes`? That removes the stub in section 2.

## Decisions
1. **Data stays local.** `data/hivestack/` is in `.gitignore`. Anyone without the files gets synthetic data, because sample mode falls back automatically.
2. **Sample mode is the default** in `config.yml` (`hivestack.seed: sample`).
3. **Buy side is synthetic, built on real IDs**, until campaigns, line items and creatives are captured.
4. **Media types:** derived from the units, with a stub parent (101 `retail`) until `/mediatypes` is captured.

## What was built
| File | Change |
|---|---|
| `app/hivestack_model.py` | New. JSON-column lists, the capture-file mapping and the v2 table list. The seed code and the routes both read these, so the lists are defined once. |
| `app/config.py` | `get_hivestack_settings()`. The `HIVESTACK_SEED` and `HIVESTACK_SAMPLE_DIR` env vars override `config.yml`. |
| `app/database.py` | `_load_hivestack_capture` (upserts, then derives media types and location extras). The seed is split into `_seed_hivestack_v2_synthetic` (what a capture replaces) and `_seed_hivestack_v2_dependents` (built from whatever units, advertisers and accounts are in the tables). `HS_ORGANIZATION_ID` comes from the capture, or 5021 in synthetic mode. |
| `app/hivestack_import.py` | New. `python -m app.hivestack_import [--dir PATH] [--replace]` |
| `tests/test_hivestack_sample.py` | New. Every captured record round-trips through `GET /hivestack/<entity>/<id>`. Also covers list counts, the media type tree, references from the synthetic buy side, and the organization used for new records. Skipped when the capture is absent. |

Notes:
- The comparison in the test is on values. JSON key order is not compared.
- Running the import without `--replace` on a synthetic database leaves both datasets side by side, and the synthetic media type 101 is kept. Use `--replace` to switch.
- Tested on SQLite only. The upserts use `ON CONFLICT ... DO UPDATE`, which Postgres also supports, but this hasn't been run against Postgres.

## Sanitization
Captured records are sanitized on import by `app/hivestack_sanitize.py` (rule: `.kiro/steering/data-handling.md`). Names, descriptions, addresses, codes, external IDs, logos, domains and UUIDs are replaced with generic values derived from each record's numeric ID. Coordinates are rounded to 2 decimals. IDs, references and public geography are kept. Entities without a rule are rejected.
