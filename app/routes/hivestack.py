"""Hivestack API v2 mock endpoints — all resources under /hivestack.

Mirrors: https://apps.hivestack.com/api/v2/
Base path here: /hivestack (prefixed in router)
Contract: app/specs/hivestack.openapi.json. Field shapes in that file were
inferred from live GET /api/v2/* responses sampled 2026-11-10. main.py serves
it as the /hivestack part of /openapi.json via apply_openapi_spec().

Live-API conventions reproduced here:
  - List endpoints return a bare JSON array (no envelope).
  - Paging uses $top / $skip. $count=true adds an x-total-count header.
    Lists also send x-page-offset (echoes $skip) and x-page-size (echoes
    $top, omitted when no $top applies). Inventory lists (networks, sites,
    units, unitpacks, demographics) have no default $top and return all rows.
  - IDs are integers named <resource>_id (account_id, campaign_id, unit_id...).
  - Timestamps are ISO 8601 with milliseconds and no UTC offset.
  - Nested objects/arrays live in JSON text columns and are decoded on read.

Sections:
  Ad Serving      – /adserving/units/{uuid}/schedulevast  (VAST), upcoming creatives
  Avails          – /avails
  Accounts        – /accounts
  Advertisers     – /advertisers
  Campaigns       – /campaigns
  Line Items      – /lineitems
  Creatives       – /creatives  (+ files, approvals, line-item associations)
  Networks        – /networks
  Sites           – /sites  (+ /sites/external/{external_id})
  Units           – /units  (+ /units/external/{external_id})
  Unit Packs      – /unitpacks  (+ units, targeting)
  Demographics    – /demographics
  Languages       – /languages
  Locations       – /locations
  Mediatypes      – /mediatypes  (tree)
  Unit Languages  – /unitlanguages
  Unit Properties – /unitproperties
  Concentrations  – /concentrations
  Report Defs     – /reportdefinitions
  Report Execs    – /reportexecutions
  Custom Reports  – /customreports
  Media Owners    – /mediaowners
  Events          – /customevents, /eventdata
  Deals (DSP)     – /dsp/creatives, /dsp/deals
  OpenRTB         – /openrtb/bidrequests
  Tracking        – /win  /bill  /imp  /loss  /impressions
  Sellers.json    – /sellers.json
"""

import copy
import json
import re
import uuid as _uuid_mod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from app.database import HS_ORGANIZATION_ID, get_db

router = APIRouter(prefix="/hivestack")

SPEC_PATH = Path(__file__).resolve().parent.parent / "specs" / "hivestack.openapi.json"

_NO_LIMIT_DESC = "Page size. If omitted, the live API applies no limit and returns every record."
_ALWAYS = "1" * 168  # daypart / operating hours: one flag per hour of the week


# ── OpenAPI ───────────────────────────────────────────────────────────────────

def apply_openapi_spec(schema: dict) -> dict:
    """Replace the generated /hivestack paths with app/specs/hivestack.openapi.json.

    The spec file carries the live-API response models (Account, Campaign,
    Unit ...) that FastAPI cannot derive from these dict-based handlers.
    """
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    prefix = router.prefix + "/"
    paths = {p: v for p, v in schema.get("paths", {}).items() if not p.startswith(prefix)}
    paths.update(spec["paths"])
    schema["paths"] = paths
    components = schema.setdefault("components", {})
    components.setdefault("schemas", {}).update(spec["components"]["schemas"])
    components.setdefault("securitySchemes", {}).update(spec["components"]["securitySchemes"])
    return schema


# ── Shared helpers ────────────────────────────────────────────────────────────

def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now():
    """Live-API timestamp format: 2026-11-10T14:03:22.123 (no offset)."""
    return _utcnow().isoformat(timespec="milliseconds")


def _today(offset_days=0):
    return (_utcnow() + timedelta(days=offset_days)).strftime("%Y-%m-%d")


def _uuid():
    return str(_uuid_mod.uuid4())


def _cur(conn):
    return conn.cursor()


def _one(conn, sql, params=()):
    c = conn.cursor()
    c.execute(sql, params)
    return c.fetchone()


def _all(conn, sql, params=()):
    c = conn.cursor()
    c.execute(sql, params)
    return c.fetchall()


def _404(resource, id_):
    raise HTTPException(404, f"{resource} not found: {id_}")


def _int_id(value, resource):
    """Path IDs typed as strings in the spec but integer in storage."""
    try:
        return int(value)
    except (TypeError, ValueError):
        _404(resource, value)


def _mock_vast(w=1920, h=1080):
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<VAST version="2.0"><Ad id="mock-001"><InLine>'
        "<AdSystem>AdBridge Mock</AdSystem>"
        "<AdTitle><![CDATA[Mock DOOH Ad]]></AdTitle>"
        "<Impression><![CDATA[https://mock.adbridge.local/vast-imp]]></Impression>"
        "<Creatives><Creative><Linear><Duration>00:00:15</Duration><MediaFiles>"
        f'<MediaFile width="{w}" height="{h}" type="video/mp4" delivery="progressive">'
        "<![CDATA[https://mock.adbridge.local/creative/sample.mp4]]></MediaFile>"
        "</MediaFiles></Linear></Creative></Creatives>"
        "</InLine></Ad></VAST>"
    )


# ── Resource descriptors ──────────────────────────────────────────────────────

class _Resource:
    """Maps one live-API resource onto one table.

    fields    – response keys, in live-API order. Every field that is not
                computed is a column of the same name.
    computed  – fields filled by `enrich` rather than read from a column.
    json      – columns holding JSON text (nested values, or a type the live
                sample never revealed because it was always null).
    bools     – columns decoded to bool (SQLite returns 0/1).
    omit_null – optional fields left out of the response when NULL.
    defaults  – values for POST when the body omits a field (callables are called).
    prepare   – hook(conn, record, body, is_create) to derive fields before a write.
    enrich    – hook(conn, items) to fill computed fields after a read.
    """

    def __init__(self, label, table, pk, fields, *, computed=(), json=(), bools=(), omit_null=(),
                 defaults=None, order=None, id_start=1, prepare=None, enrich=None):
        self.label, self.table, self.pk = label, table, pk
        self.fields = list(fields)
        self.computed = set(computed)
        self.columns = [f for f in self.fields if f not in self.computed]
        self.json, self.bools, self.omit_null = set(json), set(bools), set(omit_null)
        self.defaults = defaults or {}
        self.order = order or pk
        self.id_start = id_start
        self.prepare, self.enrich = prepare, enrich


def _encode(res, field, value):
    if field in res.json:
        return None if value is None else json.dumps(value)
    if field in res.bools and value is not None:
        return bool(value)
    return value


def _decode(res, row):
    out = {}
    for f in res.fields:
        if f in res.computed:
            out[f] = None
            continue
        v = row.get(f)
        if v is not None and f in res.json:
            v = json.loads(v)
        elif v is not None and f in res.bools:
            v = bool(v)
        if v is None and f in res.omit_null:
            continue
        out[f] = v
    return out


def _render(conn, res, rows):
    items = [_decode(res, r) for r in rows]
    if res.enrich and items:
        res.enrich(conn, items)
    return items


def _wants_count(request):
    return request.query_params.get("$count", "").lower() == "true"


def _list_response(items, request, top, skip, total):
    headers = {"x-page-offset": str(skip)}
    if top is not None:
        headers["x-page-size"] = str(top)
    if _wants_count(request):
        headers["x-total-count"] = str(total)
    return JSONResponse(content=items, headers=headers)


def _list(conn, res, request, top=None, skip=0, where="", params=(), order=None):
    """Bare-array list with $top/$skip paging and x-* paging headers."""
    where_sql = f" WHERE {where}" if where else ""
    total = _one(conn, f"SELECT COUNT(*) FROM {res.table}{where_sql}", params)["count"]
    sql = f"SELECT * FROM {res.table}{where_sql} ORDER BY {order or res.order}"
    if top is not None:
        rows = _all(conn, sql + " LIMIT %s OFFSET %s", list(params) + [top, skip])
    else:
        rows = _all(conn, sql, params)[skip:]
    return _list_response(_render(conn, res, rows), request, top, skip, total)


def _get_row(conn, res, id_):
    row = _one(conn, f"SELECT * FROM {res.table} WHERE {res.pk} = %s", (id_,))
    if not row:
        _404(res.label, id_)
    return row


def _get(conn, res, id_):
    return _render(conn, res, [_get_row(conn, res, id_)])[0]


def _next_id(conn, res):
    top = _one(conn, f"SELECT MAX({res.pk}) AS m FROM {res.table}")["m"]
    return max((top or 0) + 1, res.id_start)


def _write(conn, res, record):
    cols = [c for c in res.columns if c in record]
    _cur(conn).execute(
        f"INSERT INTO {res.table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
        [_encode(res, c, record[c]) for c in cols],
    )


def _create(conn, res, body, **overrides):
    """POST: defaults <- body <- overrides. The server assigns the integer ID."""
    record = {k: (v() if callable(v) else copy.deepcopy(v)) for k, v in res.defaults.items()}
    record.update({k: v for k, v in body.items() if k in res.columns and k != res.pk})
    record.update(overrides)
    now = _now()
    for stamp in ("created_on_utc", "modified_on_utc"):
        if stamp in res.columns:
            record[stamp] = now
    record[res.pk] = _next_id(conn, res)
    if res.prepare:
        res.prepare(conn, record, body, True)
    _write(conn, res, record)
    conn.commit()
    return _get(conn, res, record[res.pk])


def _update(conn, res, id_, body):
    """PUT: partial update of the fields present in the body."""
    _get_row(conn, res, id_)
    changes = {k: v for k, v in body.items() if k in res.columns and k != res.pk}
    if res.prepare:
        res.prepare(conn, changes, body, False)
    if changes and "modified_on_utc" in res.columns:
        changes["modified_on_utc"] = _now()
    if changes:
        sets = ", ".join(f"{c} = %s" for c in changes)
        _cur(conn).execute(
            f"UPDATE {res.table} SET {sets} WHERE {res.pk} = %s",
            [_encode(res, c, v) for c, v in changes.items()] + [id_],
        )
        conn.commit()
    return _get(conn, res, id_)


def _delete(conn, res, id_):
    _get_row(conn, res, id_)
    _cur(conn).execute(f"DELETE FROM {res.table} WHERE {res.pk} = %s", (id_,))
    conn.commit()
    return Response(status_code=204)


def _by_external_id(conn, res, external_id):
    row = _one(conn, f"SELECT {res.pk} FROM {res.table} WHERE external_id = %s ORDER BY {res.pk}",
               (external_id,))
    if not row:
        _404(res.label, external_id)
    return row[res.pk]


# ── $filter (OData subset) ────────────────────────────────────────────────────

_FILTER_TERM = re.compile(r"^\s*(\w+)\s+(eq|ne|gt|ge|lt|le)\s+(.+?)\s*$", re.IGNORECASE)
_FILTER_OPS = {"eq": "=", "ne": "<>", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}


def _odata_literal(raw):
    if len(raw) >= 2 and raw[0] == raw[-1] == "'":
        return raw[1:-1].replace("''", "'")
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    if low == "null":
        return None
    for cast in (int, float):
        try:
            return cast(raw)
        except ValueError:
            pass
    return raw


def _filter_sql(res, expr):
    """Translate `field op value [and ...]` to SQL. Field names are checked
    against the resource's columns; values are bound as parameters. Anything
    else falls back to a case-insensitive name search."""
    if not expr:
        return "", ()
    clauses, params = [], []
    for term in re.split(r"\s+and\s+", expr, flags=re.IGNORECASE):
        m = _FILTER_TERM.match(term)
        if not m or m.group(1) not in res.columns or m.group(1) in res.json:
            return "name ILIKE %s", (f"%{expr}%",)
        field, op, raw = m.group(1), m.group(2).lower(), m.group(3)
        value = _odata_literal(raw)
        if value is None and op in ("eq", "ne"):
            clauses.append(f"{field} IS {'NOT ' if op == 'ne' else ''}NULL")
        else:
            clauses.append(f"{field} {_FILTER_OPS[op]} %s")
            params.append(value)
    return " AND ".join(clauses), tuple(params)


# ── Derived-field hooks ───────────────────────────────────────────────────────

def _empty_inventory_targeting(targeting_id):
    from app.database import _hs_empty_inventory_targeting
    return _hs_empty_inventory_targeting(targeting_id)


def _prepare_advertiser(conn, record, body, is_create):
    if is_create and "unique_advertiser" not in body:
        record["unique_advertiser"] = {
            "advertiser_domains": [{"domain": d} for d in body.get("domains", [])],
            "logo_url": None, "name": record["name"], "uniqueadvertiser_id": record["advertiser_id"],
        }


def _enrich_campaigns(conn, items):
    """Embed the account and advertiser objects the live API returns."""
    acct_ids = {i["account_id"] for i in items if i["account_id"] is not None}
    adv_ids = {i["advertiser_id"] for i in items if i["advertiser_id"] is not None}
    accounts = {a["account_id"]: a for a in _fetch_in(conn, ACCOUNTS, acct_ids)}
    advertisers = {a["advertiser_id"]: a for a in _fetch_in(conn, ADVERTISERS, adv_ids)}
    for item in items:
        item["account"] = accounts.get(item["account_id"])
        item["advertiser"] = advertisers.get(item["advertiser_id"])


def _fetch_in(conn, res, ids):
    if not ids:
        return []
    ids = sorted(ids)
    rows = _all(conn, f"SELECT * FROM {res.table} WHERE {res.pk} IN ({', '.join(['%s'] * len(ids))})", ids)
    return _render(conn, res, rows)


def _prepare_lineitem(conn, record, body, is_create):
    if is_create:
        record.setdefault("created_at", record["created_on_utc"])


def _prepare_creative(conn, record, body, is_create):
    if is_create:
        record.setdefault("created_at", _now())
        if "inventory_targeting" not in body:
            record["inventory_targeting"] = _empty_inventory_targeting(record["creative_id"])


def _prepare_network(conn, record, body, is_create):
    if is_create and not record.get("code"):
        record["code"] = re.sub(r"[^A-Z0-9]", "", record["name"].upper())[:8] or f"NET{record['network_id']}"


def _prepare_site(conn, record, body, is_create):
    if is_create and not record.get("external_id"):
        record["external_id"] = f"SITE-{record['site_id']}"


def _prepare_unitpack(conn, record, body, is_create):
    if is_create and not record.get("external_id"):
        record["external_id"] = f"PACK-{record['unitpack_id']}"


def _location_snapshot(conn, location_id):
    row = _one(conn, "SELECT * FROM hs_locations WHERE location_id = %s", (location_id,))
    if not row:
        return None
    snap = {k: row.get(k) for k in ("archived", "code", "country_id", "hierarchy", "latitude", "level",
                                     "location_id", "longitude", "name", "region_id", "version")}
    snap["archived"] = bool(snap["archived"])
    return snap


def _mediatype_snapshot(conn, mediatype_id):
    row = _one(conn, "SELECT * FROM hs_mediatypes WHERE mediatype_id = %s", (mediatype_id,))
    if not row:
        return None
    snap = {k: row.get(k) for k in ("archived", "code", "enumeration_id", "mediatype_id", "name",
                                     "parent_mediatype_id")}
    snap["archived"] = bool(snap["archived"])
    return snap


def _prepare_unit(conn, record, body, is_create):
    """Keep the embedded location / media_type objects in step with their IDs."""
    if is_create and not record.get("external_id"):
        record["external_id"] = f"UNIT-{record['unit_id']}"
    if is_create and record.get("site_id") is not None:
        site = _one(conn, "SELECT owner_id, location_id FROM hs_sites WHERE site_id = %s", (record["site_id"],))
        if site:
            if record.get("owner_id") is None:
                record["owner_id"] = site["owner_id"]
            if record.get("location_id") is None:
                record["location_id"] = site["location_id"]
    if "location_id" in record and "location" not in body:
        record["location"] = _location_snapshot(conn, record["location_id"])
    if "mediatype_id" in record and "media_type" not in body:
        mt = _mediatype_snapshot(conn, record["mediatype_id"])
        record["media_type"] = mt
        record["mediatype_name"] = mt["name"] if mt else None


def _enrich_mediatypes(conn, items):
    for item in items:
        item["children"] = []


# ── Resource definitions (field order = live-API order) ───────────────────────

ACCOUNTS = _Resource(
    "Account", "hs_accounts", "account_id",
    ["account_id", "external_id", "logo_url", "name", "organization_id"],
    defaults={"external_id": None, "logo_url": None, "name": "New Account",
              "organization_id": HS_ORGANIZATION_ID},
    id_start=3101)

ADVERTISERS = _Resource(
    "Advertiser", "hs_advertisers", "advertiser_id",
    ["advertiser_id", "created_on_utc", "external_id", "modified_on_utc", "name", "organization_id",
     "unique_advertiser"],
    json=["external_id", "unique_advertiser"],
    defaults={"external_id": None, "name": "New Advertiser", "organization_id": HS_ORGANIZATION_ID},
    id_start=48201, prepare=_prepare_advertiser)

CAMPAIGNS = _Resource(
    "Campaign", "hs_campaigns", "campaign_id",
    ["account", "account_id", "advertiser", "advertiser_id", "budget", "budget_cap", "campaign_id",
     "campaign_properties", "campaign_type", "cap", "created_on_utc", "end_date",
     "min_seconds_between_plays", "modified_on_utc", "name", "notes", "organization_id", "pacing_budget",
     "pacing_budget_mode", "pacing_strategy", "priority", "start_date"],
    computed=["account", "advertiser"],
    json=["campaign_properties", "cap", "pacing_budget", "pacing_budget_mode", "pacing_strategy", "priority"],
    defaults={"account_id": None, "budget": None, "budget_cap": None, "campaign_properties": [],
              "campaign_type": "standard", "cap": None, "end_date": None, "min_seconds_between_plays": 0,
              "name": "New Campaign", "notes": None, "organization_id": HS_ORGANIZATION_ID,
              "pacing_budget": None, "pacing_budget_mode": None, "pacing_strategy": None, "priority": None,
              "start_date": None},
    id_start=61001, enrich=_enrich_campaigns)

LINEITEMS = _Resource(
    "Line item", "hs_lineitems", "lineitem_id",
    ["ad_duration", "budget", "budget_mode", "cadence", "cadence_smooth_catchup", "campaign_id",
     "cancelled_at", "cap", "cpp", "created_at", "created_by", "created_on_utc", "creative_ids", "daypart",
     "delivery_curve", "demographic_ids", "external_id", "flight_from", "flight_to", "is_cpm_from_rate_card",
     "lineitem_id", "lineitem_properties", "market_targeted_impressions", "max_bid_cpm", "mediaowner_ids",
     "min_concentration_factor", "min_seconds_between_plays", "modified_on_utc", "name", "notes",
     "package_id", "priority", "purchase_type", "run_state", "state", "submitted_at", "unit_ids", "cpm_net",
     "deal_id"],
    json=["cpp", "creative_ids", "demographic_ids", "external_id", "lineitem_properties", "max_bid_cpm",
          "mediaowner_ids", "unit_ids", "cpm_net"],
    bools=["cadence_smooth_catchup", "is_cpm_from_rate_card"],
    omit_null=["deal_id"],
    defaults={"ad_duration": 15, "budget": None, "budget_mode": "total", "cadence": 1.0,
              "cadence_smooth_catchup": False, "cancelled_at": None, "cap": None, "cpp": None,
              "created_by": "adbridge@mock.local", "creative_ids": [], "daypart": _ALWAYS,
              "delivery_curve": "even", "demographic_ids": [], "external_id": None,
              "flight_from": lambda: _today(), "flight_to": lambda: _today(30),
              "is_cpm_from_rate_card": False, "lineitem_properties": [], "market_targeted_impressions": None,
              "max_bid_cpm": None, "mediaowner_ids": [], "min_concentration_factor": 0.0,
              "min_seconds_between_plays": 0, "name": "New Line Item", "notes": None, "package_id": None,
              "priority": 5, "purchase_type": "direct", "run_state": "not_started", "state": "draft",
              "submitted_at": None, "unit_ids": [], "cpm_net": []},
    id_start=72001, prepare=_prepare_lineitem)

CREATIVES = _Resource(
    "Creative", "hs_creatives", "creative_id",
    ["advertiser_id", "created_at", "creative_files", "creative_id", "daypart", "iab_categories",
     "inventory_targeting", "language_ids", "name", "organization_id", "retired_at", "run_state", "state",
     "submitted_at", "uuid", "last_played_time_utc"],
    json=["creative_files", "iab_categories", "inventory_targeting", "language_ids", "last_played_time_utc"],
    defaults={"advertiser_id": None, "creative_files": [], "daypart": _ALWAYS, "iab_categories": [],
              "language_ids": [], "name": "New Creative", "organization_id": HS_ORGANIZATION_ID,
              "retired_at": None, "run_state": "inactive", "state": "draft", "submitted_at": None,
              "uuid": _uuid, "last_played_time_utc": None},
    id_start=85001, prepare=_prepare_creative)

CREATIVE_APPROVALS = _Resource(
    "Creative approval", "hs_creative_approvals", "id",
    ["id", "creative_id", "media_owner_id", "approval_status", "notes", "created_at", "updated_at"],
    order="created_at")

LINEITEM_CREATIVES = _Resource(
    "Line item creative", "hs_lineitem_creatives", "lineitemcreative_id",
    ["lineitemcreative_id", "lineitem_id", "creative_id", "local_from_date", "local_to_date", "weight"])

LINEITEM_DEAL_CREATIVES = _Resource(
    "Line item deal creative", "hs_lineitem_deal_creatives", "lineitem_id",
    ["lineitem_id", "creative_id", "deal_id", "deal_flight_from", "deal_flight_to", "local_from_date",
     "local_to_date"],
    json=["local_from_date", "local_to_date"], order="lineitem_id, creative_id, deal_id")

NETWORKS = _Resource(
    "Network", "hs_networks", "network_id",
    ["code", "created_on_utc", "loop_length", "modified_on_utc", "name", "network_id", "owner_id",
     "spot_length"],
    json=["loop_length", "spot_length"],
    defaults={"loop_length": None, "name": "New Network", "owner_id": None, "spot_length": None},
    id_start=2201, prepare=_prepare_network)

SITES = _Resource(
    "Site", "hs_sites", "site_id",
    ["active", "created_on_utc", "description", "external_id", "latitude", "location_id", "longitude",
     "modified_on_utc", "name", "owner_id", "site_id", "uuid"],
    bools=["active"],
    defaults={"active": True, "description": "", "latitude": 0.0, "location_id": None, "longitude": 0.0,
              "name": "New Site", "owner_id": None, "uuid": _uuid},
    id_start=9101, prepare=_prepare_site)

_UNIT_FIELDS = [
    "active", "allow_audio", "allow_flash", "allow_html", "allow_image", "allow_video", "allow_zip",
    "available_for_adserver", "available_for_deals", "available_for_open_exchange", "available_for_storefront",
    "blacklist_unique_advertiser_ids", "created_on_utc", "description", "enable_strict_iab_blacklisting",
    "enable_strict_iab_frequency_capping", "external_id", "facing_direction", "floor_cpm", "image_uri",
    "impression_verifier_vendor", "impression_verifier_vendor_external_id", "latitude", "location",
    "location_id", "longitude", "loop_length", "max_spot_length", "media_type", "mediatype_id",
    "mediatype_name", "min_seconds_between_ad_domain_plays", "min_seconds_between_creative_category_plays",
    "min_seconds_between_iab_category_plays", "min_spot_length", "modified_on_utc", "name", "network_id",
    "operating_hours", "owner_id", "physical_screen_height_cm", "physical_screen_width_cm",
    "physical_unit_count", "screen_height", "screen_width", "site_id", "spot_length", "timezone", "unit_id",
    "uuid", "weekly_impressions", "weekly_traffic", "last_heard_from_seconds_ago", "last_heard_from_utc",
    "last_played_on_seconds_ago", "last_played_on_utc", "has_audience_data",
]
_UNIT_JSON = ["blacklist_unique_advertiser_ids", "facing_direction", "image_uri",
              "impression_verifier_vendor_external_id", "location", "media_type", "physical_unit_count"]
_UNIT_BOOLS = ["active", "allow_audio", "allow_flash", "allow_html", "allow_image", "allow_video", "allow_zip",
               "available_for_adserver", "available_for_deals", "available_for_open_exchange",
               "available_for_storefront", "enable_strict_iab_blacklisting",
               "enable_strict_iab_frequency_capping", "has_audience_data"]

UNITS = _Resource(
    "Unit", "hs_units", "unit_id", _UNIT_FIELDS, json=_UNIT_JSON, bools=_UNIT_BOOLS,
    defaults={"active": True, "allow_audio": False, "allow_flash": False, "allow_html": False,
              "allow_image": True, "allow_video": True, "allow_zip": False, "available_for_adserver": True,
              "available_for_deals": True, "available_for_open_exchange": True,
              "available_for_storefront": False, "blacklist_unique_advertiser_ids": [], "description": "",
              "enable_strict_iab_blacklisting": False, "enable_strict_iab_frequency_capping": False,
              "facing_direction": None, "floor_cpm": 0.0, "image_uri": None, "impression_verifier_vendor": None,
              "impression_verifier_vendor_external_id": None, "latitude": 0.0, "location_id": None,
              "longitude": 0.0, "loop_length": None, "max_spot_length": 30, "mediatype_id": None,
              "min_seconds_between_ad_domain_plays": 0, "min_seconds_between_creative_category_plays": 0,
              "min_seconds_between_iab_category_plays": 0, "min_spot_length": 5, "name": "New Unit",
              "network_id": None, "operating_hours": _ALWAYS, "owner_id": None,
              "physical_screen_height_cm": None, "physical_screen_width_cm": None, "physical_unit_count": None,
              "screen_height": 1080, "screen_width": 1920, "site_id": None, "spot_length": 15,
              "timezone": "UTC", "uuid": _uuid, "weekly_impressions": 0, "weekly_traffic": 0,
              "last_heard_from_seconds_ago": None, "last_heard_from_utc": None,
              "last_played_on_seconds_ago": None, "last_played_on_utc": None, "has_audience_data": False},
    id_start=30101, prepare=_prepare_unit)

# GET /unitpacks/{id}/units returns units without heartbeat / audience fields
_PACK_UNIT_DROP = {"blacklist_unique_advertiser_ids", "last_heard_from_seconds_ago", "last_heard_from_utc",
                   "last_played_on_seconds_ago", "last_played_on_utc", "has_audience_data"}
UNITPACK_UNITS = _Resource(
    "Unit", "hs_units", "unit_id", [f for f in _UNIT_FIELDS if f not in _PACK_UNIT_DROP],
    json=_UNIT_JSON, bools=_UNIT_BOOLS)

UNITPACKS = _Resource(
    "Unit pack", "hs_unitpacks", "unitpack_id",
    ["active", "created_on_utc", "description", "external_id", "modified_on_utc", "name", "owner_id",
     "prepopulation_option", "unitpack_id"],
    bools=["active"],
    defaults={"active": True, "description": None, "name": "New Pack", "owner_id": None,
              "prepopulation_option": "none"},
    id_start=4401, prepare=_prepare_unitpack)

DEMOGRAPHICS = _Resource(
    "Demographic", "hs_demographics", "demographic_id",
    ["code", "created_on_utc", "demographic_id", "modified_on_utc", "name", "organization_id", "show_buyers",
     "type"],
    json=["organization_id"], bools=["show_buyers"],
    defaults={"code": "", "name": "New Demographic", "organization_id": None, "show_buyers": True,
              "type": "custom"},
    id_start=801)

LANGUAGES = _Resource("Language", "hs_languages", "language_id", ["code", "language_id", "name"])

LOCATIONS = _Resource(
    "Location", "hs_locations", "location_id",
    ["code", "country_id", "latitude", "level", "location_id", "longitude", "name", "region_id"])

MEDIATYPES = _Resource(
    "Media type", "hs_mediatypes", "mediatype_id",
    ["archived", "code", "enumeration_id", "mediatype_id", "name", "parent_mediatype_id", "children"],
    computed=["children"], bools=["archived"], enrich=_enrich_mediatypes)

UNIT_LANGUAGES = _Resource(
    "Unit language", "hs_unit_languages", "unitlanguage_id",
    ["created_on_utc", "language_id", "modified_on_utc", "unit_id", "unitlanguage_id"], id_start=89001)

UNIT_PROPERTIES = _Resource(
    "Unit property", "hs_unit_properties", "unitproperty_id",
    ["created_on_utc", "modified_on_utc", "name", "unit_id", "unitproperty_id", "value"], id_start=89501)

REPORT_DEFINITIONS = _Resource(
    "Report definition", "hs_report_definitions", "report_definition_id",
    ["definition_json", "exchange_id", "last_executed_on_utc", "name", "organization_id",
     "report_definition_id"],
    json=["definition_json", "exchange_id"], id_start=5501)

REPORT_EXECUTIONS = _Resource(
    "Report execution", "hs_report_executions", "report_execution_id",
    ["execution_finished_time_utc", "execution_started_time_utc", "progress", "report_definition_id",
     "report_execution_id", "report_file_url", "status_code", "status_message"],
    json=["status_message"], order="report_execution_id DESC", id_start=5601)

PUBLISHERS = _Resource("Media owner", "hs_publishers", "id",
                       ["id", "name", "domain", "seller_type", "created_at"])
DEALS = _Resource("Deal", "hs_deals", "id",
                  ["id", "publisher_id", "name", "bidfloor", "bidfloorcur", "deal_type", "must_bid", "wseat",
                   "created_at"])
BID_REQUESTS = _Resource("Bid request", "hs_bid_requests", "id", ["id", "payload", "received_at"],
                         order="received_at")
IMPRESSIONS = _Resource("Impression", "hs_impressions", "id",
                        ["id", "bid_request_id", "event_type", "total_imp", "target_imp", "price", "recorded_at"],
                        order="recorded_at")
CUSTOM_EVENTS = _Resource("Custom event", "hs_custom_events", "id",
                          ["id", "name", "description", "status", "created_at", "updated_at"],
                          order="created_at DESC")
EVENT_DATA = _Resource("Event data", "hs_event_data", "id",
                       ["id", "event_id", "entity_type", "entity_id", "status", "updated_at"])


# ══════════════════════════════════════════════════════════════════════════════
# AD SERVING
# ══════════════════════════════════════════════════════════════════════════════

def _unit_by(conn, column, value):
    return _one(conn, f"SELECT * FROM hs_units WHERE {column} = %s", (value,))


def _vast_for(unit):
    w, h = (unit["screen_width"], unit["screen_height"]) if unit else (1920, 1080)
    return Response(content=_mock_vast(w, h), media_type="application/xml")


def _upcoming_creatives(conn, unit):
    """Approved creatives whose inventory targeting includes the unit."""
    if not unit:
        return []
    creatives = _render(conn, CREATIVES, _all(conn, "SELECT * FROM hs_creatives WHERE state = %s ORDER BY creative_id",
                                                ("approved",)))
    return [c for c in creatives if unit["unit_id"] in (c["inventory_targeting"] or {}).get("unit_ids", [])]


@router.get("/adserving/units/{uuid}/schedulevast", response_class=Response)
def schedule_vast_by_uuid(uuid: str, conn=Depends(get_db)):
    """Request an ad for a single screen by UUID — returns VAST 2.0 XML."""
    return _vast_for(_unit_by(conn, "uuid", uuid))


@router.get("/adserving/units/external/{screen_id}/schedulevast", response_class=Response)
def schedule_vast_by_screen_id(screen_id: str, conn=Depends(get_db)):
    """Request an ad for a single screen by external screen ID — returns VAST 2.0 XML."""
    return _vast_for(_unit_by(conn, "external_id", screen_id))


@router.post("/adserving/syncgroup/schedulevast", response_class=Response)
def schedule_vast_sync_group(body: dict = Body(...), conn=Depends(get_db)):
    """Request ads for a sync group (multiple screens) — returns VAST 2.0 XML."""
    return Response(content=_mock_vast(1920, 1080), media_type="application/xml")


@router.get("/adserving/units/{uuid}/creatives")
def get_upcoming_creatives_by_uuid(uuid: str, conn=Depends(get_db)):
    """Creatives that might play in the next 24 hours for a screen (by UUID)."""
    return _upcoming_creatives(conn, _unit_by(conn, "uuid", uuid))


@router.get("/adserving/units/external/{screen_id}/creatives")
def get_upcoming_creatives_by_screen_id(screen_id: str, conn=Depends(get_db)):
    """Creatives that might play in the next 24 hours for a screen (by external ID)."""
    return _upcoming_creatives(conn, _unit_by(conn, "external_id", screen_id))


# ══════════════════════════════════════════════════════════════════════════════
# AVAILS
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/avails")
def check_avails_by_targeting(body: dict = Body(...), conn=Depends(get_db)):
    """Check avails for a targeting specification."""
    return {
        "available_impressions": 125000,
        "available_units": 8,
        "estimated_reach": 45000,
        "currency": "USD",
        "cpm_floor": 1.50,
    }


@router.get("/avails/lineitems/{lineitem_id}")
def check_avails_by_lineitem(lineitem_id: int, conn=Depends(get_db)):
    """Check avails for an existing line item."""
    item = _get(conn, LINEITEMS, lineitem_id)
    return {
        "lineitem_id": lineitem_id,
        "available_impressions": 98000,
        "available_units": len(item["unit_ids"] or []),
        "currency": "USD",
    }


# ══════════════════════════════════════════════════════════════════════════════
# ACCOUNTS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/accounts")
def list_accounts(request: Request, top: int = Query(100, alias="$top"), skip: int = Query(0, alias="$skip"),
                  count: bool = Query(False, alias="$count"), conn=Depends(get_db)):
    return _list(conn, ACCOUNTS, request, top, skip)


@router.post("/accounts", status_code=201)
def create_account(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, ACCOUNTS, body)


@router.get("/accounts/{account_id}")
def get_account(account_id: int, conn=Depends(get_db)):
    return _get(conn, ACCOUNTS, account_id)


@router.put("/accounts/{account_id}")
def update_account(account_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, ACCOUNTS, account_id, body)


# ══════════════════════════════════════════════════════════════════════════════
# ADVERTISERS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/advertisers")
def list_advertisers(request: Request, top: int = Query(100, alias="$top"), skip: int = Query(0, alias="$skip"),
                     filter: Optional[str] = Query(None, alias="$filter"), conn=Depends(get_db)):
    where, params = _filter_sql(ADVERTISERS, filter)
    return _list(conn, ADVERTISERS, request, top, skip, where, params)


@router.post("/advertisers", status_code=201)
def create_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, ADVERTISERS, body)


@router.get("/advertisers/{advertiser_id}")
def get_advertiser(advertiser_id: int, conn=Depends(get_db)):
    return _get(conn, ADVERTISERS, advertiser_id)


@router.put("/advertisers/{advertiser_id}")
def update_advertiser(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, ADVERTISERS, advertiser_id, body)


# ══════════════════════════════════════════════════════════════════════════════
# CAMPAIGNS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/campaigns")
def list_campaigns(request: Request, top: int = Query(100, alias="$top"), skip: int = Query(0, alias="$skip"),
                   filter: Optional[str] = Query(None, alias="$filter"), conn=Depends(get_db)):
    where, params = _filter_sql(CAMPAIGNS, filter)
    return _list(conn, CAMPAIGNS, request, top, skip, where, params)


@router.post("/campaigns", status_code=201)
def create_campaign(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, CAMPAIGNS, body)


@router.get("/campaigns/{campaign_id}")
def get_campaign(campaign_id: int, conn=Depends(get_db)):
    return _get(conn, CAMPAIGNS, campaign_id)


@router.put("/campaigns/{campaign_id}")
def update_campaign(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, CAMPAIGNS, campaign_id, body)


# ══════════════════════════════════════════════════════════════════════════════
# LINE ITEMS
# ══════════════════════════════════════════════════════════════════════════════

def _expand_lineitems(conn, items, expand):
    """$expand=campaign,creatives embeds the related objects."""
    wanted = {e.strip().lower() for e in (expand or "").split(",") if e.strip()}
    if "campaign" in wanted:
        campaigns = {c["campaign_id"]: c for c in _fetch_in(conn, CAMPAIGNS, {i["campaign_id"] for i in items
                                                                               if i["campaign_id"] is not None})}
        for item in items:
            item["campaign"] = campaigns.get(item["campaign_id"])
    if "creatives" in wanted:
        ids = {cid for i in items for cid in (i["creative_ids"] or [])}
        creatives = {c["creative_id"]: c for c in _fetch_in(conn, CREATIVES, ids)}
        for item in items:
            item["creatives"] = [creatives[c] for c in (item["creative_ids"] or []) if c in creatives]
    return items


@router.get("/lineitems")
def list_lineitems(request: Request, top: int = Query(20, alias="$top"), skip: int = Query(0, alias="$skip"),
                   filter: Optional[str] = Query(None, alias="$filter"),
                   expand: Optional[str] = Query(None, alias="$expand"), conn=Depends(get_db)):
    where, params = _filter_sql(LINEITEMS, filter)
    resp = _list(conn, LINEITEMS, request, top, skip, where, params)
    if not expand:
        return resp
    items = _expand_lineitems(conn, json.loads(resp.body), expand)
    return JSONResponse(content=items, headers={k: v for k, v in resp.headers.items() if k.startswith("x-")})


@router.post("/lineitems", status_code=201)
def create_lineitem(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, LINEITEMS, body)


@router.get("/lineitems/{lineitem_id}")
def get_lineitem(lineitem_id: int, expand: Optional[str] = Query(None, alias="$expand"), conn=Depends(get_db)):
    item = _get(conn, LINEITEMS, lineitem_id)
    return _expand_lineitems(conn, [item], expand)[0] if expand else item


@router.put("/lineitems/{lineitem_id}")
def update_lineitem(lineitem_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, LINEITEMS, lineitem_id, body)


@router.get("/lineitems/{lineitem_id}/units")
def get_lineitem_targeted_units(lineitem_id: int, conn=Depends(get_db)):
    """Returns unit IDs targeted by the line item."""
    return _get(conn, LINEITEMS, lineitem_id)["unit_ids"] or []


# ══════════════════════════════════════════════════════════════════════════════
# CREATIVES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/creatives")
def list_creatives(request: Request, top: int = Query(100, alias="$top"), skip: int = Query(0, alias="$skip"),
                   filter: Optional[str] = Query(None, alias="$filter"), conn=Depends(get_db)):
    where, params = _filter_sql(CREATIVES, filter)
    return _list(conn, CREATIVES, request, top, skip, where, params)


@router.post("/creatives", status_code=201)
def create_creative(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, CREATIVES, body)


@router.get("/creatives/{creative_id}")
def get_creative(creative_id: int, conn=Depends(get_db)):
    return _get(conn, CREATIVES, creative_id)


@router.put("/creatives/{creative_id}")
def update_creative(creative_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, CREATIVES, creative_id, body)


@router.get("/creatives/{creative_id}/units")
def get_creative_targeted_units(creative_id: int, conn=Depends(get_db)):
    """Unit IDs from the creative's inventory_targeting."""
    targeting = _get(conn, CREATIVES, creative_id)["inventory_targeting"] or {}
    return targeting.get("unit_ids", [])


@router.post("/creatives/{creative_id}/files")
def upload_creative_files(creative_id: int, body: dict = Body(...), conn=Depends(get_db)):
    """Upload/link creative files to a creative.

    Body: one file object, or {"files": [...]}. Each file needs at least `url`.
    Returns the updated creative.
    """
    creative = _get(conn, CREATIVES, creative_id)
    files = creative["creative_files"] or []
    existing = _all(conn, "SELECT creative_files FROM hs_creatives")
    next_file_id = 1 + max(
        [f["creativefile_id"] for r in existing for f in json.loads(r["creative_files"] or "[]")] + [86000])
    for i, f in enumerate(body.get("files", [body])):
        url = f.get("url", "")
        is_video = url.lower().endswith((".mp4", ".mov", ".webm"))
        files.append({
            "archived": False, "creative_id": creative_id, "creativefile_id": next_file_id + i,
            "duration": f.get("duration", 15.0 if is_video else None),
            "external_id": f.get("external_id", f"CF-{next_file_id + i}"),
            "file_size": f.get("file_size", 0), "file_validation_errors": None,
            "format": f.get("format", "video/mp4" if is_video else "image/jpeg"), "generated": False,
            "height": f.get("height", 1080), "is_vast": f.get("is_vast", False),
            "name": f.get("name", url.rsplit("/", 1)[-1] or "file"), "placement_type": None,
            "prefetch_approval": None, "prefetch_is_external": False, "tag_content": None, "tag_type": None,
            "url": url, "uuid": _uuid(), "width": f.get("width", 1920),
        })
    return _update(conn, CREATIVES, creative_id, {"creative_files": files})


# ── Creative Approvals ────────────────────────────────────────────────────────

@router.get("/creativeapprovals")
def list_creative_approvals(request: Request, top: int = Query(100, alias="$top"),
                            skip: int = Query(0, alias="$skip"), conn=Depends(get_db)):
    return _list(conn, CREATIVE_APPROVALS, request, top, skip)


@router.put("/creativeapprovals/{approval_id}")
def update_creative_approval(approval_id: str, body: dict = Body(...), conn=Depends(get_db)):
    _get_row(conn, CREATIVE_APPROVALS, approval_id)
    _cur(conn).execute(
        "UPDATE hs_creative_approvals SET approval_status=%s, notes=COALESCE(%s, notes), updated_at=%s WHERE id=%s",
        (body.get("approval_status", "approved"), body.get("notes"), _now(), approval_id),
    )
    conn.commit()
    return _get(conn, CREATIVE_APPROVALS, approval_id)


@router.post("/creativeapprovals/{approval_id}/convert")
def convert_creative_approval_mode(approval_id: str, body: dict = Body(...), conn=Depends(get_db)):
    """Switch a media owner between approval modes."""
    return {"approval_id": approval_id, "mode": body.get("mode", "auto"), "status": "ok"}


# ── LineItem-Creative Associations ────────────────────────────────────────────

@router.get("/lineitemcreatives")
def list_lineitem_creatives(request: Request, lineitem_id: Optional[int] = None, conn=Depends(get_db)):
    where, params = ("lineitem_id = %s", (lineitem_id,)) if lineitem_id is not None else ("", ())
    return _list(conn, LINEITEM_CREATIVES, request, None, 0, where, params)


@router.get("/lineitemdealcreatives")
def list_lineitem_deal_creatives(request: Request, lineitem_id: Optional[int] = None, conn=Depends(get_db)):
    where, params = ("lineitem_id = %s", (lineitem_id,)) if lineitem_id is not None else ("", ())
    return _list(conn, LINEITEM_DEAL_CREATIVES, request, None, 0, where, params)


# ══════════════════════════════════════════════════════════════════════════════
# NETWORKS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/networks")
def list_networks(request: Request, top: Optional[int] = Query(None, alias="$top", description=_NO_LIMIT_DESC),
                  skip: int = Query(0, alias="$skip"), count: bool = Query(False, alias="$count"),
                  conn=Depends(get_db)):
    return _list(conn, NETWORKS, request, top, skip)


@router.post("/networks", status_code=201)
def create_network(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, NETWORKS, body)


@router.get("/networks/{network_id}")
def get_network(network_id: int, conn=Depends(get_db)):
    return _get(conn, NETWORKS, network_id)


@router.put("/networks/{network_id}")
def update_network(network_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, NETWORKS, network_id, body)


@router.delete("/networks/{network_id}", status_code=204)
def delete_network(network_id: int, conn=Depends(get_db)):
    return _delete(conn, NETWORKS, network_id)


# ══════════════════════════════════════════════════════════════════════════════
# SITES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/sites")
def list_sites(request: Request, top: Optional[int] = Query(None, alias="$top", description=_NO_LIMIT_DESC),
               skip: int = Query(0, alias="$skip"), count: bool = Query(False, alias="$count"),
               conn=Depends(get_db)):
    return _list(conn, SITES, request, top, skip)


@router.post("/sites", status_code=201)
def create_site(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, SITES, body)


@router.get("/sites/{site_id}")
def get_site(site_id: int, conn=Depends(get_db)):
    return _get(conn, SITES, site_id)


@router.put("/sites/{site_id}")
def update_site(site_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, SITES, site_id, body)


@router.delete("/sites/{site_id}", status_code=204)
def delete_site(site_id: int, conn=Depends(get_db)):
    return _delete(conn, SITES, site_id)


@router.get("/sites/external/{external_id}")
def get_site_by_external_id(external_id: str, conn=Depends(get_db)):
    return _get(conn, SITES, _by_external_id(conn, SITES, external_id))


@router.put("/sites/external/{external_id}")
def update_site_by_external_id(external_id: str, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, SITES, _by_external_id(conn, SITES, external_id), body)


@router.delete("/sites/external/{external_id}", status_code=204)
def delete_site_by_external_id(external_id: str, conn=Depends(get_db)):
    return _delete(conn, SITES, _by_external_id(conn, SITES, external_id))


# ══════════════════════════════════════════════════════════════════════════════
# UNITS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/units")
def list_units(request: Request, top: Optional[int] = Query(None, alias="$top", description=_NO_LIMIT_DESC),
               skip: int = Query(0, alias="$skip"), count: bool = Query(False, alias="$count"),
               conn=Depends(get_db)):
    return _list(conn, UNITS, request, top, skip)


@router.post("/units", status_code=201)
def create_unit(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, UNITS, body)


@router.get("/units/{unit_id}")
def get_unit(unit_id: int, conn=Depends(get_db)):
    return _get(conn, UNITS, unit_id)


@router.put("/units/{unit_id}")
def update_unit(unit_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, UNITS, unit_id, body)


@router.delete("/units/{unit_id}", status_code=204)
def delete_unit(unit_id: int, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_unitpack_units WHERE unit_id = %s", (unit_id,))
    return _delete(conn, UNITS, unit_id)


@router.get("/units/external/{external_id}")
def get_unit_by_external_id(external_id: str, conn=Depends(get_db)):
    return _get(conn, UNITS, _by_external_id(conn, UNITS, external_id))


@router.put("/units/external/{external_id}")
def update_unit_by_external_id(external_id: str, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, UNITS, _by_external_id(conn, UNITS, external_id), body)


@router.delete("/units/external/{external_id}", status_code=204)
def delete_unit_by_external_id(external_id: str, conn=Depends(get_db)):
    return delete_unit(_by_external_id(conn, UNITS, external_id), conn)


# ══════════════════════════════════════════════════════════════════════════════
# UNIT PACKS
# ══════════════════════════════════════════════════════════════════════════════

def _set_pack_units(conn, pack_id, unit_ids):
    c = _cur(conn)
    c.execute("DELETE FROM hs_unitpack_units WHERE unitpack_id = %s", (pack_id,))
    for unit_id in dict.fromkeys(unit_ids):
        c.execute("INSERT INTO hs_unitpack_units (unitpack_id, unit_id) VALUES (%s,%s)", (pack_id, int(unit_id)))
    conn.commit()


@router.get("/unitpacks")
def list_unitpacks(request: Request, top: Optional[int] = Query(None, alias="$top", description=_NO_LIMIT_DESC),
                   skip: int = Query(0, alias="$skip"), count: bool = Query(False, alias="$count"),
                   conn=Depends(get_db)):
    return _list(conn, UNITPACKS, request, top, skip)


@router.post("/unitpacks", status_code=201)
def create_unitpack(body: dict = Body(...), conn=Depends(get_db)):
    """Body may include `unit_ids` to populate the pack."""
    pack = _create(conn, UNITPACKS, body)
    if body.get("unit_ids"):
        _set_pack_units(conn, pack["unitpack_id"], body["unit_ids"])
    return pack


@router.get("/unitpacks/{pack_id}")
def get_unitpack(pack_id: int, conn=Depends(get_db)):
    return _get(conn, UNITPACKS, pack_id)


@router.put("/unitpacks/{pack_id}")
def update_unitpack(pack_id: int, body: dict = Body(...), conn=Depends(get_db)):
    """Body may include `unit_ids` to replace the pack's units."""
    pack = _update(conn, UNITPACKS, pack_id, body)
    if "unit_ids" in body:
        _set_pack_units(conn, pack_id, body["unit_ids"] or [])
    return pack


@router.delete("/unitpacks/{pack_id}", status_code=204)
def delete_unitpack(pack_id: int, conn=Depends(get_db)):
    c = _cur(conn)
    c.execute("DELETE FROM hs_unitpack_units WHERE unitpack_id = %s", (pack_id,))
    c.execute("DELETE FROM hs_unitpack_targeting WHERE unitpack_id = %s", (pack_id,))
    return _delete(conn, UNITPACKS, pack_id)


@router.get("/unitpacks/{pack_id}/units")
def get_unitpack_units(pack_id: int, request: Request, conn=Depends(get_db)):
    _get_row(conn, UNITPACKS, pack_id)
    rows = _all(conn, "SELECT u.* FROM hs_units u JOIN hs_unitpack_units p ON p.unit_id = u.unit_id "
                      "WHERE p.unitpack_id = %s ORDER BY u.unit_id", (pack_id,))
    items = _render(conn, UNITPACK_UNITS, rows)
    return _list_response(items, request, None, 0, len(items))


@router.get("/unitpacks/{pack_id}/targeting")
def get_unitpack_targeting(pack_id: int, conn=Depends(get_db)):
    _get_row(conn, UNITPACKS, pack_id)
    rows = _all(conn, "SELECT * FROM hs_unitpack_targeting WHERE unitpack_id = %s ORDER BY created_at", (pack_id,))
    return [dict(r) for r in rows]


@router.post("/unitpacks/{pack_id}/targeting", status_code=201)
def create_unitpack_targeting(pack_id: int, body: dict = Body(...), conn=Depends(get_db)):
    _get_row(conn, UNITPACKS, pack_id)
    id_ = _uuid()
    _cur(conn).execute(
        "INSERT INTO hs_unitpack_targeting (id, unitpack_id, targeting_type, targeting_value, created_at) "
        "VALUES (%s,%s,%s,%s,%s)",
        (id_, pack_id, body.get("type"), body.get("value"), _now()))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_unitpack_targeting WHERE id = %s", (id_,)))


@router.put("/unitpacks/{pack_id}/targeting")
def update_unitpack_targeting(pack_id: int, body: dict = Body(...), conn=Depends(get_db)):
    _get_row(conn, UNITPACKS, pack_id)
    _cur(conn).execute("DELETE FROM hs_unitpack_targeting WHERE unitpack_id = %s", (pack_id,))
    conn.commit()
    return create_unitpack_targeting(pack_id, body, conn)


# ══════════════════════════════════════════════════════════════════════════════
# DEMOGRAPHICS  (inventory-side)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/demographics")
def list_demographics(request: Request,
                      top: Optional[int] = Query(None, alias="$top", description=_NO_LIMIT_DESC),
                      skip: int = Query(0, alias="$skip"), conn=Depends(get_db)):
    return _list(conn, DEMOGRAPHICS, request, top, skip)


@router.post("/demographics", status_code=201)
def create_demographic(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, DEMOGRAPHICS, body)


@router.get("/demographics/{demo_id}")
def get_demographic(demo_id: int, conn=Depends(get_db)):
    return _get(conn, DEMOGRAPHICS, demo_id)


@router.put("/demographics/{demo_id}")
def update_demographic(demo_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, DEMOGRAPHICS, demo_id, body)


# ══════════════════════════════════════════════════════════════════════════════
# LANGUAGES / LOCATIONS / MEDIATYPES  (reference data)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/languages")
def list_languages(request: Request, conn=Depends(get_db)):
    return _list(conn, LANGUAGES, request)


@router.get("/languages/{lang_id}")
def get_language(lang_id: int, conn=Depends(get_db)):
    return _get(conn, LANGUAGES, lang_id)


@router.get("/locations")
def list_locations(request: Request, top: int = Query(1000, alias="$top"), skip: int = Query(0, alias="$skip"),
                   conn=Depends(get_db)):
    return _list(conn, LOCATIONS, request, top, skip)


@router.get("/locations/{loc_id}")
def get_location(loc_id: int, conn=Depends(get_db)):
    return _get(conn, LOCATIONS, loc_id)


@router.get("/mediatypes")
def list_mediatypes(conn=Depends(get_db)):
    """Media types as a tree: root types with nested `children`."""
    items = _render(conn, MEDIATYPES, _all(conn, "SELECT * FROM hs_mediatypes ORDER BY mediatype_id"))
    by_id = {m["mediatype_id"]: m for m in items}
    roots = []
    for m in items:
        parent = by_id.get(m["parent_mediatype_id"])
        (parent["children"] if parent else roots).append(m)
    return roots


# ══════════════════════════════════════════════════════════════════════════════
# UNIT LANGUAGES / UNIT PROPERTIES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/unitlanguages")
def list_unit_languages(request: Request, unit_id: Optional[int] = None, conn=Depends(get_db)):
    where, params = ("unit_id = %s", (unit_id,)) if unit_id is not None else ("", ())
    return _list(conn, UNIT_LANGUAGES, request, None, 0, where, params)


@router.post("/unitlanguages", status_code=201)
def create_unit_language(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, UNIT_LANGUAGES, body)


@router.get("/unitlanguages/{ul_id}")
def get_unit_language(ul_id: int, conn=Depends(get_db)):
    return _get(conn, UNIT_LANGUAGES, ul_id)


@router.put("/unitlanguages/{ul_id}")
def update_unit_language(ul_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, UNIT_LANGUAGES, ul_id, body)


@router.delete("/unitlanguages/{ul_id}", status_code=204)
def delete_unit_language(ul_id: int, conn=Depends(get_db)):
    return _delete(conn, UNIT_LANGUAGES, ul_id)


@router.get("/unitproperties")
def list_unit_properties(request: Request, unit_id: Optional[int] = None, conn=Depends(get_db)):
    where, params = ("unit_id = %s", (unit_id,)) if unit_id is not None else ("", ())
    return _list(conn, UNIT_PROPERTIES, request, None, 0, where, params)


@router.post("/unitproperties", status_code=201)
def create_unit_property(body: dict = Body(...), conn=Depends(get_db)):
    return _create(conn, UNIT_PROPERTIES, body)


@router.get("/unitproperties/{prop_id}")
def get_unit_property(prop_id: int, conn=Depends(get_db)):
    return _get(conn, UNIT_PROPERTIES, prop_id)


@router.put("/unitproperties/{prop_id}")
def update_unit_property(prop_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return _update(conn, UNIT_PROPERTIES, prop_id, body)


@router.delete("/unitproperties/{prop_id}", status_code=204)
def delete_unit_property(prop_id: int, conn=Depends(get_db)):
    return _delete(conn, UNIT_PROPERTIES, prop_id)


# ══════════════════════════════════════════════════════════════════════════════
# HOURLY CONCENTRATIONS
# ══════════════════════════════════════════════════════════════════════════════

_UPSERT_CONCENTRATION = (
    "INSERT INTO hs_concentrations (unit_id, day_of_week, hour, audience_count, updated_at) "
    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (unit_id, day_of_week, hour) DO UPDATE "
    "SET audience_count=EXCLUDED.audience_count, updated_at=EXCLUDED.updated_at"
)


def _upsert_concentrations(conn, unit_id, entries):
    c = _cur(conn)
    for entry in entries:
        c.execute(_UPSERT_CONCENTRATION, (int(unit_id), entry.get("day_of_week"), entry.get("hour"),
                                          entry.get("audience_count", 0), _now()))
    _cur(conn).execute("UPDATE hs_units SET has_audience_data = %s WHERE unit_id = %s", (True, int(unit_id)))


@router.post("/concentrations")
def upsert_concentrations(body: dict = Body(...), conn=Depends(get_db)):
    """Insert or update hourly audience concentration data for a unit."""
    unit_id = body.get("unit_id")
    entries = body.get("data", [])
    _upsert_concentrations(conn, unit_id, entries)
    conn.commit()
    return {"status": "ok", "unit_id": unit_id, "records_upserted": len(entries)}


@router.post("/concentrations/bulk")
def upsert_concentrations_bulk(body: dict = Body(...), conn=Depends(get_db)):
    """Update audience data for multiple units at once."""
    total = 0
    for unit_block in body.get("units", []):
        entries = unit_block.get("data", [])
        _upsert_concentrations(conn, unit_block.get("unit_id"), entries)
        total += len(entries)
    conn.commit()
    return {"status": "ok", "records_upserted": total}


# ══════════════════════════════════════════════════════════════════════════════
# REPORT DEFINITIONS / EXECUTIONS / CUSTOM REPORTS
# ══════════════════════════════════════════════════════════════════════════════

def _execute_report(conn, definition_id):
    """Mock executions finish immediately with a downloadable file URL."""
    now = _now()
    exec_id = _next_id(conn, REPORT_EXECUTIONS)
    _write(conn, REPORT_EXECUTIONS, {
        "report_execution_id": exec_id, "execution_finished_time_utc": now,
        "execution_started_time_utc": now, "progress": 100.0, "report_definition_id": definition_id,
        "report_file_url": f"https://mock.adbridge.local/reports/{exec_id}.csv",
        "status_code": "completed", "status_message": None,
    })
    if definition_id is not None:
        _cur(conn).execute("UPDATE hs_report_definitions SET last_executed_on_utc = %s "
                           "WHERE report_definition_id = %s", (now, definition_id))
    conn.commit()
    return _get(conn, REPORT_EXECUTIONS, exec_id)


@router.get("/reportdefinitions")
def list_report_definitions(request: Request, conn=Depends(get_db)):
    return _list(conn, REPORT_DEFINITIONS, request)


@router.get("/reportdefinitions/{def_id}")
def get_report_definition(def_id: int, conn=Depends(get_db)):
    return _get(conn, REPORT_DEFINITIONS, def_id)


@router.post("/reportdefinitions/{def_id}/run")
def run_report_definition(def_id: int, conn=Depends(get_db)):
    _get_row(conn, REPORT_DEFINITIONS, def_id)
    return _execute_report(conn, def_id)


@router.get("/reportexecutions")
def list_report_executions(request: Request, conn=Depends(get_db)):
    return _list(conn, REPORT_EXECUTIONS, request)


@router.get("/reportexecutions/{exec_id}")
def get_report_execution(exec_id: int, conn=Depends(get_db)):
    return _get(conn, REPORT_EXECUTIONS, exec_id)


@router.post("/customreports")
def create_custom_report(body: dict = Body(...), conn=Depends(get_db)):
    """Ad-hoc report: returns a finished execution (no stored definition)."""
    return _execute_report(conn, None)


@router.get("/customreports/{exec_id}")
def get_custom_report(exec_id: str, conn=Depends(get_db)):
    return _get(conn, REPORT_EXECUTIONS, _int_id(exec_id, "Report execution"))


# ══════════════════════════════════════════════════════════════════════════════
# MEDIA OWNERS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/mediaowners")
def list_media_owners(request: Request, top: int = Query(100, alias="$top"), skip: int = Query(0, alias="$skip"),
                      conn=Depends(get_db)):
    return _list(conn, PUBLISHERS, request, top, skip)


# ══════════════════════════════════════════════════════════════════════════════
# EVENT MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/customevents")
def list_custom_events(conn=Depends(get_db)):
    return _render(conn, CUSTOM_EVENTS, _all(conn, "SELECT * FROM hs_custom_events ORDER BY created_at DESC"))


@router.post("/customevents", status_code=201)
def create_custom_event(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_custom_events (id, name, description, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Event"), body.get("description"), body.get("status", "active"), now, now))
    conn.commit()
    return _get(conn, CUSTOM_EVENTS, id_)


@router.get("/customevents/{event_id}")
def get_custom_event(event_id: str, conn=Depends(get_db)):
    return _get(conn, CUSTOM_EVENTS, event_id)


@router.put("/customevents/{event_id}")
def update_custom_event(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _get_row(conn, CUSTOM_EVENTS, event_id)
    _cur(conn).execute("UPDATE hs_custom_events SET name=%s, status=%s, updated_at=%s WHERE id=%s",
                       (body.get("name", row["name"]), body.get("status", row["status"]), _now(), event_id))
    conn.commit()
    return _get(conn, CUSTOM_EVENTS, event_id)


# Event Data Management
def _event_data(conn, event_id, entity_type):
    rows = _all(conn, "SELECT * FROM hs_event_data WHERE event_id=%s AND entity_type=%s ORDER BY entity_id",
                (event_id, entity_type))
    return _render(conn, EVENT_DATA, rows)


@router.get("/eventdata/active")
def get_active_events(conn=Depends(get_db)):
    return _render(conn, CUSTOM_EVENTS, _all(conn, "SELECT * FROM hs_custom_events WHERE status = %s "
                                                   "ORDER BY created_at DESC", ("active",)))


@router.get("/eventdata/{event_id}/units")
def get_event_units(event_id: str, conn=Depends(get_db)):
    return _event_data(conn, event_id, "unit")


@router.get("/eventdata/{event_id}/sites")
def get_event_sites(event_id: str, conn=Depends(get_db)):
    return _event_data(conn, event_id, "site")


@router.get("/eventdata/{event_id}/locations")
def get_event_locations(event_id: str, conn=Depends(get_db)):
    return _event_data(conn, event_id, "location")


@router.get("/eventdata/{event_id}/global")
def get_event_global(event_id: str, conn=Depends(get_db)):
    row = _get_row(conn, CUSTOM_EVENTS, event_id)
    active = _one(conn, "SELECT COUNT(*) FROM hs_event_data WHERE event_id=%s AND entity_type=%s AND status=%s",
                  (event_id, "unit", "active"))["count"]
    return {"event_id": event_id, "global_status": row["status"], "active_units": active}


def _upsert_event_data(conn, event_id, entity_type, body):
    _get_row(conn, CUSTOM_EVENTS, event_id)
    _cur(conn).execute(
        "INSERT INTO hs_event_data (id, event_id, entity_type, entity_id, status, updated_at) VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (event_id, entity_type, entity_id) DO UPDATE SET status=EXCLUDED.status, updated_at=EXCLUDED.updated_at",
        (_uuid(), event_id, entity_type, str(body.get("entity_id", "")), body.get("status", "active"), _now()))
    conn.commit()
    return {"status": "ok"}


@router.post("/eventdata/{event_id}/units/status")
def update_event_unit_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    return _upsert_event_data(conn, event_id, "unit", body)


@router.post("/eventdata/{event_id}/sites/status")
def update_event_site_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    return _upsert_event_data(conn, event_id, "site", body)


@router.post("/eventdata/{event_id}/locations/status")
def update_event_location_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    return _upsert_event_data(conn, event_id, "location", body)


@router.post("/eventdata/{event_id}/global/status")
def update_event_global_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    _get_row(conn, CUSTOM_EVENTS, event_id)
    if "status" in body:
        _cur(conn).execute("UPDATE hs_custom_events SET status=%s, updated_at=%s WHERE id=%s",
                           (body["status"], _now(), event_id))
        conn.commit()
    return {"event_id": event_id, "status": "ok"}


# ══════════════════════════════════════════════════════════════════════════════
# DSP — CREATIVE SUBMISSION & DEALS
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/dsp/creatives", status_code=201)
def dsp_submit_creative(body: dict = Body(...), conn=Depends(get_db)):
    """DSP creative submission endpoint. Submitted creatives start in review."""
    return _create(conn, CREATIVES, body, state=body.get("state", "pending"), submitted_at=_now())


@router.get("/dsp/creatives")
def dsp_list_creatives(request: Request, top: int = Query(100, alias="$top"), skip: int = Query(0, alias="$skip"),
                       conn=Depends(get_db)):
    return _list(conn, CREATIVES, request, top, skip)


@router.get("/dsp/creatives/{creative_id}")
def dsp_get_creative(creative_id: str, conn=Depends(get_db)):
    return _get(conn, CREATIVES, _int_id(creative_id, "Creative"))


@router.get("/dsp/creatives/{creative_id}/approvals")
def dsp_get_creative_approvals(creative_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_creative_approvals WHERE creative_id = %s ORDER BY created_at",
                (_int_id(creative_id, "Creative"),))
    return _render(conn, CREATIVE_APPROVALS, rows)


@router.get("/dsp/deals")
def dsp_list_deals(request: Request, top: int = Query(100, alias="$top"), skip: int = Query(0, alias="$skip"),
                   conn=Depends(get_db)):
    return _list(conn, DEALS, request, top, skip)


@router.get("/dsp/deals/{deal_id}")
def dsp_get_deal(deal_id: str, conn=Depends(get_db)):
    return _get(conn, DEALS, deal_id)


# ══════════════════════════════════════════════════════════════════════════════
# OPENRTB 2.5 BID ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/openrtb/bidrequests")
def receive_bid_request(body: dict = Body(...), conn=Depends(get_db)):
    """Receive an OpenRTB 2.5 bid request, store it, return a mock bid response."""
    req_id = body.get("id", _uuid())
    _cur(conn).execute(
        "INSERT INTO hs_bid_requests (id, payload, received_at) VALUES (%s,%s,%s) "
        "ON CONFLICT (id) DO NOTHING",
        (req_id, json.dumps(body), _now()))
    conn.commit()
    imps = body.get("imp", [{}])
    cur_list = body.get("cur", ["USD"])
    currency = cur_list[0] if cur_list else "USD"
    bids = []
    for imp in imps:
        floor = imp.get("bidfloor", 1.0) or 1.0
        deal_id = None
        pmp = imp.get("pmp", {})
        if pmp and pmp.get("deals"):
            deal_id = pmp["deals"][0].get("id")
        bids.append({
            "id": str(abs(hash(imp.get("id", "1"))))[:16],
            "impid": imp.get("id", "1"),
            "price": floor,
            "adm": _mock_vast(),
            "adomain": ["mock-advertiser.com"],
            "crid": f"CRID-{_uuid()[:8]}",
            "cat": ["IAB3"],
            "dealid": deal_id,
            "nurl": "https://mock.adbridge.local/hivestack/win?auction_id=${AUCTION_ID}&price=${AUCTION_PRICE}",
            "burl": "https://mock.adbridge.local/hivestack/bill?total=${TOTAL_IMP}&price=${AUCTION_PRICE}",
            "lurl": "https://mock.adbridge.local/hivestack/loss?reason=${AUCTION_LOSS}",
            "ext": {
                "impurls": ["https://mock.adbridge.local/hivestack/imp?total=${TOTAL_IMP}&target=${TARGET_IMP}&price=${TOTAL_PRICE}"],
                "targetdemo": ["GNDR-*", "AGE-18-54"],
            },
        })
    return {"id": req_id, "cur": currency, "bidid": _uuid(),
            "seatbid": [{"bid": bids, "seat": "mock-seat-001"}]}


@router.get("/openrtb/bidrequests")
def list_bid_requests(request: Request, top: int = Query(25, alias="$top"), skip: int = Query(0, alias="$skip"),
                      conn=Depends(get_db)):
    return _list(conn, BID_REQUESTS, request, top, skip)


@router.get("/openrtb/bidrequests/{request_id}")
def get_bid_request(request_id: str, conn=Depends(get_db)):
    return _get(conn, BID_REQUESTS, request_id)


# ══════════════════════════════════════════════════════════════════════════════
# TRACKING CALLBACKS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/win")
def win_notice(auction_id: Optional[str] = None, price: Optional[float] = None, conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, price, recorded_at) VALUES (%s,%s,%s,%s)",
        (auction_id, "win", price, _now()))
    conn.commit()
    return {"status": "ok", "event": "win", "auction_id": auction_id}


@router.get("/bill")
def billing_notice(auction_id: Optional[str] = None, total: Optional[float] = None,
                   price: Optional[float] = None, conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, total_imp, price, recorded_at) VALUES (%s,%s,%s,%s,%s)",
        (auction_id, "bill", total, price, _now()))
    conn.commit()
    return {"status": "ok", "event": "bill", "auction_id": auction_id}


@router.get("/imp")
def impression_notice(auction_id: Optional[str] = None, total: Optional[float] = None,
                      target: Optional[float] = None, price: Optional[float] = None,
                      conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, total_imp, target_imp, price, recorded_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (auction_id, "impression", total, target, price, _now()))
    conn.commit()
    return {"status": "ok", "event": "impression", "total_imp": total, "target_imp": target}


@router.get("/loss")
def loss_notice(auction_id: Optional[str] = None, reason: Optional[str] = None, conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, recorded_at) VALUES (%s,%s,%s)",
        (auction_id, f"loss:{reason}", _now()))
    conn.commit()
    return {"status": "ok", "event": "loss"}


@router.get("/impressions")
def list_impressions(request: Request, event_type: Optional[str] = None, top: int = Query(50, alias="$top"),
                     skip: int = Query(0, alias="$skip"), conn=Depends(get_db)):
    where, params = ("event_type = %s", (event_type,)) if event_type else ("", ())
    return _list(conn, IMPRESSIONS, request, top, skip, where, params)


# ══════════════════════════════════════════════════════════════════════════════
# SELLERS.JSON
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/sellers.json")
def sellers_json(conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_publishers ORDER BY id")
    sellers = [{"seller_id": r["id"], "name": r["name"], "domain": r["domain"],
                "seller_type": r.get("seller_type", "PUBLISHER"),
                "ext": {"impression_multiplier_verifier": {"domain": r["domain"], "seller_id": r["id"]}}}
               for r in rows]
    return {"sellers": sellers, "contact_email": "support@hivestack.com", "version": "1.0"}
