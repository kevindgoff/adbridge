"""Sanitize captured Hivestack records before they are stored.

Rule (see .kiro/steering/data-handling.md): client data pulled from a live API
is sanitized on import. Raw values never reach the database or the API.

What is replaced, per entity:
  - names, descriptions, codes, external IDs: generic labels built from the
    record's own numeric ID (e.g. "Sample Site 277426"), so they are stable
    across imports and can't be reversed into the original text
  - street addresses and people's names (site / unit descriptions)
  - logo URLs (cleared) and advertiser domains (example.com)
  - UUIDs: regenerated deterministically from the numeric ID
  - site / unit coordinates: rounded to 2 decimals (about 1 km)

What is kept: numeric IDs and the references between them, organization /
owner IDs, timestamps, flags, screen specs, media types and public geography
(the locations table and the location objects embedded in units).
"""

import copy
import uuid

_NS = uuid.UUID("5b0f3e3a-2d64-4a8e-9c1e-6a1d8c1b7f10")  # fixed namespace for regenerated UUIDs


def _uuid_for(kind, id_):
    return str(uuid.uuid5(_NS, f"{kind}:{id_}"))


def _round(v):
    return round(v, 2) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def _account(r, ctx):
    i = r["account_id"]
    r["name"] = f"Sample Account {i}"
    r["logo_url"] = None
    if r.get("external_id") is not None:
        r["external_id"] = f"ACCT-{i}"


def _advertiser(r, ctx):
    i = r["advertiser_id"]
    r["name"] = f"Sample Advertiser {i}"
    if r.get("external_id") is not None:
        r["external_id"] = f"ADV-{i}"
    ua = r.get("unique_advertiser")
    if isinstance(ua, dict):
        uid = ua.get("uniqueadvertiser_id", i)
        ua["name"] = r["name"]
        ua["logo_url"] = None
        ua["advertiser_domains"] = [{"domain": f"advertiser-{uid}.example.com"}
                                    for _ in ua.get("advertiser_domains") or []]


def _network(r, ctx):
    i = r["network_id"]
    r["name"] = f"Sample Network {i}"
    r["code"] = f"NET{i}"


def _site(r, ctx):
    i = r["site_id"]
    city = ctx["city"].get(r.get("location_id"))
    r["name"] = f"Sample Site {i}"
    r["description"] = f"Sample venue, {city}" if city else "Sample venue"
    r["external_id"] = f"SITE-{i}"
    r["uuid"] = _uuid_for("site", i)
    r["latitude"], r["longitude"] = _round(r.get("latitude")), _round(r.get("longitude"))


def _unit(r, ctx):
    i = r["unit_id"]
    r["name"] = f"Sample Unit {i}"
    r["description"] = f"Sample screen {i}"
    r["external_id"] = f"UNIT-{i}"
    r["uuid"] = _uuid_for("unit", i)
    r["latitude"], r["longitude"] = _round(r.get("latitude")), _round(r.get("longitude"))
    if r.get("impression_verifier_vendor_external_id") is not None:
        r["impression_verifier_vendor_external_id"] = f"IV-{i}"


_RULES = {"accounts": _account, "advertisers": _advertiser, "networks": _network,
          "sites": _site, "units": _unit, "locations": None}


def sanitize(captured):
    """Return a sanitized deep copy of {entity: [records]}.

    Raises ValueError for an entity with no rule, so a new capture type can't
    be imported unsanitized by accident. `locations` is public geography and
    passes through unchanged.
    """
    unknown = sorted(set(captured) - set(_RULES))
    if unknown:
        raise ValueError(f"No sanitization rule for Hivestack entities {unknown}; "
                         "add one to app/hivestack_sanitize.py before importing them")
    out = copy.deepcopy(captured)
    ctx = {"city": {loc["location_id"]: loc.get("name") for loc in out.get("locations", [])}}
    for entity, records in out.items():
        rule = _RULES[entity]
        if rule:
            for r in records:
                rule(r, ctx)
    return out
