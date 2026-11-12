"""Hivestack sample-capture import: captured records come back from the API unchanged.

Runs against a throwaway SQLite database. Skipped when the capture directory
(data/hivestack/sample, git-ignored) is not present.
"""
import importlib
import json
import os
from pathlib import Path

import pytest

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "hivestack" / "sample"
ENTITIES = [("accounts", "account_id"), ("advertisers", "advertiser_id"), ("networks", "network_id"),
            ("sites", "site_id"), ("units", "unit_id"), ("locations", "location_id")]

pytestmark = pytest.mark.skipif(not (SAMPLE_DIR / "units.json").exists(),
                                reason="Hivestack sample capture not present")


def _raw(entity):
    data = json.loads((SAMPLE_DIR / f"{entity}.json").read_text(encoding="utf-8-sig"))
    return data["records"] if isinstance(data, dict) else data


def _records(entity):
    """What the API should serve: the capture after sanitization."""
    from app.hivestack_sanitize import sanitize
    return sanitize({e: _raw(e) for e, _ in ENTITIES})[entity]


# Values that identify the client and must never be served
_SENSITIVE = {"accounts": ["name", "logo_url", "external_id"],
              "advertisers": ["name", "external_id"],
              "networks": ["name", "code"],
              "sites": ["name", "description", "external_id", "uuid"],
              "units": ["name", "description", "external_id", "uuid", "impression_verifier_vendor_external_id"]}


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    db = tmp_path_factory.mktemp("hs") / "adbridge.db"
    os.environ.update({"DB_BACKEND": "sqlite", "SQLITE_PATH": str(db),
                       "HIVESTACK_SEED": "sample", "HIVESTACK_SAMPLE_DIR": str(SAMPLE_DIR)})
    os.environ.pop("API_KEY", None)
    import app.db_backend as backend
    importlib.reload(backend)
    import app.database as database
    importlib.reload(database)
    backend.init_db()
    import app.routes.hivestack as hs
    importlib.reload(hs)
    app = FastAPI()
    app.include_router(hs.router)
    yield TestClient(app)
    for k in ("HIVESTACK_SEED", "HIVESTACK_SAMPLE_DIR"):
        os.environ.pop(k, None)


@pytest.mark.parametrize("entity,pk", ENTITIES)
def test_each_record_round_trips(client, entity, pk):
    for record in _records(entity):
        r = client.get(f"/hivestack/{entity}/{record[pk]}")
        assert r.status_code == 200, r.text
        assert r.json() == record


@pytest.mark.parametrize("entity,pk", ENTITIES)
def test_list_returns_only_captured_records(client, entity, pk):
    r = client.get(f"/hivestack/{entity}?$count=true&$top=1000")
    assert r.status_code == 200
    expected = sorted(x[pk] for x in _records(entity))
    assert sorted(x[pk] for x in r.json()) == expected
    assert r.headers["x-total-count"] == str(len(expected))


def test_mediatypes_tree_built_from_units(client):
    tree = client.get("/hivestack/mediatypes").json()
    unit_types = {u["media_type"]["mediatype_id"] for u in _records("units")}
    leaves = {c["mediatype_id"] for root in tree for c in root["children"]} | \
             {root["mediatype_id"] for root in tree if not root["children"]}
    assert unit_types <= leaves
    assert all(root["parent_mediatype_id"] is None for root in tree)


def test_synthetic_buy_side_references_real_ids(client):
    advertisers = {a["advertiser_id"] for a in _records("advertisers")}
    accounts = {a["account_id"] for a in _records("accounts")}
    units = {u["unit_id"] for u in _records("units")}
    owners = {u["owner_id"] for u in _records("units")}

    campaigns = client.get("/hivestack/campaigns").json()
    assert campaigns
    for c in campaigns:
        assert c["advertiser_id"] in advertisers
        assert c["account_id"] is None or c["account_id"] in accounts
        assert c["advertiser"]["advertiser_id"] == c["advertiser_id"]

    lineitems = client.get("/hivestack/lineitems?$top=100").json()
    assert lineitems
    for li in lineitems:
        assert set(li["unit_ids"]) <= units
        assert set(li["mediaowner_ids"]) <= owners

    for c in client.get("/hivestack/creatives").json():
        assert c["advertiser_id"] in advertisers
        assert set(c["inventory_targeting"]["unit_ids"]) <= units

    for pack in client.get("/hivestack/unitpacks").json():
        members = client.get(f"/hivestack/unitpacks/{pack['unitpack_id']}/units").json()
        assert {u["unit_id"] for u in members} <= units


def test_no_raw_client_values_are_served(client):
    raw_values = set()
    for entity, fields in _SENSITIVE.items():
        for r in _raw(entity):
            raw_values |= {r[f] for f in fields if isinstance(r.get(f), str) and len(r[f]) > 3}
            ua = r.get("unique_advertiser") or {}
            raw_values |= {d["domain"] for d in ua.get("advertiser_domains", [])}
            if ua.get("logo_url"):
                raw_values.add(ua["logo_url"])
    assert raw_values
    served = "".join(client.get(f"/hivestack/{e}?$top=1000").text for e in
                     ("accounts", "advertisers", "networks", "sites", "units", "campaigns",
                      "lineitems", "creatives", "unitpacks"))
    leaked = sorted(v for v in raw_values if v in served)
    assert not leaked, f"raw client values served: {leaked[:5]}"


def test_ids_and_references_survive_sanitization():
    for entity, pk in ENTITIES:
        assert [r[pk] for r in _records(entity)] == [r[pk] for r in _raw(entity)]
    sites = {r["site_id"] for r in _records("sites")}
    assert {u["site_id"] for u in _records("units")} <= sites


def test_unknown_entity_is_rejected():
    from app.hivestack_sanitize import sanitize
    with pytest.raises(ValueError):
        sanitize({"campaigns": [{"campaign_id": 1, "name": "x"}]})


def test_new_records_use_capture_organization(client):
    org = _records("accounts")[0]["organization_id"]
    r = client.post("/hivestack/advertisers", json={"name": "Imported Org Test"})
    assert r.status_code == 201
    assert r.json()["organization_id"] == org


def test_upcoming_creatives_for_real_unit(client):
    unit = _records("units")[0]
    r = client.get(f"/hivestack/adserving/units/{unit['uuid']}/schedulevast")
    assert r.status_code == 200 and "<VAST" in r.text
