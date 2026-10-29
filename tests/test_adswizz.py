"""AdsWizz Domain API v9 mock: route coverage and v9 behaviour.

Runs against a throwaway SQLite database, so no Postgres is needed.
"""
import importlib
import os
import re

import pytest
from fastapi.routing import APIRoute


def _routes(router):
    return {(m, r.path[len(router.prefix):])
            for r in router.routes if isinstance(r, APIRoute) for m in r.methods}


V8_ROUTES = {
    ("GET", "/agencies"), ("POST", "/agencies"), ("GET", "/agencies/{agency_id}"),
    ("PUT", "/agencies/{agency_id}"), ("GET", "/advertisers"), ("POST", "/advertisers"),
    ("GET", "/advertisers/{advertiser_id}"), ("PUT", "/advertisers/{advertiser_id}"),
    ("GET", "/advertisers/{advertiser_id}/campaigns"), ("GET", "/campaigns"),
    ("POST", "/campaigns"), ("GET", "/campaigns/{campaign_id}"),
    ("PUT", "/campaigns/{campaign_id}"), ("PATCH", "/campaigns/{campaign_id}"),
    ("PUT", "/campaigns/{campaign_id}/archive"), ("PUT", "/campaigns/{campaign_id}/unarchive"),
    ("GET", "/ads"), ("GET", "/campaigns/{campaign_id}/ads"),
    ("POST", "/campaigns/{campaign_id}/ads"), ("GET", "/campaigns/{campaign_id}/ads/{ad_id}"),
    ("PUT", "/campaigns/{campaign_id}/ads/{ad_id}"), ("GET", "/orders"), ("POST", "/orders"),
    ("GET", "/orders/{order_id}"), ("PUT", "/orders/{order_id}"),
    ("PUT", "/orders/{order_id}/archive"), ("PUT", "/orders/{order_id}/unarchive"),
    ("GET", "/orders/{order_id}/campaigns"), ("GET", "/publishers"), ("POST", "/publishers"),
    ("GET", "/publishers/{publisher_id}"), ("PUT", "/publishers/{publisher_id}"),
    ("GET", "/publishers/{publisher_id}/zones"), ("POST", "/publishers/{publisher_id}/zones"),
    ("GET", "/zones/{zone_id}"), ("PUT", "/publishers/{publisher_id}/zones/{zone_id}"),
    ("GET", "/zone-groups"), ("POST", "/zone-groups"), ("GET", "/zone-groups/{zone_group_id}"),
    ("PUT", "/zone-groups/{zone_group_id}"), ("PUT", "/zone-groups/{zone_group_id}/archive"),
    ("GET", "/zone-groups/{zone_group_id}/zones"), ("POST", "/zone-groups/{zone_group_id}/zones"),
    ("GET", "/categories"), ("GET", "/categories/subcategories"), ("POST", "/creatives"),
    ("GET", "/targeting-zones"),
}


def test_prefix_is_v9():
    from app.routes import adswizz
    assert adswizz.router.prefix == "/adswizz/v9"


def test_all_former_v8_routes_still_served():
    from app.routes import adswizz
    missing = V8_ROUTES - _routes(adswizz.router)
    assert not missing, f"routes lost in v9 upgrade: {sorted(missing)}"


def test_new_v9_routes_registered():
    from app.routes import adswizz
    routes = _routes(adswizz.router)
    for expected in [
        ("POST", "/creatives/uploads/presignedurl"),
        ("POST", "/creatives/uploads/complete"),
        ("POST", "/creatives/{advertiser_id}/audiogram"),
        ("GET", "/creatives/audiogram/{creative_id}"),
        ("PATCH", "/creatives/audiogram/{creative_id}"),
        ("DELETE", "/creatives/audiogram/{creative_id}"),
    ]:
        assert expected in routes


def test_no_duplicate_v9_routes():
    from app.routes import adswizz
    seen = [(m, r.path) for r in adswizz.router.routes
            if isinstance(r, APIRoute) for m in r.methods]
    assert len(seen) == len(set(seen))


# ── Behaviour (SQLite-backed app) ─────────────────────────────────────────────

@pytest.fixture(scope="module")
def client(tmp_path_factory):
    from fastapi.testclient import TestClient

    db = tmp_path_factory.mktemp("aw") / "adbridge.db"
    os.environ["DB_BACKEND"] = "sqlite"
    os.environ["SQLITE_PATH"] = str(db)
    os.environ.pop("API_KEY", None)
    import app.db_backend as backend
    importlib.reload(backend)
    import app.database as database
    importlib.reload(database)
    backend.init_db()
    import app.routes.adswizz as aw
    importlib.reload(aw)

    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(aw.router)
    return TestClient(app)


def test_advertisers_have_agency_id(client):
    rows = client.get("/adswizz/v9/advertisers").json()
    assert rows and all("agencyId" in r for r in rows)
    assert rows[0]["agencyId"] is not None


def test_advertiser_domain_required(client):
    body = {"name": "No Domain", "contact": "c", "email": "e@x.com"}
    assert client.post("/adswizz/v9/advertisers", json=body).status_code == 400
    body["domain"] = "nodomain.com"
    r = client.post("/adswizz/v9/advertisers", json=body, headers={"agency": "1"})
    assert r.status_code == 200 and r.json()["agency_id"] == 1


def test_advertiser_campaigns_light_domain(client):
    rows = client.get("/adswizz/v9/advertisers/1/campaigns").json()
    assert rows
    assert "objective" in rows[0] and "campaignType" in rows[0]
    assert "objective_type" not in rows[0]


def test_campaign_type_reserved_rejected(client):
    body = {"name": "R", "advertiserId": 1, "campaignType": "RESERVED"}
    assert client.post("/adswizz/v9/campaigns", json=body).status_code == 400


def test_publishers_summary_shape(client):
    rows = client.get("/adswizz/v9/publishers").json()
    assert set(rows[0]) == {"id", "name", "contact", "website", "email", "externalref"}


def test_removed_ad_type_and_missing_creative(client):
    base = {"name": "A", "status": "ACTIVE"}
    r = client.post("/adswizz/v9/campaigns/1/ads", json={**base, "type": "REDIRECT_DAAST"})
    assert r.status_code == 400
    r = client.post("/adswizz/v9/campaigns/1/ads",
                    json={**base, "type": "VIDEO", "missingCreative": True})
    assert r.status_code == 400  # durationMilliseconds required
    r = client.post("/adswizz/v9/campaigns/1/ads",
                    json={**base, "type": "VIDEO", "missingCreative": True,
                          "durationMilliseconds": 30000})
    assert r.status_code == 201
    ad = r.json()
    assert ad["data"]["missingCreative"] is True
    got = client.get(f"/adswizz/v9/campaigns/1/ads/{ad['id']}").json()
    assert got["data"]["missingCreative"] is True


def test_multipart_upload_flow(client):
    size = 250 * 1024 * 1024
    r = client.post("/adswizz/v9/creatives/uploads/presignedurl",
                    json={"fileName": "big.mp4", "sizeBytes": size})
    assert r.status_code == 200
    init = r.json()
    assert init["parts"] == 3 and len(init["urls"]) == 3
    assert init["urls"][-1]["rangeEnd"] == size - 1
    parts = [{"partNumber": u["partNumber"], "etag": f"etag{u['partNumber']}"}
             for u in init["urls"]]
    bad = client.post("/adswizz/v9/creatives/uploads/complete",
                      json={"uploadId": init["uploadId"], "parts": parts[:2]})
    assert bad.status_code == 400
    done = client.post("/adswizz/v9/creatives/uploads/complete",
                       json={"uploadId": init["uploadId"], "parts": parts})
    assert done.status_code == 200
    assert re.match(r"^_ad_", done.json()["creativeIdentifier"])
    again = client.post("/adswizz/v9/creatives/uploads/complete",
                        json={"uploadId": init["uploadId"], "parts": parts})
    assert again.status_code == 400
    assert client.post("/adswizz/v9/creatives/uploads/presignedurl",
                       json={"fileName": "x.exe", "sizeBytes": 10}).status_code == 400


def test_audiogram_lifecycle(client):
    body = {"creativeName": "gram", "audioCreativeIdentifier": "_ad_a",
            "audioFileExtension": "mp3", "displayCreativeIdentifier": "_ad_d",
            "displayFileExtension": "png"}
    assert client.post("/adswizz/v9/creatives/9999/audiogram", json=body).status_code == 404
    assert client.post("/adswizz/v9/creatives/1/audiogram",
                       json={**body, "audioFileExtension": "wma"}).status_code == 400
    r = client.post("/adswizz/v9/creatives/1/audiogram", json=body)
    assert r.status_code == 201 and r.json()["status"] == "PROCESSING"
    cid = r.json()["id"]
    # PROCESSING cannot be deleted
    assert client.delete(f"/adswizz/v9/creatives/audiogram/{cid}").status_code == 400
    assert client.get(f"/adswizz/v9/creatives/audiogram/{cid}").json()["status"] == "PUBLISHED"
    p = client.patch(f"/adswizz/v9/creatives/audiogram/{cid}", json={"creativeName": "renamed"})
    assert p.json() == {"id": cid, "creativeName": "renamed", "status": "PUBLISHED"}
    assert client.delete(f"/adswizz/v9/creatives/audiogram/{cid}").status_code == 204
    assert client.get(f"/adswizz/v9/creatives/audiogram/{cid}").status_code == 404


def test_v8_prefix_gone(client):
    assert client.get("/adswizz/v8/campaigns").status_code == 404


def test_unchanged_endpoints_respond(client):
    for path in ["/campaigns", "/orders", "/zone-groups", "/categories", "/agencies/1"]:
        assert client.get(f"/adswizz/v9{path}").status_code == 200, path


# ── Order push ────────────────────────────────────────────────────────────────

B = "/adswizz/v9"
H = {"agency": "1"}


def test_order_push_routes_registered():
    from app.routes import adswizz
    routes = _routes(adswizz.router)
    for expected in [
        ("POST", "/campaigns/check-duplicates"),
        ("POST", "/campaigns/{campaign_id}/ads/{ad_id}/zone-groups"),
        ("GET", "/campaigns/{campaign_id}/ads/{ad_id}/targeting-rules"),
        ("PUT", "/campaigns/{campaign_id}/ads/{ad_id}/targeting-rules"),
        ("GET", "/ads/{ad_id}"),
        ("GET", "/ads/{ad_id}/targeting-rules"),
    ]:
        assert expected in routes, expected


def _order_body(**over):
    body = {
        "name": "WO Order 123", "advertiserId": 1,
        "startDate": "2026-11-01T00:00:00Z", "endDate": "2026-11-30T23:59:59Z",
        "objective": {"type": "IMPRESSIONS", "value": 500000, "currency": "USD"},
        "externalReference": "WO-123", "dealId": "D-1",
        "cappingRules": [{"id": None, "unit": "DAYS", "unitMultiplier": 1, "limit": 3}],
    }
    body.update(over)
    return body


def _campaign_body(order_id, **over):
    body = {
        "name": "WO Line 1", "campaignType": "STANDARD", "advertiserId": 1,
        "orderId": order_id,
        "startDate": "2026-11-01T00:00:00Z", "endDate": "2026-11-30T23:59:59Z",
        "campaignRevenue": {"type": "CPM", "value": 12.5, "currency": "USD"},
        "billing": "SOLD", "objective": {"type": "IMPRESSIONS", "value": 250000},
        "campaignDeliveryPacing": {"type": "EVENLY", "priority": 5},
        "categoryIds": [1, 2], "adClashing": True, "inventoryPack": {"dealId": "PMP-9"},
        "cappingRules": [{"unit": "HOURS", "unitMultiplier": 12, "limit": 1}],
        "externalReference": "WO-123-L1",
    }
    body.update(over)
    return body


def test_order_push_end_to_end(client):
    # Order
    r = client.post(f"{B}/orders", headers=H, json=_order_body())
    assert r.status_code == 201, r.text
    order = r.json()
    oid = order["id"]
    assert order["advertiserId"] == 1 and order["startDate"] == "2026-11-01T00:00:00Z"
    assert order["externalReference"] == "WO-123" and order["dealId"] == "D-1"
    assert order["archived"] is False
    assert order["objective"]["managedObjective"] is False
    [rule] = order["cappingRules"]
    assert rule["id"] and rule["unit"] == "DAYS" and rule["limit"] == 3
    # Backward compatibility: snake_case keys are still there
    assert order["advertiser_id"] == 1 and order["external_reference"] == "WO-123"

    # Duplicate guard: nothing matches yet
    dup_req = {"advertiserId": 1, "startDate": "2026-11-01T00:00:00Z",
               "endDate": "2026-11-30T23:59:59Z", "objectiveValue": 250000,
               "objectiveType": "IMPRESSIONS"}
    dup = client.post(f"{B}/campaigns/check-duplicates", headers=H, json=dup_req).json()
    assert dup == {"validationEvents": [], "valid": True}

    # Campaign (order line)
    r = client.post(f"{B}/campaigns", headers=H, json=_campaign_body(oid))
    assert r.status_code == 201, r.text
    camp = r.json()
    cid = camp["id"]
    assert camp["orderId"] == oid and camp["campaignType"] == "STANDARD"
    assert camp["categoryIds"] == [1, 2] and camp["adClashing"] is True
    assert camp["inventoryPack"] == {"dealId": "PMP-9"}
    assert camp["cappingRules"][0]["unitMultiplier"] == 12
    assert camp["status"] == "DRAFT" and camp["order_id"] == oid

    # Same line again is now flagged, matching on +0000 offsets too
    dup = client.post(f"{B}/campaigns/check-duplicates", headers=H,
                      json={**dup_req, "startDate": "2026-11-01T00:00:00+0000"}).json()
    assert dup["valid"] is False and dup["validationEvents"][0]["invalidValue"] == cid

    # Ad without a creative (we don't push creatives)
    r = client.post(f"{B}/campaigns/{cid}/ads", headers=H, json={
        "name": "Spot 30", "status": "ACTIVE", "type": "AUDIO",
        "durationMilliseconds": 30000,
        "cappingRules": [{"unit": "DAYS", "unitMultiplier": 1, "limit": 2}]})
    assert r.status_code == 201, r.text
    ad = r.json()
    aid = ad["id"]
    assert ad["data"]["cappingRules"][0]["limit"] == 2
    assert ad["data"]["includedInCampaignObjective"] is True

    # Inventory: zone groups, then full targeting
    assert client.post(f"{B}/campaigns/{cid}/ads/{aid}/zone-groups",
                       headers=H, json=[2, 1, 2]).json() == [1, 2]
    t = client.get(f"{B}/campaigns/{cid}/ads/{aid}/targeting-rules", headers=H).json()
    assert t["zoneGroupIds"] == [1, 2] and t["zoneIds"] == [] and t["rules"] == []
    rules = [{"type": "LOCATION_COUNTRY", "values": ["US"]}]
    r = client.put(f"{B}/campaigns/{cid}/ads/{aid}/targeting-rules", headers=H, json={
        "zoneIds": [1], "zoneGroupIds": [3], "rules": rules,
        "deliveryInstructions": {"adBreakPositions": ["PRE_ROLL"]}})
    assert r.status_code == 200, r.text
    assert r.json() == {"zoneIds": [1], "zoneGroupIds": [3], "rules": rules,
                        "deliveryInstructions": {"adBreakPositions": ["PRE_ROLL"]}}
    # Linking zone groups replaces the groups but keeps the rest of the targeting
    client.post(f"{B}/campaigns/{cid}/ads/{aid}/zone-groups", headers=H, json=[4])
    t = client.get(f"{B}/ads/{aid}/targeting-rules", headers=H).json()
    assert t["zoneGroupIds"] == [4] and t["zoneIds"] == [1] and t["rules"] == rules
    assert client.get(f"{B}/ads/{aid}", headers=H).json()["campaignId"] == cid

    # Launch (permissive)
    r = client.patch(f"{B}/campaigns/{cid}?action=launch", headers=H)
    assert r.status_code == 200 and r.json()["status"] == "RUNNING"

    # Read back
    lines = client.get(f"{B}/orders/{oid}/campaigns", headers=H).json()
    assert [c["id"] for c in lines] == [cid]
    since = client.get(f"{B}/orders?modifiedAfter=2026-01-01T00:00:00Z", headers=H).json()
    assert oid in [o["id"] for o in since]
    future = client.get(f"{B}/orders?modifiedAfter=2999-01-01T00:00:00Z", headers=H).json()
    assert future == []

    # Update: replace capping (keep the existing rule by id, add one)
    upd = _order_body(name="WO Order 123 v2", cappingRules=[
        {"id": rule["id"], "unit": "WEEKS", "unitMultiplier": 1, "limit": 10},
        {"unit": "HOURS", "unitMultiplier": 1, "limit": 1}])
    got = client.put(f"{B}/orders/{oid}", headers=H, json=upd).json()
    assert got["name"] == "WO Order 123 v2"
    assert [c["id"] == rule["id"] for c in got["cappingRules"]] == [True, False]
    assert got["cappingRules"][0]["unit"] == "WEEKS"
    # null removes all rules (spec)
    got = client.put(f"{B}/orders/{oid}", headers=H,
                     json=_order_body(cappingRules=None)).json()
    assert got["cappingRules"] == []

    # Cancel
    assert client.patch(f"{B}/campaigns/{cid}?action=pause", headers=H).json()["status"] == "PAUSED"
    assert client.put(f"{B}/orders/{oid}/archive", headers=H).status_code == 200
    assert client.get(f"{B}/orders/{oid}", headers=H).json()["archived"] is True


def test_order_push_validation_errors(client):
    def err(r, status, element):
        assert r.status_code == status, r.text
        body = r.json()
        assert body["code"] and body["message"]
        assert "detail" in body  # kept for existing callers
        if element:
            assert body["errors"][0]["element"] == element
        return body

    err(client.post(f"{B}/orders", json=_order_body(advertiserId=99999)), 404, None)
    bad = _order_body()
    del bad["startDate"]
    err(client.post(f"{B}/orders", json=bad), 400, "startDate")
    err(client.post(f"{B}/orders", json=_order_body(endDate="2026-10-01T00:00:00Z")),
        400, "endDate")
    err(client.post(f"{B}/orders", json=_order_body(
        cappingRules=[{"unit": "YEARS", "unitMultiplier": 1, "limit": 1}])),
        400, "cappingRules[0].unit")
    err(client.post(f"{B}/orders", json=_order_body(objective={"type": "CLICKS"})),
        400, "objective.type")

    err(client.post(f"{B}/campaigns", json=_campaign_body(99999)), 404, None)
    err(client.post(f"{B}/campaigns", json=_campaign_body(None, billing="MAYBE")), 400, "billing")
    err(client.post(f"{B}/campaigns", json=_campaign_body(None, advertiserId=99999)), 404, None)
    err(client.post(f"{B}/campaigns/99999/ads", json={"name": "a", "type": "AUDIO",
                                                      "status": "ACTIVE"}), 404, None)
    err(client.post(f"{B}/campaigns/check-duplicates", json={"advertiserId": 1}),
        400, "startDate")

    err(client.put(f"{B}/campaigns/1/ads/99999/targeting-rules", json={}), 404, None)
    ad_id = client.get(f"{B}/campaigns/1/ads").json()[0]["id"]
    err(client.put(f"{B}/campaigns/1/ads/{ad_id}/targeting-rules",
                   json={"zoneGroupIds": [99999]}), 400, "zoneGroupIds")
    err(client.put(f"{B}/campaigns/1/ads/{ad_id}/targeting-rules",
                   json={"rules": [{"type": "NOPE"}]}), 400, "rules[0].type")
    err(client.post(f"{B}/campaigns/1/ads/{ad_id}/zone-groups", json=[99999]),
        400, "zoneGroupIds")
    err(client.get(f"{B}/orders?modifiedAfter=yesterday"), 400, "modifiedAfter")


def test_seeded_data_still_round_trips(client):
    """GET-then-PUT of a seeded campaign works and keeps the legacy keys."""
    camp = client.get(f"{B}/campaigns/1").json()
    assert camp["campaign_type"] == camp["campaignType"]
    assert isinstance(camp["archived"], bool)
    r = client.put(f"{B}/campaigns/1", json=camp)
    assert r.status_code == 200, r.text
    # Seeded targeting on the first ad of campaign 1
    first_ad = client.get(f"{B}/campaigns/1/ads").json()[0]["id"]
    assert client.get(f"{B}/ads/{first_ad}/targeting-rules").json()["zoneGroupIds"]


def test_targeting_zones_filters(client):
    all_zones = client.get(f"{B}/targeting-zones").json()
    group_zones = {z["id"] for z in client.get(f"{B}/zone-groups/1/zones").json()}
    filtered = client.get(f"{B}/targeting-zones?zoneGroupIds=1").json()
    assert {z["id"] for z in filtered} == group_zones
    assert len(filtered) < len(all_zones)
    assert client.get(f"{B}/targeting-zones?zoneGroupIds=x").status_code == 400
