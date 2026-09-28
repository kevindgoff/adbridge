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
