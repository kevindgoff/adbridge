"""AdsWizz Domain API v9 mock endpoints under /adswizz/v9.

Mirrors: https://docs.adswizz.com/domain-api/v9/
Spec:    https://docs.adswizz.com/domain-api/v9/openapi.json
(see platform_api_sources.yml for every URL this mock was built from)

Core resources: Agencies, Advertisers, Campaigns, Ads, Orders,
                Publishers, Zones, Zone Groups, Categories, Creatives.

Pagination: page-based (limit + page, default limit=100, page=1).
Response envelope: bare JSON arrays for lists, plain objects for singles.
Auth header: agency (required), environment (optional).

Replaces the former v8 mock. v9 deltas vs v8:
  - Advertiser summaries carry agencyId; create/update require `domain`.
  - GET /advertisers/{id}/campaigns returns CampaignLightDomain (`objective`
    object instead of the v7 campaignObjective/objectiveType fields).
  - campaignType RESERVED removed.
  - GET /publishers returns PublisherSummaryDomainDto (summary fields only).
  - Ad types REDIRECT_DAAST / REDIRECT_TARGETSPOT / REDIRECT_VAST_CLIENT
    removed; VIDEO ads accept `missingCreative`.
  - New: multipart creative upload, audiogram creatives.
  - Removed: GET /commons/openrtb-buyers (never mocked).
"""

import math
import uuid
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Body, Header, Response
from typing import Optional, List

from app.database import get_db

router = APIRouter(prefix="/adswizz/v9")

# ── v9 enums ──────────────────────────────────────────────────────────────────

CAMPAIGN_TYPES = {"STANDARD", "FILLER", "INTERACTIVE", "SPONSORSHIP"}
REMOVED_AD_TYPES = {"REDIRECT_DAAST", "REDIRECT_TARGETSPOT", "REDIRECT_VAST_CLIENT"}
UPLOAD_EXTENSIONS = {
    "mp3", "wma", "aac", "ogg", "wav",                           # audio
    "gif", "jpg", "jpeg", "png",                                 # display
    "swf", "flv", "f4v", "mp4", "m4v", "3gp", "wmv",             # video
}
AUDIOGRAM_AUDIO_EXT = {"mp3", "aac", "ogg", "wav"}
AUDIOGRAM_DISPLAY_EXT = {"jpg", "jpeg", "gif", "png"}
MULTIPART_PART_SIZE = 100 * 1024 * 1024  # 100 MiB per part (mock choice)


# ── Shared helpers ────────────────────────────────────────────────────────────

def _q(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _one(conn, sql, params=()):
    row = _q(conn, sql, params).fetchone()
    return dict(row) if row else None


def _paginate(conn, sql, params, limit=100, page=1, order="id"):
    offset = (page - 1) * limit
    rows = _q(conn, f"{sql} ORDER BY {order} LIMIT %s OFFSET %s",
              (*params, limit, offset)).fetchall()
    return [dict(r) for r in rows]


def _404(resource, id_):
    raise HTTPException(404, {"code": "not.found", "message": f"{resource} not found: {id_}"})


def _400(element, message, code="validation.error"):
    raise HTTPException(400, {
        "code": code, "message": message,
        "errors": [{"code": code, "message": message, "element": element}],
    })


def _agency_id(agency: Optional[str]):
    """The `agency` header carries the agency id; tolerate non-numeric values."""
    try:
        return int(agency) if agency is not None else None
    except ValueError:
        return None


# ── Agencies ──────────────────────────────────────────────────────────────────

@router.get("/agencies")
def list_agencies(limit: int = Query(100, ge=1, le=1000),
                  page: int = Query(1, ge=1),
                  conn=Depends(get_db)):
    return _paginate(conn, "SELECT * FROM aw_agencies", (), limit, page)


@router.post("/agencies", status_code=201)
def create_agency(body: dict = Body(...), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_agencies (name, contact, email, external_reference, currency,
           timezone, margin, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s, NOW())
           RETURNING *""",
        (body["name"], body.get("contact"), body.get("email"),
         body.get("externalReference"), body.get("currency", "USD"),
         body.get("timezone", "UTC"), body.get("margin", 0)),
    )
    conn.commit()
    return dict(cur.fetchone())


@router.get("/agencies/{agency_id}")
def get_agency(agency_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_agencies WHERE id = %s", (agency_id,))
    if not row:
        _404("Agency", agency_id)
    return row


@router.put("/agencies/{agency_id}")
def update_agency(agency_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_agencies WHERE id = %s", (agency_id,))
    if not row:
        _404("Agency", agency_id)
    _q(conn,
        """UPDATE aw_agencies SET name=%s, contact=%s, email=%s,
           external_reference=%s, currency=%s, timezone=%s, margin=%s
           WHERE id=%s""",
        (body["name"], body.get("contact"), body.get("email"),
         body.get("externalReference"), body.get("currency"),
         body.get("timezone"), body.get("margin", 0), agency_id))
    conn.commit()
    return {"id": agency_id}


# ── Advertisers ───────────────────────────────────────────────────────────────

@router.get("/advertisers")
def list_advertisers(limit: int = Query(100, ge=1, le=1000),
                     page: int = Query(1, ge=1),
                     name: Optional[str] = None,
                     conn=Depends(get_db)):
    sql = "SELECT * FROM aw_advertisers"
    params: list = []
    if name:
        sql += " WHERE name ILIKE %s"
        params.append(f"%{name}%")
    rows = _paginate(conn, sql, tuple(params), limit, page)
    for r in rows:
        r["agencyId"] = r.pop("agency_id", None)
    return rows


@router.post("/advertisers")
def create_advertiser(body: dict = Body(...),
                      agency: Optional[str] = Header(None),
                      conn=Depends(get_db)):
    if not body.get("domain"):
        _400("domain", "domain is required")
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_advertisers (name, domain, contact, email, comments,
           external_reference, status, ad_clashing, agency_id, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW()) RETURNING *""",
        (body["name"], body.get("domain"), body["contact"], body["email"],
         body.get("comments"), body.get("externalReference"),
         "ACTIVE", body.get("adClashing", False), _agency_id(agency)),
    )
    conn.commit()
    return dict(cur.fetchone())


@router.get("/advertisers/{advertiser_id}")
def get_advertiser(advertiser_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_advertisers WHERE id = %s", (advertiser_id,))
    if not row:
        _404("Advertiser", advertiser_id)
    return row


@router.put("/advertisers/{advertiser_id}")
def update_advertiser(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    if not body.get("domain"):
        _400("domain", "domain is required")
    row = _one(conn, "SELECT * FROM aw_advertisers WHERE id = %s", (advertiser_id,))
    if not row:
        _404("Advertiser", advertiser_id)
    _q(conn,
        """UPDATE aw_advertisers SET name=%s, domain=%s, contact=%s, email=%s,
           comments=%s, external_reference=%s, ad_clashing=%s WHERE id=%s""",
        (body["name"], body.get("domain"), body["contact"], body["email"],
         body.get("comments"), body.get("externalReference"),
         body.get("adClashing", False), advertiser_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM aw_advertisers WHERE id = %s", (advertiser_id,)))


@router.get("/advertisers/{advertiser_id}/campaigns")
def list_advertiser_campaigns(advertiser_id: int,
                              limit: int = Query(100, ge=1, le=1000),
                              page: int = Query(1, ge=1),
                              status: Optional[str] = None,
                              conn=Depends(get_db)):
    sql = "SELECT * FROM aw_campaigns WHERE advertiser_id = %s"
    params: list = [advertiser_id]
    if status:
        statuses = [s.strip() for s in status.split(",")]
        placeholders = ",".join(["%s"] * len(statuses))
        sql += f" AND status IN ({placeholders})"
        params.extend(statuses)
    rows = _paginate(conn, sql, tuple(params), limit, page)
    return [_format_campaign_light(r) for r in rows]


# ── Campaigns ─────────────────────────────────────────────────────────────────

def _format_campaign_light(row):
    """CampaignLightDomain (v9): `objective` object replaces v7 objective fields."""
    return {
        "id": row["id"],
        "name": row["name"],
        "campaignType": row.get("campaign_type"),
        "advertiserId": row.get("advertiser_id"),
        "orderId": row.get("order_id"),
        "status": row.get("status"),
        "startDate": row.get("start_date"),
        "endDate": row.get("end_date"),
        "archived": row.get("archived", False),
        "objective": {
            "type": row.get("objective_type"),
            "value": row.get("objective_value"),
            "unlimited": row.get("objective_unlimited") or False,
        },
    }


def _check_campaign_type(body):
    ctype = body.get("campaignType", "STANDARD")
    if ctype not in CAMPAIGN_TYPES:
        _400("campaignType", f"campaignType must be one of {sorted(CAMPAIGN_TYPES)}")

def _format_campaign(row):
    """Nest revenue, objective, and pacing into the AdsWizz response shape."""
    rev_type = row.pop("revenue_type", None)
    rev_value = row.pop("revenue_value", None)
    rev_currency = row.pop("revenue_currency", None)
    if rev_type:
        row["campaignRevenue"] = {"type": rev_type, "value": rev_value, "currency": rev_currency}

    obj_type = row.pop("objective_type", None)
    obj_value = row.pop("objective_value", None)
    obj_unlimited = row.pop("objective_unlimited", None)
    row["objective"] = {"type": obj_type, "value": obj_value, "unlimited": obj_unlimited or False}

    pacing_type = row.pop("pacing_type", None)
    pacing_priority = row.pop("pacing_priority", None)
    if pacing_type:
        row["campaignDeliveryPacing"] = {"type": pacing_type, "priority": pacing_priority}

    return row


@router.get("/campaigns")
def list_campaigns(limit: int = Query(100, ge=1, le=1000),
                   page: int = Query(1, ge=1),
                   advertiser_id: Optional[int] = Query(None, alias="advertiserId"),
                   order_id: Optional[int] = Query(None, alias="orderId"),
                   status: Optional[str] = None,
                   query: Optional[str] = None,
                   conn=Depends(get_db)):
    sql = "SELECT * FROM aw_campaigns WHERE 1=1"
    params: list = []
    if advertiser_id:
        sql += " AND advertiser_id = %s"
        params.append(advertiser_id)
    if order_id:
        sql += " AND order_id = %s"
        params.append(order_id)
    if status:
        statuses = [s.strip() for s in status.split(",")]
        placeholders = ",".join(["%s"] * len(statuses))
        sql += f" AND status IN ({placeholders})"
        params.extend(statuses)
    if query:
        sql += " AND (name ILIKE %s OR CAST(id AS TEXT) ILIKE %s)"
        params.extend([f"%{query}%", f"%{query}%"])
    rows = _paginate(conn, sql, tuple(params), limit, page)
    return [_format_campaign(r) for r in rows]


@router.post("/campaigns", status_code=201)
def create_campaign(body: dict = Body(...), conn=Depends(get_db)):
    _check_campaign_type(body)
    rev = body.get("campaignRevenue", {})
    obj = body.get("objective", {})
    pacing = body.get("campaignDeliveryPacing", {})
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_campaigns
           (name, campaign_type, advertiser_id, order_id, status, billing,
            start_date, end_date, revenue_type, revenue_value, revenue_currency,
            objective_type, objective_value, objective_unlimited,
            pacing_type, pacing_priority, comments, external_reference,
            archived, created_at)
           VALUES (%s,%s,%s,%s,'DRAFT',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false,NOW())
           RETURNING *""",
        (body["name"], body.get("campaignType", "STANDARD"),
         body["advertiserId"], body.get("orderId"),
         body.get("billing", "UNSOLD"),
         body.get("startDate"), body.get("endDate"),
         rev.get("type"), rev.get("value"), rev.get("currency", "USD"),
         obj.get("type", "IMPRESSIONS"), obj.get("value"),
         obj.get("unlimited", False),
         pacing.get("type", "EVENLY"), pacing.get("priority", 5),
         body.get("comments"), body.get("externalReference")),
    )
    conn.commit()
    return _format_campaign(dict(cur.fetchone()))


@router.get("/campaigns/{campaign_id}")
def get_campaign(campaign_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        _404("Campaign", campaign_id)
    return _format_campaign(row)


@router.put("/campaigns/{campaign_id}")
def update_campaign(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    _check_campaign_type(body)
    existing = _one(conn, "SELECT * FROM aw_campaigns WHERE id = %s", (campaign_id,))
    if not existing:
        _404("Campaign", campaign_id)
    rev = body.get("campaignRevenue", {})
    obj = body.get("objective", {})
    pacing = body.get("campaignDeliveryPacing", {})
    _q(conn,
        """UPDATE aw_campaigns SET name=%s, campaign_type=%s, billing=%s,
           start_date=%s, end_date=%s, revenue_type=%s, revenue_value=%s,
           revenue_currency=%s, objective_type=%s, objective_value=%s,
           objective_unlimited=%s, pacing_type=%s, pacing_priority=%s,
           comments=%s, external_reference=%s WHERE id=%s""",
        (body["name"], body.get("campaignType", "STANDARD"),
         body.get("billing"), body.get("startDate"), body.get("endDate"),
         rev.get("type"), rev.get("value"), rev.get("currency"),
         obj.get("type"), obj.get("value"), obj.get("unlimited", False),
         pacing.get("type"), pacing.get("priority"),
         body.get("comments"), body.get("externalReference"), campaign_id))
    conn.commit()
    return _format_campaign(dict(_one(conn, "SELECT * FROM aw_campaigns WHERE id = %s", (campaign_id,))))


@router.patch("/campaigns/{campaign_id}")
def campaign_action(campaign_id: int,
                    action: str = Query(...),
                    conn=Depends(get_db)):
    """Launch, pause, or resume a campaign."""
    row = _one(conn, "SELECT * FROM aw_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        _404("Campaign", campaign_id)
    status_map = {"launch": "RUNNING", "pause": "PAUSED", "resume": "RUNNING"}
    new_status = status_map.get(action)
    if not new_status:
        raise HTTPException(400, {"code": "invalid.action", "message": f"Unknown action: {action}"})
    _q(conn, "UPDATE aw_campaigns SET status = %s WHERE id = %s", (new_status, campaign_id))
    conn.commit()
    return _format_campaign(dict(_one(conn, "SELECT * FROM aw_campaigns WHERE id = %s", (campaign_id,))))


@router.put("/campaigns/{campaign_id}/archive")
def archive_campaign(campaign_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        _404("Campaign", campaign_id)
    _q(conn, "UPDATE aw_campaigns SET archived = true WHERE id = %s", (campaign_id,))
    conn.commit()
    return None


@router.put("/campaigns/{campaign_id}/unarchive")
def unarchive_campaign(campaign_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        _404("Campaign", campaign_id)
    _q(conn, "UPDATE aw_campaigns SET archived = false WHERE id = %s", (campaign_id,))
    conn.commit()
    return None


# ── Ads ───────────────────────────────────────────────────────────────────────

def _format_ad(row):
    """Shape flat DB row into AdsWizz ad response."""
    ad = {
        "id": row["id"],
        "campaignId": row["campaign_id"],
        "archived": row.get("archived", False),
        "type": row["type"],
        "adUnitId": row.get("ad_unit_id", 0),
        "data": {
            "name": row["name"],
            "status": row["status"],
            "includedInCampaignObjective": row.get("included_in_objective", True),
            "weight": row.get("weight", 1),
            "comments": row.get("comments"),
            "externalReference": row.get("external_reference"),
            "trackingType": row.get("tracking_type"),
            "tracking": row.get("tracking"),
            "type": row["type"],
            "creativeFileName": row.get("creative_file_name"),
            "durationMilliseconds": row.get("duration_ms"),
            "destinationUrl": row.get("destination_url"),
        },
    }
    if row["type"] == "VIDEO":
        ad["data"]["missingCreative"] = bool(row.get("missing_creative"))
    return ad


def _check_ad_body(body):
    if body.get("type") in REMOVED_AD_TYPES:
        _400("type", f"Ad type {body['type']} is not supported in v9")
    if body.get("missingCreative"):
        if body.get("type") != "VIDEO":
            _400("missingCreative", "missingCreative applies to VIDEO ads only")
        if not body.get("durationMilliseconds"):
            _400("durationMilliseconds",
                 "durationMilliseconds is required when missingCreative is true")


@router.get("/ads")
def filter_ads(limit: int = Query(100, ge=1, le=1000),
               page: int = Query(1, ge=1),
               campaign_id: Optional[int] = Query(None, alias="campaignId"),
               conn=Depends(get_db)):
    sql = "SELECT * FROM aw_ads WHERE 1=1"
    params: list = []
    if campaign_id:
        sql += " AND campaign_id = %s"
        params.append(campaign_id)
    rows = _paginate(conn, sql, tuple(params), limit, page)
    return [{"id": r["id"], "name": r["name"], "type": r["type"],
             "subtype": r.get("subtype", r["type"]),
             "status": r["status"],
             "filename": r.get("creative_file_name")} for r in rows]


@router.get("/campaigns/{campaign_id}/ads")
def list_campaign_ads(campaign_id: int,
                      limit: int = Query(100, ge=1, le=1000),
                      page: int = Query(1, ge=1),
                      conn=Depends(get_db)):
    rows = _paginate(conn, "SELECT * FROM aw_ads WHERE campaign_id = %s",
                     (campaign_id,), limit, page)
    return [{"id": r["id"], "name": r["name"], "type": r["type"],
             "subtype": r.get("subtype", r["type"]),
             "status": r["status"],
             "filename": r.get("creative_file_name"),
             "archived": r.get("archived", False)} for r in rows]


@router.post("/campaigns/{campaign_id}/ads", status_code=201)
def create_ad(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    _check_ad_body(body)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_ads
           (campaign_id, name, type, subtype, status, included_in_objective,
            weight, comments, external_reference, tracking_type, tracking,
            creative_file_name, duration_ms, destination_url, missing_creative,
            archived, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false,NOW())
           RETURNING *""",
        (campaign_id, body["name"], body["type"], body.get("subtype", body["type"]),
         body.get("status", "ACTIVE"),
         body.get("includedInCampaignObjective", True),
         body.get("weight", 1), body.get("comments"),
         body.get("externalReference"), body.get("trackingType"),
         body.get("tracking"), body.get("creativeFileName"),
         body.get("durationMilliseconds"), body.get("destinationUrl"),
         bool(body.get("missingCreative", False))),
    )
    conn.commit()
    return _format_ad(dict(cur.fetchone()))


@router.get("/campaigns/{campaign_id}/ads/{ad_id}")
def get_ad(campaign_id: int, ad_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_ads WHERE id = %s AND campaign_id = %s",
               (ad_id, campaign_id))
    if not row:
        _404("Ad", ad_id)
    return _format_ad(row)


@router.put("/campaigns/{campaign_id}/ads/{ad_id}")
def update_ad(campaign_id: int, ad_id: int, body: dict = Body(...), conn=Depends(get_db)):
    _check_ad_body(body)
    existing = _one(conn, "SELECT * FROM aw_ads WHERE id = %s AND campaign_id = %s",
                    (ad_id, campaign_id))
    if not existing:
        _404("Ad", ad_id)
    _q(conn,
        """UPDATE aw_ads SET name=%s, status=%s, weight=%s, comments=%s,
           external_reference=%s, tracking_type=%s, tracking=%s,
           creative_file_name=%s, duration_ms=%s, destination_url=%s,
           missing_creative=%s
           WHERE id=%s""",
        (body["name"], body.get("status", "ACTIVE"), body.get("weight", 1),
         body.get("comments"), body.get("externalReference"),
         body.get("trackingType"), body.get("tracking"),
         body.get("creativeFileName"), body.get("durationMilliseconds"),
         body.get("destinationUrl"),
         bool(body.get("missingCreative", existing.get("missing_creative") or False)),
         ad_id))
    conn.commit()
    return _format_ad(dict(_one(conn, "SELECT * FROM aw_ads WHERE id = %s", (ad_id,))))


# ── Orders ────────────────────────────────────────────────────────────────────

def _format_order(row):
    obj_type = row.pop("objective_type", None)
    obj_value = row.pop("objective_value", None)
    obj_currency = row.pop("objective_currency", None)
    obj_unlimited = row.pop("objective_unlimited", None)
    row["objective"] = {
        "type": obj_type, "value": obj_value,
        "currency": obj_currency, "unlimited": obj_unlimited or False,
    }
    return row


@router.get("/orders")
def list_orders(limit: int = Query(100, ge=1, le=1000),
                page: int = Query(1, ge=1),
                advertiser_id: Optional[int] = Query(None, alias="advertiserId"),
                conn=Depends(get_db)):
    sql = "SELECT * FROM aw_orders WHERE 1=1"
    params: list = []
    if advertiser_id:
        sql += " AND advertiser_id = %s"
        params.append(advertiser_id)
    rows = _paginate(conn, sql, tuple(params), limit, page)
    return [_format_order(r) for r in rows]


@router.post("/orders", status_code=201)
def create_order(body: dict = Body(...), conn=Depends(get_db)):
    obj = body.get("objective", {})
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_orders
           (name, advertiser_id, start_date, end_date,
            objective_type, objective_value, objective_currency, objective_unlimited,
            comments, external_reference, deal_id, archived, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false,NOW()) RETURNING *""",
        (body["name"], body["advertiserId"], body["startDate"], body.get("endDate"),
         obj.get("type", "IMPRESSIONS"), obj.get("value"), obj.get("currency", "USD"),
         obj.get("unlimited", False),
         body.get("comments"), body.get("externalReference"),
         body.get("dealId")),
    )
    conn.commit()
    return _format_order(dict(cur.fetchone()))


@router.get("/orders/{order_id}")
def get_order(order_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_orders WHERE id = %s", (order_id,))
    if not row:
        _404("Order", order_id)
    return _format_order(row)


@router.put("/orders/{order_id}")
def update_order(order_id: int, body: dict = Body(...), conn=Depends(get_db)):
    existing = _one(conn, "SELECT * FROM aw_orders WHERE id = %s", (order_id,))
    if not existing:
        _404("Order", order_id)
    obj = body.get("objective", {})
    _q(conn,
        """UPDATE aw_orders SET name=%s, start_date=%s, end_date=%s,
           objective_type=%s, objective_value=%s, objective_currency=%s,
           objective_unlimited=%s, comments=%s, external_reference=%s,
           deal_id=%s WHERE id=%s""",
        (body["name"], body["startDate"], body.get("endDate"),
         obj.get("type"), obj.get("value"), obj.get("currency"),
         obj.get("unlimited", False),
         body.get("comments"), body.get("externalReference"),
         body.get("dealId"), order_id))
    conn.commit()
    return _format_order(dict(_one(conn, "SELECT * FROM aw_orders WHERE id = %s", (order_id,))))


@router.put("/orders/{order_id}/archive")
def archive_order(order_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_orders WHERE id = %s", (order_id,))
    if not row:
        _404("Order", order_id)
    _q(conn, "UPDATE aw_orders SET archived = true WHERE id = %s", (order_id,))
    conn.commit()
    return None


@router.put("/orders/{order_id}/unarchive")
def unarchive_order(order_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_orders WHERE id = %s", (order_id,))
    if not row:
        _404("Order", order_id)
    _q(conn, "UPDATE aw_orders SET archived = false WHERE id = %s", (order_id,))
    conn.commit()
    return None


@router.get("/orders/{order_id}/campaigns")
def list_order_campaigns(order_id: int,
                         limit: int = Query(100, ge=1, le=1000),
                         page: int = Query(1, ge=1),
                         status: Optional[str] = None,
                         conn=Depends(get_db)):
    sql = "SELECT * FROM aw_campaigns WHERE order_id = %s"
    params: list = [order_id]
    if status:
        statuses = [s.strip() for s in status.split(",")]
        placeholders = ",".join(["%s"] * len(statuses))
        sql += f" AND status IN ({placeholders})"
        params.extend(statuses)
    rows = _paginate(conn, sql, tuple(params), limit, page)
    return [_format_campaign(r) for r in rows]


# ── Publishers ────────────────────────────────────────────────────────────────

@router.get("/publishers")
def list_publishers(limit: int = Query(100, ge=1, le=1000),
                    page: int = Query(1, ge=1),
                    conn=Depends(get_db)):
    """PublisherSummaryDomainDto: summary fields only in v9."""
    rows = _paginate(conn, "SELECT * FROM aw_publishers", (), limit, page)
    return [{"id": r["id"], "name": r["name"], "contact": r.get("contact"),
             "website": r.get("website"), "email": r.get("email"),
             "externalref": r.get("external_reference")} for r in rows]


@router.post("/publishers")
def create_publisher(body: dict = Body(...), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_publishers
           (name, contact, website, email, description, timezone, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,NOW()) RETURNING *""",
        (body["name"], body.get("contact"), body["website"], body["email"],
         body.get("description"), body.get("timeZone", "UTC")),
    )
    conn.commit()
    return dict(cur.fetchone())


@router.get("/publishers/{publisher_id}")
def get_publisher(publisher_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_publishers WHERE id = %s", (publisher_id,))
    if not row:
        _404("Publisher", publisher_id)
    return row


@router.put("/publishers/{publisher_id}")
def update_publisher(publisher_id: int, body: dict = Body(...), conn=Depends(get_db)):
    existing = _one(conn, "SELECT * FROM aw_publishers WHERE id = %s", (publisher_id,))
    if not existing:
        _404("Publisher", publisher_id)
    _q(conn,
        """UPDATE aw_publishers SET name=%s, contact=%s, website=%s, email=%s,
           description=%s, timezone=%s WHERE id=%s""",
        (body["name"], body.get("contact"), body["website"], body["email"],
         body.get("description"), body.get("timeZone"), publisher_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM aw_publishers WHERE id = %s", (publisher_id,)))


# ── Zones ─────────────────────────────────────────────────────────────────────

@router.get("/publishers/{publisher_id}/zones")
def list_zones(publisher_id: int,
               limit: int = Query(100, ge=1, le=1000),
               page: int = Query(1, ge=1),
               conn=Depends(get_db)):
    return _paginate(conn, "SELECT * FROM aw_zones WHERE publisher_id = %s",
                     (publisher_id,), limit, page)


@router.post("/publishers/{publisher_id}/zones")
def create_zone(publisher_id: int, body: dict = Body(...), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_zones
           (publisher_id, name, description, type, format_id,
            width, height, duration_min, duration_max, comments, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) RETURNING *""",
        (publisher_id, body["name"], body.get("description"),
         body["type"], body.get("formatId"),
         body.get("width"), body.get("height"),
         body.get("durationMin"), body.get("durationMax"),
         body.get("comments")),
    )
    conn.commit()
    return dict(cur.fetchone())


@router.get("/zones/{zone_id}")
def get_zone(zone_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_zones WHERE id = %s", (zone_id,))
    if not row:
        _404("Zone", zone_id)
    return row


@router.put("/publishers/{publisher_id}/zones/{zone_id}")
def update_zone(publisher_id: int, zone_id: int, body: dict = Body(...),
                conn=Depends(get_db)):
    existing = _one(conn, "SELECT * FROM aw_zones WHERE id = %s AND publisher_id = %s",
                    (zone_id, publisher_id))
    if not existing:
        _404("Zone", zone_id)
    _q(conn,
        """UPDATE aw_zones SET name=%s, description=%s, type=%s, format_id=%s,
           width=%s, height=%s, duration_min=%s, duration_max=%s, comments=%s
           WHERE id=%s""",
        (body["name"], body.get("description"), body["type"],
         body.get("formatId"), body.get("width"), body.get("height"),
         body.get("durationMin"), body.get("durationMax"),
         body.get("comments"), zone_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM aw_zones WHERE id = %s", (zone_id,)))


# ── Zone Groups ───────────────────────────────────────────────────────────────

@router.get("/zone-groups")
def list_zone_groups(limit: int = Query(100, ge=1, le=1000),
                     page: int = Query(1, ge=1),
                     conn=Depends(get_db)):
    return _paginate(conn, "SELECT * FROM aw_zone_groups", (), limit, page)


@router.post("/zone-groups")
def create_zone_group(body: dict = Body(...), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_zone_groups
           (name, description, total_capping, session_capping, comments, archived, created_at)
           VALUES (%s,%s,%s,%s,%s,false,NOW()) RETURNING *""",
        (body["name"], body.get("description"),
         body.get("totalCapping"), body.get("sessionCapping"),
         body.get("comments")),
    )
    conn.commit()
    return dict(cur.fetchone())


@router.get("/zone-groups/{zone_group_id}")
def get_zone_group(zone_group_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_zone_groups WHERE id = %s", (zone_group_id,))
    if not row:
        _404("Zone Group", zone_group_id)
    return row


@router.put("/zone-groups/{zone_group_id}")
def update_zone_group(zone_group_id: int, body: dict = Body(...), conn=Depends(get_db)):
    existing = _one(conn, "SELECT * FROM aw_zone_groups WHERE id = %s", (zone_group_id,))
    if not existing:
        _404("Zone Group", zone_group_id)
    _q(conn,
        """UPDATE aw_zone_groups SET name=%s, description=%s,
           total_capping=%s, session_capping=%s, comments=%s WHERE id=%s""",
        (body["name"], body.get("description"),
         body.get("totalCapping"), body.get("sessionCapping"),
         body.get("comments"), zone_group_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM aw_zone_groups WHERE id = %s", (zone_group_id,)))


@router.put("/zone-groups/{zone_group_id}/archive")
def archive_zone_group(zone_group_id: int, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM aw_zone_groups WHERE id = %s", (zone_group_id,))
    if not row:
        _404("Zone Group", zone_group_id)
    _q(conn, "UPDATE aw_zone_groups SET archived = true WHERE id = %s", (zone_group_id,))
    conn.commit()
    return None


@router.get("/zone-groups/{zone_group_id}/zones")
def list_zone_group_zones(zone_group_id: int, conn=Depends(get_db)):
    rows = _q(conn,
              """SELECT z.* FROM aw_zones z
                 JOIN aw_zone_group_zones zgz ON z.id = zgz.zone_id
                 WHERE zgz.zone_group_id = %s ORDER BY z.id""",
              (zone_group_id,)).fetchall()
    return [dict(r) for r in rows]


@router.post("/zone-groups/{zone_group_id}/zones")
def link_zones_to_group(zone_group_id: int, body: List[int] = Body(...),
                        conn=Depends(get_db)):
    cur = conn.cursor()
    for zone_id in body:
        cur.execute(
            """INSERT INTO aw_zone_group_zones (zone_group_id, zone_id)
               VALUES (%s, %s) ON CONFLICT DO NOTHING""",
            (zone_group_id, zone_id))
    conn.commit()
    return body


# ── Categories ────────────────────────────────────────────────────────────────

@router.get("/categories")
def list_categories(limit: int = Query(100, ge=1, le=1000),
                    page: int = Query(1, ge=1),
                    conn=Depends(get_db)):
    return _paginate(conn, "SELECT * FROM aw_categories WHERE parent_id IS NULL",
                     (), limit, page)


@router.get("/categories/subcategories")
def list_subcategories(limit: int = Query(100, ge=1, le=1000),
                       page: int = Query(1, ge=1),
                       conn=Depends(get_db)):
    rows = _paginate(conn,
                     """SELECT c.*, p.name AS parent_name
                        FROM aw_categories c
                        LEFT JOIN aw_categories p ON c.parent_id = p.id
                        WHERE c.parent_id IS NOT NULL""",
                     (), limit, page)
    return [{"id": r["id"], "name": r["name"], "description": r.get("description"),
             "parentId": r.get("parent_id"), "parentName": r.get("parent_name")} for r in rows]


# ── Creatives (upload placeholder) ───────────────────────────────────────────

@router.post("/creatives")
def upload_creative(conn=Depends(get_db)):
    """Placeholder — returns a mock creative identifier."""
    import uuid
    return {"creativeIdentifier": str(uuid.uuid4())}


# ── Targeting Zones (read-only) ──────────────────────────────────────────────

@router.get("/targeting-zones")
def list_targeting_zones(conn=Depends(get_db)):
    rows = _q(conn, "SELECT id, name, description FROM aw_zones ORDER BY id LIMIT 100").fetchall()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════
# New in v9
# ═══════════════════════════════════════════════════════════════════════════

# ── Creatives: multipart upload ──────────────────────────────────────────────

@router.post("/creatives/uploads/presignedurl")
def create_multipart_upload(body: dict = Body(...), conn=Depends(get_db)):
    file_name = body.get("fileName")
    size = body.get("sizeBytes")
    if not file_name:
        _400("fileName", "fileName is required")
    if not isinstance(size, int) or size <= 0:
        _400("sizeBytes", "sizeBytes must be a positive integer")
    ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    if ext not in UPLOAD_EXTENSIONS:
        _400("fileName", f"Unsupported file extension: {ext or '(none)'}")

    upload_id = str(uuid.uuid4())
    parts = max(1, math.ceil(size / MULTIPART_PART_SIZE))
    expires_at = (datetime.utcnow() + timedelta(hours=1)).isoformat() + "Z"
    _q(conn,
       """INSERT INTO aw_multipart_uploads
          (upload_id, file_name, size_bytes, parts, status, expires_at, created_at)
          VALUES (%s,%s,%s,%s,'INITIATED',%s,NOW())""",
       (upload_id, file_name, size, parts, expires_at))
    conn.commit()

    urls = []
    for n in range(1, parts + 1):
        start = (n - 1) * MULTIPART_PART_SIZE
        end = min(n * MULTIPART_PART_SIZE, size) - 1
        urls.append({
            "partNumber": n,
            "url": f"https://mock.adbridge.local/adswizz/uploads/{upload_id}/parts/{n}",
            "rangeStart": start,
            "rangeEnd": end,
        })
    return {"uploadId": upload_id, "parts": parts, "urls": urls, "expiresAt": expires_at}


@router.post("/creatives/uploads/complete")
def complete_multipart_upload(body: dict = Body(...), conn=Depends(get_db)):
    upload_id = body.get("uploadId")
    parts = body.get("parts")
    if not upload_id:
        _400("uploadId", "uploadId is required")
    if not isinstance(parts, list) or not parts:
        _400("parts", "parts is required")
    upload = _one(conn, "SELECT * FROM aw_multipart_uploads WHERE upload_id = %s", (upload_id,))
    if not upload or upload["status"] != "INITIATED":
        _400("uploadId", f"Unknown or already completed upload: {upload_id}")
    numbers = sorted(p.get("partNumber") for p in parts if p.get("etag"))
    if numbers != list(range(1, upload["parts"] + 1)):
        _400("parts", f"Expected an etag for each of parts 1..{upload['parts']}")

    creative_identifier = f"_ad_{uuid.uuid4()}"
    _q(conn,
       """UPDATE aw_multipart_uploads SET status = 'COMPLETED', creative_identifier = %s
          WHERE upload_id = %s""",
       (creative_identifier, upload_id))
    conn.commit()
    return {"creativeIdentifier": creative_identifier}


# ── Creatives: audiogram ─────────────────────────────────────────────────────

def _format_light_creative(row):
    return {"id": row["id"], "creativeName": row["creative_name"], "status": row["status"]}


def _get_audiogram(conn, creative_id):
    row = _one(conn, "SELECT * FROM aw_audiogram_creatives WHERE id = %s", (creative_id,))
    if not row:
        _404("Creative", creative_id)
    return row


@router.post("/creatives/{advertiser_id}/audiogram", status_code=201)
def create_audiogram(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    for field in ("creativeName", "audioCreativeIdentifier", "audioFileExtension",
                  "displayCreativeIdentifier", "displayFileExtension"):
        if not body.get(field):
            _400(field, f"{field} is required")
    if body["audioFileExtension"].lower() not in AUDIOGRAM_AUDIO_EXT:
        _400("audioFileExtension", f"Supported audio extensions: {sorted(AUDIOGRAM_AUDIO_EXT)}")
    if body["displayFileExtension"].lower() not in AUDIOGRAM_DISPLAY_EXT:
        _400("displayFileExtension",
             f"Supported display extensions: {sorted(AUDIOGRAM_DISPLAY_EXT)}")
    if not _one(conn, "SELECT id FROM aw_advertisers WHERE id = %s", (advertiser_id,)):
        _404("Advertiser", advertiser_id)

    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_audiogram_creatives
           (advertiser_id, creative_name, audio_creative_identifier, audio_file_extension,
            display_creative_identifier, display_file_extension, status, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,'PROCESSING',NOW()) RETURNING *""",
        (advertiser_id, body["creativeName"], body["audioCreativeIdentifier"],
         body["audioFileExtension"].lower(), body["displayCreativeIdentifier"],
         body["displayFileExtension"].lower()),
    )
    conn.commit()
    return _format_light_creative(dict(cur.fetchone()))


@router.get("/creatives/audiogram/{creative_id}")
def get_audiogram_status(creative_id: int, conn=Depends(get_db)):
    """Mock async processing: a PROCESSING creative is PUBLISHED on the first poll."""
    row = _get_audiogram(conn, creative_id)
    if row["status"] == "PROCESSING":
        _q(conn, "UPDATE aw_audiogram_creatives SET status = 'PUBLISHED' WHERE id = %s",
           (creative_id,))
        conn.commit()
        row["status"] = "PUBLISHED"
    return _format_light_creative(row)


@router.patch("/creatives/audiogram/{creative_id}")
def patch_audiogram(creative_id: int, body: dict = Body(...), conn=Depends(get_db)):
    if not body.get("creativeName"):
        _400("creativeName", "creativeName is required")
    _get_audiogram(conn, creative_id)
    _q(conn, "UPDATE aw_audiogram_creatives SET creative_name = %s WHERE id = %s",
       (body["creativeName"], creative_id))
    conn.commit()
    return _format_light_creative(_get_audiogram(conn, creative_id))


@router.delete("/creatives/audiogram/{creative_id}", status_code=204)
def delete_audiogram(creative_id: int, conn=Depends(get_db)):
    row = _get_audiogram(conn, creative_id)
    if row["status"] not in ("FAILED", "PUBLISHED"):
        _400("status", "Only FAILED or PUBLISHED audiogram creatives can be deleted",
             code="invalid.status")
    _q(conn, "DELETE FROM aw_audiogram_creatives WHERE id = %s", (creative_id,))
    conn.commit()
    return Response(status_code=204)
