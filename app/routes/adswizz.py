"""AdsWizz Domain API v8 mock endpoints under /adswizz/v8.

Mirrors: https://docs.adswizz.com/domain-api/v8/
Core resources: Agencies, Advertisers, Campaigns, Ads, Orders,
                Publishers, Zones, Zone Groups, Categories.

Pagination: page-based (limit + page, default limit=100, page=1).
Response envelope: bare JSON arrays for lists, plain objects for singles.
Auth header: agency (required), environment (optional).
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Optional, List

from app.database import get_db

router = APIRouter(prefix="/adswizz/v8")


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
    return _paginate(conn, sql, tuple(params), limit, page)


@router.post("/advertisers")
def create_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_advertisers (name, domain, contact, email, comments,
           external_reference, status, ad_clashing, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW()) RETURNING *""",
        (body["name"], body.get("domain"), body["contact"], body["email"],
         body.get("comments"), body.get("externalReference"),
         "ACTIVE", body.get("adClashing", False)),
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
    return _paginate(conn, sql, tuple(params), limit, page)


# ── Campaigns ─────────────────────────────────────────────────────────────────

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
    return {
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
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO aw_ads
           (campaign_id, name, type, subtype, status, included_in_objective,
            weight, comments, external_reference, tracking_type, tracking,
            creative_file_name, duration_ms, destination_url, archived, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false,NOW())
           RETURNING *""",
        (campaign_id, body["name"], body["type"], body.get("subtype", body["type"]),
         body.get("status", "ACTIVE"),
         body.get("includedInCampaignObjective", True),
         body.get("weight", 1), body.get("comments"),
         body.get("externalReference"), body.get("trackingType"),
         body.get("tracking"), body.get("creativeFileName"),
         body.get("durationMilliseconds"), body.get("destinationUrl")),
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
    existing = _one(conn, "SELECT * FROM aw_ads WHERE id = %s AND campaign_id = %s",
                    (ad_id, campaign_id))
    if not existing:
        _404("Ad", ad_id)
    _q(conn,
        """UPDATE aw_ads SET name=%s, status=%s, weight=%s, comments=%s,
           external_reference=%s, tracking_type=%s, tracking=%s,
           creative_file_name=%s, duration_ms=%s, destination_url=%s
           WHERE id=%s""",
        (body["name"], body.get("status", "ACTIVE"), body.get("weight", 1),
         body.get("comments"), body.get("externalReference"),
         body.get("trackingType"), body.get("tracking"),
         body.get("creativeFileName"), body.get("durationMilliseconds"),
         body.get("destinationUrl"), ad_id))
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
    return _paginate(conn, "SELECT * FROM aw_publishers", (), limit, page)


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
