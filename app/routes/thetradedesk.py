"""The Trade Desk Platform API v3 mock endpoints under /ttd/v3.

The Trade Desk is a demand-side platform (DSP) for programmatic advertising.
Its REST API uses POST-based query endpoints with PageStartIndex/PageSize
pagination and PascalCase JSON property names.

Entity hierarchy: Partner → Advertiser → Campaign (+ CampaignFlight) → Ad Group → Creative
"""

from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Optional

from app.database import get_db

router = APIRouter(prefix="/ttd/v3")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _ttd_paginate(conn, sql, params, page_start_index=0, page_size=25):
    """TTD-style POST pagination: PageStartIndex + PageSize → Result list."""
    count_sql = f"SELECT COUNT(*) FROM ({sql}) _c"
    total = _q(conn, count_sql, params).fetchone()["count"]

    full_sql = f"{sql} LIMIT %s OFFSET %s"
    rows = _q(conn, full_sql, (*params, page_size, page_start_index)).fetchall()

    return {
        "Result": [dict(r) for r in rows],
        "ResultCount": len(rows),
        "TotalFilteredCount": total,
        "PageStartIndex": page_start_index,
        "PageSize": page_size,
    }


def _to_pascal(row: dict) -> dict:
    """Convert snake_case DB column names to PascalCase for TTD responses."""
    mapping = {
        "advertiser_id": "AdvertiserId",
        "partner_id": "PartnerId",
        "advertiser_name": "AdvertiserName",
        "description": "Description",
        "currency_code": "CurrencyCode",
        "domain_url": "DomainUrl",
        "category_id": "CategoryId",
        "industry_id": "IndustryId",
        "availability": "Availability",
        "created_at": "CreatedAtUtc",
        "updated_at": "UpdatedAtUtc",
        "campaign_id": "CampaignId",
        "campaign_name": "CampaignName",
        "budget_amount": "Budget",
        "daily_budget": "DailyBudget",
        "start_date": "StartDate",
        "end_date": "EndDate",
        "campaign_goal_type": "CampaignGoalType",
        "campaign_goal_value": "CampaignGoalValue",
        "pacing_mode": "PacingMode",
        "frequency_cap": "FrequencyCap",
        "frequency_period": "FrequencyPeriod",
        "status": "Availability",
        "flight_id": "CampaignFlightId",
        "flight_name": "FlightName",
        "budget_in_impressions": "BudgetInImpressions",
        "daily_target_in_impressions": "DailyTargetInImpressions",
        "budget_in_advertiser_currency": "BudgetInAdvertiserCurrency",
        "daily_target_in_advertiser_currency": "DailyTargetInAdvertiserCurrency",
        "ad_group_id": "AdGroupId",
        "ad_group_name": "AdGroupName",
        "bid_amount": "BidAmount",
        "bid_type": "BidType",
        "is_enabled": "IsEnabled",
        "creative_id": "CreativeId",
        "creative_name": "CreativeName",
        "creative_type": "CreativeType",
        "width": "Width",
        "height": "Height",
        "click_url": "ClickUrl",
        "landing_page_url": "LandingPageUrl",
        "ad_format": "AdFormat",
        "ad_server_id": "AdServerId",
        "approval_status": "ApprovalStatus",
        "tracking_tag_id": "TrackingTagId",
        "tag_name": "TagName",
        "tag_type": "TagType",
        "number_of_fires": "NumberOfFires",
        "server_side": "ServerSide",
    }
    out = {}
    for k, v in row.items():
        out[mapping.get(k, k)] = v
    return out


def _pascal_list(rows):
    return [_to_pascal(dict(r)) for r in rows]


# ── Authentication (mock) ────────────────────────────────────────────────────

@router.post("/authentication")
def authenticate(body: dict = Body(...)):
    """Mock TTD token endpoint. Returns a static token."""
    return {
        "Token": "mock-ttd-token-abc123xyz",
        "TokenExpirationUtc": "2099-12-31T23:59:59Z",
    }


# ── Advertiser ───────────────────────────────────────────────────────────────

@router.post("/advertiser/query/partner")
def query_advertisers_by_partner(body: dict = Body(...), conn=Depends(get_db)):
    partner_id = body.get("PartnerId")
    page_start = body.get("PageStartIndex", 0)
    page_size = body.get("PageSize", 25)

    sql = "SELECT * FROM ttd_advertisers"
    params = []
    if partner_id:
        sql += " WHERE partner_id = %s"
        params.append(partner_id)
    sql += " ORDER BY advertiser_id"

    result = _ttd_paginate(conn, sql, tuple(params), page_start, page_size)
    result["Result"] = _pascal_list(result["Result"])
    return result


@router.get("/advertiser/{advertiser_id}")
def get_advertiser(advertiser_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM ttd_advertisers WHERE advertiser_id = %s",
             (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    return _to_pascal(dict(row))


@router.post("/advertiser")
def create_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _uuid, _now
    adv_id = body.get("AdvertiserId", _uuid()[:8])
    now = _now()
    _q(conn,
        "INSERT INTO ttd_advertisers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (adv_id, body.get("PartnerId", "mock-partner-1"),
         body.get("AdvertiserName", "New Advertiser"),
         body.get("Description", ""),
         body.get("CurrencyCode", "USD"),
         body.get("DomainUrl", ""),
         body.get("CategoryId"), body.get("IndustryId"),
         "Available", now))
    conn.commit()
    row = _q(conn, "SELECT * FROM ttd_advertisers WHERE advertiser_id = %s",
             (adv_id,)).fetchone()
    return _to_pascal(dict(row))


@router.put("/advertiser")
def update_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    adv_id = body.get("AdvertiserId")
    if not adv_id:
        raise HTTPException(400, "AdvertiserId is required")
    row = _q(conn, "SELECT * FROM ttd_advertisers WHERE advertiser_id = %s",
             (adv_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    field_map = {
        "AdvertiserName": "advertiser_name",
        "Description": "description",
        "CurrencyCode": "currency_code",
        "DomainUrl": "domain_url",
    }
    updates = {}
    for pascal_key, db_col in field_map.items():
        if pascal_key in body:
            updates[db_col] = body[pascal_key]
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE ttd_advertisers SET {set_clause} WHERE advertiser_id = %s",
           (*updates.values(), adv_id))
        conn.commit()
    row = _q(conn, "SELECT * FROM ttd_advertisers WHERE advertiser_id = %s",
             (adv_id,)).fetchone()
    return _to_pascal(dict(row))


# ── Campaign ─────────────────────────────────────────────────────────────────

@router.post("/campaign/query/advertiser")
def query_campaigns_by_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    adv_id = body.get("AdvertiserId")
    page_start = body.get("PageStartIndex", 0)
    page_size = body.get("PageSize", 25)

    sql = "SELECT * FROM ttd_campaigns"
    params = []
    if adv_id:
        sql += " WHERE advertiser_id = %s"
        params.append(adv_id)
    sql += " ORDER BY campaign_id"

    result = _ttd_paginate(conn, sql, tuple(params), page_start, page_size)
    result["Result"] = _pascal_list(result["Result"])
    return result


@router.get("/campaign/{campaign_id}")
def get_campaign(campaign_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM ttd_campaigns WHERE campaign_id = %s",
             (campaign_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    return _to_pascal(dict(row))


@router.post("/campaign")
def create_campaign(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _uuid, _now
    cid = body.get("CampaignId", "ttd-c-" + _uuid()[:8])
    now = _now()
    _q(conn,
        "INSERT INTO ttd_campaigns VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (cid, body.get("AdvertiserId"),
         body.get("CampaignName", "New Campaign"),
         body.get("Budget", 0), body.get("DailyBudget", 0),
         body.get("StartDate"), body.get("EndDate"),
         body.get("CampaignGoalType", "CPC"),
         body.get("CampaignGoalValue", 1.0),
         body.get("PacingMode", "PaceEvenly"),
         body.get("FrequencyCap"), body.get("FrequencyPeriod"),
         now))
    conn.commit()
    row = _q(conn, "SELECT * FROM ttd_campaigns WHERE campaign_id = %s",
             (cid,)).fetchone()
    return _to_pascal(dict(row))


@router.put("/campaign")
def update_campaign(body: dict = Body(...), conn=Depends(get_db)):
    cid = body.get("CampaignId")
    if not cid:
        raise HTTPException(400, "CampaignId is required")
    row = _q(conn, "SELECT * FROM ttd_campaigns WHERE campaign_id = %s",
             (cid,)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    field_map = {
        "CampaignName": "campaign_name",
        "Budget": "budget_amount",
        "DailyBudget": "daily_budget",
        "StartDate": "start_date",
        "EndDate": "end_date",
        "CampaignGoalType": "campaign_goal_type",
        "CampaignGoalValue": "campaign_goal_value",
        "PacingMode": "pacing_mode",
    }
    updates = {}
    for pascal_key, db_col in field_map.items():
        if pascal_key in body:
            updates[db_col] = body[pascal_key]
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE ttd_campaigns SET {set_clause} WHERE campaign_id = %s",
           (*updates.values(), cid))
        conn.commit()
    row = _q(conn, "SELECT * FROM ttd_campaigns WHERE campaign_id = %s",
             (cid,)).fetchone()
    return _to_pascal(dict(row))


# ── Campaign Flight ──────────────────────────────────────────────────────────

@router.post("/campaignflight/query/campaign")
def query_flights_by_campaign(body: dict = Body(...), conn=Depends(get_db)):
    cid = body.get("CampaignId")
    page_start = body.get("PageStartIndex", 0)
    page_size = body.get("PageSize", 25)

    sql = "SELECT * FROM ttd_campaign_flights"
    params = []
    if cid:
        sql += " WHERE campaign_id = %s"
        params.append(cid)
    sql += " ORDER BY flight_id"

    result = _ttd_paginate(conn, sql, tuple(params), page_start, page_size)
    result["Result"] = _pascal_list(result["Result"])
    return result


@router.get("/campaignflight/{flight_id}")
def get_flight(flight_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM ttd_campaign_flights WHERE flight_id = %s",
             (flight_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign flight not found")
    return _to_pascal(dict(row))


# ── Ad Group ─────────────────────────────────────────────────────────────────

@router.post("/adgroup/query/campaign")
def query_adgroups_by_campaign(body: dict = Body(...), conn=Depends(get_db)):
    cid = body.get("CampaignId")
    page_start = body.get("PageStartIndex", 0)
    page_size = body.get("PageSize", 25)

    sql = "SELECT * FROM ttd_ad_groups"
    params = []
    if cid:
        sql += " WHERE campaign_id = %s"
        params.append(cid)
    sql += " ORDER BY ad_group_id"

    result = _ttd_paginate(conn, sql, tuple(params), page_start, page_size)
    result["Result"] = _pascal_list(result["Result"])
    return result


@router.post("/adgroup/query/advertiser")
def query_adgroups_by_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    adv_id = body.get("AdvertiserId")
    page_start = body.get("PageStartIndex", 0)
    page_size = body.get("PageSize", 25)

    sql = "SELECT ag.* FROM ttd_ad_groups ag JOIN ttd_campaigns c ON ag.campaign_id = c.campaign_id"
    params = []
    if adv_id:
        sql += " WHERE c.advertiser_id = %s"
        params.append(adv_id)
    sql += " ORDER BY ag.ad_group_id"

    result = _ttd_paginate(conn, sql, tuple(params), page_start, page_size)
    result["Result"] = _pascal_list(result["Result"])
    return result


@router.get("/adgroup/{ad_group_id}")
def get_ad_group(ad_group_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM ttd_ad_groups WHERE ad_group_id = %s",
             (ad_group_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Ad group not found")
    return _to_pascal(dict(row))


@router.post("/adgroup")
def create_ad_group(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _uuid, _now
    agid = body.get("AdGroupId", "ttd-ag-" + _uuid()[:8])
    now = _now()
    _q(conn,
        "INSERT INTO ttd_ad_groups VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (agid, body.get("CampaignId"),
         body.get("AdGroupName", "New Ad Group"),
         body.get("BidAmount", 5.0),
         body.get("BidType", "CPM"),
         body.get("IsEnabled", True),
         body.get("AdFormat", "Display"),
         body.get("StartDate"), body.get("EndDate"),
         body.get("FrequencyCap"), now))
    conn.commit()
    row = _q(conn, "SELECT * FROM ttd_ad_groups WHERE ad_group_id = %s",
             (agid,)).fetchone()
    return _to_pascal(dict(row))


@router.put("/adgroup")
def update_ad_group(body: dict = Body(...), conn=Depends(get_db)):
    agid = body.get("AdGroupId")
    if not agid:
        raise HTTPException(400, "AdGroupId is required")
    row = _q(conn, "SELECT * FROM ttd_ad_groups WHERE ad_group_id = %s",
             (agid,)).fetchone()
    if not row:
        raise HTTPException(404, "Ad group not found")
    field_map = {
        "AdGroupName": "ad_group_name",
        "BidAmount": "bid_amount",
        "BidType": "bid_type",
        "IsEnabled": "is_enabled",
        "AdFormat": "ad_format",
        "StartDate": "start_date",
        "EndDate": "end_date",
    }
    updates = {}
    for pascal_key, db_col in field_map.items():
        if pascal_key in body:
            updates[db_col] = body[pascal_key]
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE ttd_ad_groups SET {set_clause} WHERE ad_group_id = %s",
           (*updates.values(), agid))
        conn.commit()
    row = _q(conn, "SELECT * FROM ttd_ad_groups WHERE ad_group_id = %s",
             (agid,)).fetchone()
    return _to_pascal(dict(row))


# ── Creative ─────────────────────────────────────────────────────────────────

@router.post("/creative/query/advertiser")
def query_creatives_by_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    adv_id = body.get("AdvertiserId")
    page_start = body.get("PageStartIndex", 0)
    page_size = body.get("PageSize", 25)

    sql = "SELECT * FROM ttd_creatives"
    params = []
    if adv_id:
        sql += " WHERE advertiser_id = %s"
        params.append(adv_id)
    sql += " ORDER BY creative_id"

    result = _ttd_paginate(conn, sql, tuple(params), page_start, page_size)
    result["Result"] = _pascal_list(result["Result"])
    return result


@router.get("/creative/{creative_id}")
def get_creative(creative_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM ttd_creatives WHERE creative_id = %s",
             (creative_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Creative not found")
    return _to_pascal(dict(row))


@router.post("/creative")
def create_creative(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _uuid, _now
    crid = body.get("CreativeId", "ttd-cr-" + _uuid()[:8])
    now = _now()
    _q(conn,
        "INSERT INTO ttd_creatives VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (crid, body.get("AdvertiserId"),
         body.get("CreativeName", "New Creative"),
         body.get("CreativeType", "Banner"),
         body.get("Width", 300), body.get("Height", 250),
         body.get("ClickUrl", ""),
         body.get("LandingPageUrl", ""),
         body.get("AdServerId"),
         "Pending", now))
    conn.commit()
    row = _q(conn, "SELECT * FROM ttd_creatives WHERE creative_id = %s",
             (crid,)).fetchone()
    return _to_pascal(dict(row))


@router.put("/creative")
def update_creative(body: dict = Body(...), conn=Depends(get_db)):
    crid = body.get("CreativeId")
    if not crid:
        raise HTTPException(400, "CreativeId is required")
    row = _q(conn, "SELECT * FROM ttd_creatives WHERE creative_id = %s",
             (crid,)).fetchone()
    if not row:
        raise HTTPException(404, "Creative not found")
    field_map = {
        "CreativeName": "creative_name",
        "ClickUrl": "click_url",
        "LandingPageUrl": "landing_page_url",
        "Width": "width",
        "Height": "height",
    }
    updates = {}
    for pascal_key, db_col in field_map.items():
        if pascal_key in body:
            updates[db_col] = body[pascal_key]
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE ttd_creatives SET {set_clause} WHERE creative_id = %s",
           (*updates.values(), crid))
        conn.commit()
    row = _q(conn, "SELECT * FROM ttd_creatives WHERE creative_id = %s",
             (crid,)).fetchone()
    return _to_pascal(dict(row))


# ── Tracking Tags ────────────────────────────────────────────────────────────

@router.post("/trackingtag/query/advertiser")
def query_tracking_tags(body: dict = Body(...), conn=Depends(get_db)):
    adv_id = body.get("AdvertiserId")
    page_start = body.get("PageStartIndex", 0)
    page_size = body.get("PageSize", 25)

    sql = "SELECT * FROM ttd_tracking_tags"
    params = []
    if adv_id:
        sql += " WHERE advertiser_id = %s"
        params.append(adv_id)
    sql += " ORDER BY tracking_tag_id"

    result = _ttd_paginate(conn, sql, tuple(params), page_start, page_size)
    result["Result"] = _pascal_list(result["Result"])
    return result


@router.get("/trackingtag/{tracking_tag_id}")
def get_tracking_tag(tracking_tag_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM ttd_tracking_tags WHERE tracking_tag_id = %s",
             (tracking_tag_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Tracking tag not found")
    return _to_pascal(dict(row))
