"""Basis DSP API mock endpoints under /basisuil.

Mirrors the real Basis DSP API (https://api.sitescout.com/) hierarchy:
  /advertisers/{advertiserId}
  /advertisers/{advertiserId}/brands
  /advertisers/{advertiserId}/brands/{brandId}/campaignGroups
  /advertisers/{advertiserId}/brands/{brandId}/campaignGroups/{groupId}/campaigns

Uses page-based pagination (page/pageSize) and returns responses shaped like:
  {"links": [], "totalCount": N, "results": [...]}

Real API base URL: https://api.sitescout.com/
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.database import get_db

from datetime import datetime, timedelta


def _past_date(days_ago):
    return (datetime.utcnow() - timedelta(days=days_ago)).strftime("%Y%m%d")

router = APIRouter(prefix="/basisuil")


def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _paginated_query(conn, table, page=1, page_size=20, sort_by=None,
                     sort_direction="desc", where_clause="", where_params=(),
                     id_column="id"):
    """Page-based pagination matching the Basis DSP API style.

    Returns (results_list, total_count).
    """
    cur = conn.cursor()
    params = list(where_params)
    where = f"WHERE {where_clause}" if where_clause else ""

    # Count
    cur.execute(f"SELECT COUNT(*) FROM {table} {where}", params)
    total_count = cur.fetchone()["count"]

    # Sort
    order_col = sort_by or id_column
    direction = "ASC" if sort_direction.lower() == "asc" else "DESC"

    # Page offset
    offset = (page - 1) * page_size
    cur.execute(
        f"SELECT * FROM {table} {where} ORDER BY {order_col} {direction} LIMIT %s OFFSET %s",
        params + [page_size, offset]
    )
    rows = [dict(r) for r in cur.fetchall()]
    return rows, total_count


def _list_response(results, total_count):
    """Build the Basis DSP-style list response envelope."""
    return {
        "links": [],
        "totalCount": total_count,
        "results": results,
    }


# ═══════════════════════ Response Shaping Helpers ═════════════════════════════

def _shape_advertiser(row):
    """Shape a DB row into the documented Advertiser response."""
    return {
        "links": [{"href": f"http://api.sitescout.com/advertisers/{row['advertiser_id']}", "rel": "self"}],
        "advertiserId": row["advertiser_id"],
        "companyName": row["company_name"],
        "currencyCode": row["currency_code"],
        "email": row["email"],
        "notes": row["notes"] or "",
        "status": row["status"],
        "active": bool(row["active"]),
        "advertiserProperty": {
            "maxBudgetAmount": row["max_budget_amount"],
            "minCampaignBudgetAmount": row["min_campaign_budget_amount"],
            "activeCampaignLimit": row["active_campaign_limit"],
        },
    }


def _shape_brand(row):
    """Shape a DB row into the documented Brand response."""
    return {
        "links": [{"href": "...", "rel": "self"}],
        "brandId": row["brand_id"],
        "name": row["name"],
        "notes": row["notes"] or "",
        "archived": bool(row["archived"]),
    }


def _shape_campaign_group(row):
    """Shape a DB row into the documented Campaign Group response."""
    return {
        "links": [{"href": "...", "rel": "self"}],
        "campaignGroupId": row["campaign_group_id"],
        "name": row["name"],
        "status": row["status"],
        "kpiType": row.get("kpi_type"),
        "kpiValue": row.get("kpi_value"),
        "budget": {
            "links": [],
            "amount": row.get("budget_amount", 0),
            "type": row.get("budget_type", "none"),
            "evenDeliveryEnabled": bool(row.get("even_delivery_enabled", True)),
            "schedule": {
                "flightDates": {
                    "from": row.get("flight_start"),
                    "to": row.get("flight_end"),
                }
            },
        },
        "pacingSetting": row.get("pacing_setting", "CAMPAIGN"),
        "brandId": row.get("brand_id"),
        "advertiserSpendType": row.get("advertiser_spend_type"),
        "advertiserSpendRate": row.get("advertiser_spend_rate", 0),
    }


def _shape_campaign(row, include_schedule=False):
    """Shape a DB row into the documented Campaign response."""
    budget = {
        "links": [],
        "amount": row.get("budget_amount", 0),
        "type": row.get("budget_type", "daily"),
        "evenDeliveryEnabled": bool(row.get("even_delivery_enabled", True)),
        "impressionCap": row.get("impression_cap"),
        "impressionCapType": row.get("impression_cap_type", "none"),
    }
    if include_schedule:
        budget["schedule"] = {
            "flightDates": {
                "from": row.get("flight_start"),
                "to": row.get("flight_end"),
            }
        }

    result = {
        "links": [{"href": "...", "rel": "self"}],
        "campaignId": row["campaign_id"],
        "name": row["name"],
        "campaignGroupId": row.get("campaign_group_id"),
        "campaignGroupName": row.get("campaign_group_name"),
        "status": row["status"],
        "defaultBid": row.get("default_bid"),
        "maxBid": row.get("max_bid"),
        "notes": row.get("notes", ""),
        "budget": budget,
        "created": row.get("created"),
        "reviewStatus": row.get("review_status", "eligible"),
        "campaignType": row.get("campaign_type", "advanced"),
        "enabledROP": bool(row.get("enabled_rop", True)),
        "enableCrossDevice": bool(row.get("enable_cross_device", False)),
        "isRelativeDayParting": False,
        "pacingSetting": "CAMPAIGN",
        "excludeAnonymousDomains": True,
    }
    if include_schedule:
        result["flightDates"] = {
            "from": row.get("flight_start"),
            "to": row.get("flight_end"),
        }
    return result


def _shape_creative(row):
    """Shape a DB row into the documented Creative response."""
    ctype = row.get("creative_type", "banner")
    width = row.get("width") or 0
    height = row.get("height") or 0
    # Determine orientation
    if width > height:
        orientation = "landscape"
    elif height > width:
        orientation = "portrait"
    else:
        orientation = "square"
    # Determine format from type
    format_map = {"display": "image", "video": "video", "native": "native", "audio": "audio"}
    fmt = format_map.get(ctype, "image")

    return {
        "links": [{"href": "...", "rel": "self"}],
        "creativeId": row["creative_id"],
        "label": row["name"],
        "width": width,
        "height": height,
        "type": "banner" if ctype == "display" else ctype,
        "reviewStatus": "eligible",
        "previewUrl": f"http://preview.sitescout.ad/preview?ad={row['creative_id']}&adOnly=1",
        "sslEnabled": True,
        "assetUrl": f"https://cdn01.basis.net/mock/{row['creative_id']}.{'mp4' if ctype == 'video' else 'png'}",
        "format": fmt,
        "status": row.get("status", "online"),
        "clickUrl": row.get("landing_page_url", ""),
        "landingPageUrl": row.get("landing_page_url", ""),
        "landingPageDomain": (row.get("landing_page_url") or "").replace("https://", "").replace("http://", "").split("/")[0],
        "lastModified": row.get("created_at", "").replace("-", "").replace("T", " ").replace("Z", "")[:17],
        "created": row.get("created_at", "").replace("-", "").replace("T", " ").replace("Z", "")[:17],
        "brandId": row.get("brand_id"),
        "orientation": orientation,
        "enableClickUrlAuctionIdDecoration": True,
        "linkedCampaignCount": 0,
    }


# ══════════════════════════════ Auth ══════════════════════════════════════════

@router.post("/oauth/token")
def generate_token():
    """Generate a mock OAuth2 access token (Client Credentials grant)."""
    return {
        "scope": "STATS AUDIENCES CONTROL",
        "access_token": "mock-basisuil-token-7ebe55b54ee12a8ee07329f1cefd6de6",
        "token_type": "bearer",
        "expires_in": 3600,
    }


# ══════════════════════════════ Advertisers ═══════════════════════════════════

@router.get("/advertisers/{advertiser_id}")
def get_advertiser(advertiser_id: int, conn=Depends(get_db)):
    """Retrieve account details for an advertiser."""
    row = _q(conn, "SELECT * FROM bn_advertisers WHERE advertiser_id = %s",
             (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser cannot be found.")
    return _shape_advertiser(row)


@router.get("/advertisers/{advertiser_id}/balance")
def get_advertiser_balance(advertiser_id: int, conn=Depends(get_db)):
    """Retrieve the account balance."""
    row = _q(conn, "SELECT balance FROM bn_advertisers WHERE advertiser_id = %s",
             (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser cannot be found.")
    return row["balance"]


@router.get("/advertisers/{advertiser_id}/activeCampaigns")
def get_active_campaign_count(advertiser_id: int, conn=Depends(get_db)):
    """Retrieve the number of active campaigns."""
    row = _q(conn,
             "SELECT COUNT(*) FROM bn_campaigns c "
             "JOIN bn_campaign_groups g ON c.campaign_group_id = g.campaign_group_id "
             "JOIN bn_brands b ON g.brand_id = b.brand_id "
             "WHERE b.advertiser_id = %s AND c.status = %s",
             (advertiser_id, "online")).fetchone()
    return row["count"]


# ══════════════════════════════ Brands ════════════════════════════════════════

@router.get("/advertisers/{advertiser_id}/brands")
def list_brands(advertiser_id: int,
                page: int = Query(1, ge=1),
                pageSize: int = Query(20, ge=1, le=1000),
                sortBy: Optional[str] = None,
                sortDirection: str = Query("desc"),
                brandIds: Optional[str] = None,
                conn=Depends(get_db)):
    """Retrieve a paginated list of all non-archived brands."""
    conditions = ["advertiser_id = %s"]
    params = [advertiser_id]

    if brandIds:
        ids = [int(x.strip()) for x in brandIds.split(",")]
        placeholders = ",".join(["%s"] * len(ids))
        conditions.append(f"brand_id IN ({placeholders})")
        params.extend(ids)

    where = " AND ".join(conditions)
    results, total = _paginated_query(
        conn, "bn_brands", page=page, page_size=pageSize,
        sort_by=sortBy, sort_direction=sortDirection,
        where_clause=where, where_params=tuple(params),
        id_column="brand_id"
    )
    return _list_response([_shape_brand(r) for r in results], total)


@router.get("/advertisers/{advertiser_id}/brands/{brand_id}")
def get_brand(advertiser_id: int, brand_id: int, conn=Depends(get_db)):
    """Retrieve details for one brand."""
    row = _q(conn,
             "SELECT * FROM bn_brands WHERE brand_id = %s AND advertiser_id = %s",
             (brand_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Brand cannot be found.")
    return _shape_brand(row)


@router.post("/advertisers/{advertiser_id}/brands")
def create_brand(advertiser_id: int, body: dict, conn=Depends(get_db)):
    """Create a new brand (mock — returns synthetic data)."""
    name = body.get("name", "New Brand")
    notes = body.get("notes", "")
    row = _q(conn, "SELECT COALESCE(MAX(brand_id), 0) + 1 as next_id FROM bn_brands").fetchone()
    next_id = row["next_id"]
    from app.database import _now
    _q(conn,
       "INSERT INTO bn_brands (brand_id, advertiser_id, name, notes, archived, created_at) "
       "VALUES (%s, %s, %s, %s, %s, %s)",
       (next_id, advertiser_id, name, notes, False, _now()))
    conn.commit()
    return {
        "links": [{"href": "...", "rel": "self"}],
        "brandId": next_id,
        "name": name,
        "notes": notes,
        "archived": False,
    }


# ══════════════════════════════ Campaign Groups ═══════════════════════════════

@router.get("/advertisers/{advertiser_id}/brands/{brand_id}/campaignGroups")
def list_campaign_groups(advertiser_id: int, brand_id: int,
                         page: int = Query(1, ge=1),
                         pageSize: int = Query(20, ge=1, le=1000),
                         sortBy: Optional[str] = None,
                         sortDirection: str = Query("desc"),
                         campaignGroupIds: Optional[str] = None,
                         conn=Depends(get_db)):
    """Retrieve a list of all campaign groups for a brand."""
    conditions = ["brand_id = %s"]
    params = [brand_id]

    if campaignGroupIds:
        ids = [int(x.strip()) for x in campaignGroupIds.split(",")]
        placeholders = ",".join(["%s"] * len(ids))
        conditions.append(f"campaign_group_id IN ({placeholders})")
        params.extend(ids)

    where = " AND ".join(conditions)
    results, total = _paginated_query(
        conn, "bn_campaign_groups", page=page, page_size=pageSize,
        sort_by=sortBy, sort_direction=sortDirection,
        where_clause=where, where_params=tuple(params),
        id_column="campaign_group_id"
    )
    return _list_response([_shape_campaign_group(r) for r in results], total)


@router.get("/advertisers/{advertiser_id}/brands/{brand_id}/campaignGroups/{group_id}")
def get_campaign_group(advertiser_id: int, brand_id: int, group_id: int,
                       conn=Depends(get_db)):
    """Retrieve a campaign group's details."""
    row = _q(conn,
             "SELECT * FROM bn_campaign_groups WHERE campaign_group_id = %s AND brand_id = %s",
             (group_id, brand_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign group cannot be found.")
    return _shape_campaign_group(row)


@router.get("/advertisers/{advertiser_id}/campaignGroups")
def list_all_campaign_groups(advertiser_id: int,
                             page: int = Query(1, ge=1),
                             pageSize: int = Query(20, ge=1, le=1000),
                             sortBy: Optional[str] = None,
                             sortDirection: str = Query("desc"),
                             conn=Depends(get_db)):
    """Retrieve all campaign groups for an advertiser."""
    where = "advertiser_id = %s"
    params = (advertiser_id,)
    results, total = _paginated_query(
        conn, "bn_campaign_groups", page=page, page_size=pageSize,
        sort_by=sortBy, sort_direction=sortDirection,
        where_clause=where, where_params=params,
        id_column="campaign_group_id"
    )
    return _list_response([_shape_campaign_group(r) for r in results], total)


# ══════════════════════════════ Campaigns ═════════════════════════════════════

@router.get("/advertisers/{advertiser_id}/campaigns")
def list_campaigns(advertiser_id: int,
                   page: int = Query(1, ge=1),
                   pageSize: int = Query(20, ge=1, le=1000),
                   sortBy: Optional[str] = None,
                   sortDirection: str = Query("desc"),
                   statuses: Optional[str] = None,
                   campaignIds: Optional[str] = None,
                   conn=Depends(get_db)):
    """Retrieve a list of campaigns for an advertiser."""
    conditions = ["advertiser_id = %s"]
    params = [advertiser_id]

    if statuses:
        status_list = [s.strip() for s in statuses.split(",")]
        placeholders = ",".join(["%s"] * len(status_list))
        conditions.append(f"status IN ({placeholders})")
        params.extend(status_list)

    if campaignIds:
        ids = [int(x.strip()) for x in campaignIds.split(",")]
        placeholders = ",".join(["%s"] * len(ids))
        conditions.append(f"campaign_id IN ({placeholders})")
        params.extend(ids)

    where = " AND ".join(conditions)
    results, total = _paginated_query(
        conn, "bn_campaigns", page=page, page_size=pageSize,
        sort_by=sortBy, sort_direction=sortDirection,
        where_clause=where, where_params=tuple(params),
        id_column="campaign_id"
    )
    return _list_response([_shape_campaign(r) for r in results], total)


@router.get("/advertisers/{advertiser_id}/campaigns/{campaign_id}")
def get_campaign(advertiser_id: int, campaign_id: int, conn=Depends(get_db)):
    """Retrieve detailed campaign settings."""
    row = _q(conn,
             "SELECT * FROM bn_campaigns WHERE campaign_id = %s AND advertiser_id = %s",
             (campaign_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign cannot be found.")
    return _shape_campaign(row, include_schedule=True)


@router.get("/advertisers/{advertiser_id}/brands/{brand_id}/campaignGroups/{group_id}/campaigns")
def list_campaigns_in_group(advertiser_id: int, brand_id: int, group_id: int,
                            page: int = Query(1, ge=1),
                            pageSize: int = Query(20, ge=1, le=1000),
                            sortBy: Optional[str] = None,
                            sortDirection: str = Query("desc"),
                            statuses: Optional[str] = None,
                            conn=Depends(get_db)):
    """Retrieve campaigns within a specific campaign group."""
    conditions = ["campaign_group_id = %s"]
    params = [group_id]

    if statuses:
        status_list = [s.strip() for s in statuses.split(",")]
        placeholders = ",".join(["%s"] * len(status_list))
        conditions.append(f"status IN ({placeholders})")
        params.extend(status_list)

    where = " AND ".join(conditions)
    results, total = _paginated_query(
        conn, "bn_campaigns", page=page, page_size=pageSize,
        sort_by=sortBy, sort_direction=sortDirection,
        where_clause=where, where_params=tuple(params),
        id_column="campaign_id"
    )
    return _list_response([_shape_campaign(r) for r in results], total)


@router.patch("/advertisers/{advertiser_id}/campaigns/{campaign_id}")
def update_campaign(advertiser_id: int, campaign_id: int, body: dict,
                    conn=Depends(get_db)):
    """Update selected fields for a campaign."""
    row = _q(conn,
             "SELECT * FROM bn_campaigns WHERE campaign_id = %s AND advertiser_id = %s",
             (campaign_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign cannot be found.")

    updates = []
    params = []
    field_map = {"name": "name", "status": "status", "defaultBid": "default_bid",
                 "maxBid": "max_bid", "notes": "notes"}
    for key, col in field_map.items():
        if key in body:
            updates.append(f"{col} = %s")
            params.append(body[key])
    if updates:
        params.append(campaign_id)
        _q(conn, f"UPDATE bn_campaigns SET {', '.join(updates)} WHERE campaign_id = %s",
           tuple(params))
        conn.commit()

    updated = _q(conn, "SELECT * FROM bn_campaigns WHERE campaign_id = %s",
                 (campaign_id,)).fetchone()
    return _shape_campaign(updated)


# ══════════════════════════════ Statistics ════════════════════════════════════

@router.get("/advertisers/{advertiser_id}/stats")
def get_advertiser_stats(advertiser_id: int,
                         by: Optional[str] = None,
                         dateFrom: Optional[str] = None,
                         dateTo: Optional[str] = None,
                         timezone: Optional[str] = Query("EST"),
                         page: int = Query(1, ge=1),
                         pageSize: int = Query(20, ge=1, le=1000),
                         sortBy: Optional[str] = None,
                         sortDirection: str = Query("desc"),
                         status: Optional[str] = None,
                         campaignIds: Optional[str] = None,
                         filter: Optional[str] = None,
                         useCache: bool = False,
                         conn=Depends(get_db)):
    """Retrieve statistics for all campaigns under an advertiser.

    Mirrors GET /advertisers/{advertiserId}/stats from the Basis DSP API.
    Returns the advertiser entity with aggregated stats across all campaigns.
    """
    adv_row = _q(conn, "SELECT * FROM bn_advertisers WHERE advertiser_id = %s",
                 (advertiser_id,)).fetchone()
    if not adv_row:
        raise HTTPException(404, "Advertiser cannot be found.")

    conditions = ["s.advertiser_id = %s"]
    params = [advertiser_id]

    if dateFrom:
        conditions.append("s.date >= %s")
        params.append(dateFrom)
    if dateTo:
        conditions.append("s.date <= %s")
        params.append(dateTo)
    if status:
        conditions.append("c.status = %s")
        params.append(status)
    if campaignIds:
        ids = [int(x.strip()) for x in campaignIds.split(",")]
        placeholders = ",".join(["%s"] * len(ids))
        conditions.append(f"s.campaign_id IN ({placeholders})")
        params.extend(ids)

    where = "WHERE " + " AND ".join(conditions)

    sql = (
        f"SELECT "
        f"SUM(s.impressions) as impressionsWon, "
        f"SUM(s.impressions) as auctionsWon, "
        f"CAST(SUM(s.impressions) * 1.3 AS INTEGER) as auctionsBid, "
        f"SUM(s.clicks) as clicks, "
        f"SUM(s.spend) as auctionsSpend, "
        f"SUM(s.spend) as totalSpend, "
        f"0.0 as dataSpend, "
        f"SUM(s.conversions) as clickThruConversions, "
        f"0 as viewthruConversions "
        f"FROM bn_campaign_stats s "
        f"JOIN bn_campaigns c ON s.campaign_id = c.campaign_id "
        f"{where}"
    )
    row = _q(conn, sql, params).fetchone()

    imps = row["impressionsWon"] or 0
    clicks = row["clicks"] or 0
    spend = row["auctionsSpend"] or 0.0
    auctions_bid = row["auctionsBid"] or 0
    auctions_won = row["auctionsWon"] or 0
    ctc = row["clickThruConversions"] or 0
    vtc = row["viewthruConversions"] or 0
    total_conversions = ctc + vtc
    gross_total_spend = round(spend, 2)

    # Derived metrics
    win_rate = round(auctions_won / auctions_bid, 6) if auctions_bid else 0.0
    ctr = round(clicks / imps, 6) if imps else 0.0
    ecpm = round((spend / imps) * 1000, 6) if imps else 0.0
    ecpc = round(spend / clicks, 6) if clicks else 0.0
    ecpa = round(spend / total_conversions, 6) if total_conversions else 0.0
    click_ecpa = round(spend / ctc, 6) if ctc else 0.0
    view_ecpa = round(spend / vtc, 6) if vtc else 0.0
    video_started = int(imps * 0.15)
    video_completed = int(video_started * 0.72)
    vcr = round(video_completed / video_started, 6) if video_started else 0.0
    ecpcv = round(spend / video_completed, 6) if video_completed else 0.0
    eligible_imps = int(imps * 0.85)
    measured_imps = int(eligible_imps * 0.90)
    viewable_imps = int(measured_imps * 0.65)
    measured_rate = round(measured_imps / eligible_imps, 6) if eligible_imps else 0.0
    viewable_rate = round(viewable_imps / measured_imps, 6) if measured_imps else 0.0
    viewable_cpm = round((spend / viewable_imps) * 1000, 6) if viewable_imps else 0.0

    return {
        "entity": {
            "links": [
                {"href": f"http://api.sitescout.com/advertisers/{advertiser_id}", "rel": "self"}
            ],
            "advertiserId": advertiser_id,
            "companyName": adv_row["company_name"],
            "email": adv_row["email"],
            "status": adv_row["status"],
            "active": bool(adv_row["active"]),
        },
        "stats": {
            "auctionsBid": auctions_bid,
            "auctionsWon": auctions_won,
            "impressionsWon": imps,
            "clicks": clicks,
            "offerClicks": 0,
            "clickThruConversions": ctc,
            "viewthruConversions": vtc,
            "totalConversions": total_conversions,
            "videoStarted": video_started,
            "videoFirstQuartileReached": int(video_started * 0.92),
            "videoMidpointReached": int(video_started * 0.84),
            "videoThirdQuartileReached": int(video_started * 0.78),
            "videoCompleted": video_completed,
            "videoSkipped": int(video_started * 0.08),
            "eligibleImpressions": eligible_imps,
            "measuredImpressions": measured_imps,
            "viewableImpressions": viewable_imps,
            "companionImpressions": 0,
            "companionClicks": 0,
            "companionConversions": 0,
            "auctionsSpend": round(spend, 2),
            "dataSpend": 0.0,
            "totalSpend": round(spend, 2),
            "grossTotalSpend": gross_total_spend,
            "nonBillableSpend": 0.0,
            "revenue": 0.0,
            "ctcRevenue": 0.0,
            "vtcRevenue": 0.0,
            "advertiserSpend": 0.0,
            "winRate": win_rate,
            "clickthruRate": ctr,
            "offerClickthruRate": 0.0,
            "clickCVR": round(ctc / imps, 6) if imps else 0.0,
            "viewCVR": round(vtc / imps, 6) if imps else 0.0,
            "videoCompletionRate": vcr,
            "videoSkippedRate": round(0.08, 6),
            "measuredRate": measured_rate,
            "viewableRate": viewable_rate,
            "effectiveCPM": ecpm,
            "mediaEffectiveCPM": ecpm,
            "totalEffectiveCPM": ecpm,
            "totalEffectiveCPC": ecpc,
            "totalEffectiveCPA": ecpa,
            "totalEffectiveCPCV": ecpcv,
            "clickEffectiveCPA": click_ecpa,
            "viewEffectiveCPA": view_ecpa,
            "dataEffectiveCPM": 0.0,
            "viewableCPM": viewable_cpm,
            "grossTotalEffectiveCPC": ecpc,
            "grossTotalEffectiveCPA": ecpa,
            "grossTotalEffectiveCPCV": ecpcv,
            "grossTotalEffectiveCPM": ecpm,
            "revenuePerMille": 0.0,
            "returnOnAdSpend": 0.0,
            "totalCVRM": round(total_conversions / (imps / 1000), 6) if imps else 0.0,
            "marginOnAdvertiserSpend": 0.0,
            "effectiveCPAOnAdvertiserSpend": 0.0,
            "effectiveCPMOnAdvertiserSpend": 0.0,
            "effectiveCPCOnAdvertiserSpend": 0.0,
            "effectiveCPCVOnAdvertiserSpend": 0.0,
        },
    }


# ══════════════════════════════ Creatives ═════════════════════════════════════

@router.get("/advertisers/{advertiser_id}/brands/{brand_id}/creatives")
def list_creatives(advertiser_id: int, brand_id: int,
                   page: int = Query(1, ge=1),
                   pageSize: int = Query(20, ge=1, le=1000),
                   sortBy: Optional[str] = None,
                   sortDirection: str = Query("desc"),
                   conn=Depends(get_db)):
    """Retrieve creatives for a brand."""
    results, total = _paginated_query(
        conn, "bn_creatives", page=page, page_size=pageSize,
        sort_by=sortBy, sort_direction=sortDirection,
        where_clause="brand_id = %s", where_params=(brand_id,),
        id_column="creative_id"
    )
    return _list_response([_shape_creative(r) for r in results], total)


@router.get("/advertisers/{advertiser_id}/brands/{brand_id}/creatives/{creative_id}")
def get_creative(advertiser_id: int, brand_id: int, creative_id: int,
                 conn=Depends(get_db)):
    """Retrieve a single creative."""
    row = _q(conn,
             "SELECT * FROM bn_creatives WHERE creative_id = %s AND brand_id = %s",
             (creative_id, brand_id)).fetchone()
    if not row:
        raise HTTPException(404, "Creative cannot be found.")
    return _shape_creative(row)
