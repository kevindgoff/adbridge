"""Basis DSP API mock endpoints under /basisnet.

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
from app.helpers import single_response

from datetime import datetime, timedelta


def _past_date(days_ago):
    return (datetime.utcnow() - timedelta(days=days_ago)).strftime("%Y%m%d")

router = APIRouter(prefix="/basisnet")


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


def _single_with_links(data):
    """Build a single-item response with links array."""
    data["links"] = []
    return data


# ══════════════════════════════ Auth ══════════════════════════════════════════

@router.post("/oauth/token")
def generate_token():
    """Generate a mock OAuth2 access token (Client Credentials grant)."""
    return {
        "scope": "STATS AUDIENCES CONTROL",
        "access_token": "mock-basisnet-token-7ebe55b54ee12a8ee07329f1cefd6de6",
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
    return _single_with_links(dict(row))


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
    return _list_response(results, total)


@router.get("/advertisers/{advertiser_id}/brands/{brand_id}")
def get_brand(advertiser_id: int, brand_id: int, conn=Depends(get_db)):
    """Retrieve details for one brand."""
    row = _q(conn,
             "SELECT * FROM bn_brands WHERE brand_id = %s AND advertiser_id = %s",
             (brand_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Brand cannot be found.")
    return _single_with_links(dict(row))


@router.post("/advertisers/{advertiser_id}/brands")
def create_brand(advertiser_id: int, body: dict, conn=Depends(get_db)):
    """Create a new brand (mock — returns synthetic data)."""
    name = body.get("name", "New Brand")
    notes = body.get("notes", "")
    # Get the next brand_id
    row = _q(conn, "SELECT COALESCE(MAX(brand_id), 0) + 1 as next_id FROM bn_brands").fetchone()
    next_id = row["next_id"]
    from app.database import _now
    _q(conn,
       "INSERT INTO bn_brands (brand_id, advertiser_id, name, notes, archived, created_at) "
       "VALUES (%s, %s, %s, %s, %s, %s)",
       (next_id, advertiser_id, name, notes, False, _now()))
    conn.commit()
    return _single_with_links({
        "brandId": next_id,
        "name": name,
        "notes": notes,
        "archived": False,
    })


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
    # Shape results to match Basis DSP response format
    for r in results:
        r["links"] = []
        r["budget"] = {
            "amount": r.pop("budget_amount", 0),
            "type": r.pop("budget_type", "none"),
            "evenDeliveryEnabled": bool(r.pop("even_delivery_enabled", True)),
            "schedule": {
                "flightDates": {
                    "from": r.pop("flight_start", None),
                    "to": r.pop("flight_end", None),
                }
            }
        }
    return _list_response(results, total)


@router.get("/advertisers/{advertiser_id}/brands/{brand_id}/campaignGroups/{group_id}")
def get_campaign_group(advertiser_id: int, brand_id: int, group_id: int,
                       conn=Depends(get_db)):
    """Retrieve a campaign group's details."""
    row = _q(conn,
             "SELECT * FROM bn_campaign_groups WHERE campaign_group_id = %s AND brand_id = %s",
             (group_id, brand_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign group cannot be found.")
    data = dict(row)
    data["links"] = []
    data["budget"] = {
        "amount": data.pop("budget_amount", 0),
        "type": data.pop("budget_type", "none"),
        "evenDeliveryEnabled": bool(data.pop("even_delivery_enabled", True)),
        "schedule": {
            "flightDates": {
                "from": data.pop("flight_start", None),
                "to": data.pop("flight_end", None),
            }
        }
    }
    return data


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
    for r in results:
        r["links"] = []
        r["budget"] = {
            "amount": r.pop("budget_amount", 0),
            "type": r.pop("budget_type", "none"),
            "evenDeliveryEnabled": bool(r.pop("even_delivery_enabled", True)),
            "schedule": {
                "flightDates": {
                    "from": r.pop("flight_start", None),
                    "to": r.pop("flight_end", None),
                }
            }
        }
    return _list_response(results, total)


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
    for r in results:
        r["links"] = []
        r["budget"] = {
            "amount": r.pop("budget_amount", 0),
            "type": r.pop("budget_type", "daily"),
            "evenDeliveryEnabled": bool(r.pop("even_delivery_enabled", True)),
            "impressionCap": r.pop("impression_cap", None),
            "impressionCapType": r.pop("impression_cap_type", "none"),
        }
    return _list_response(results, total)


@router.get("/advertisers/{advertiser_id}/campaigns/{campaign_id}")
def get_campaign(advertiser_id: int, campaign_id: int, conn=Depends(get_db)):
    """Retrieve detailed campaign settings."""
    row = _q(conn,
             "SELECT * FROM bn_campaigns WHERE campaign_id = %s AND advertiser_id = %s",
             (campaign_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign cannot be found.")
    data = dict(row)
    data["links"] = []
    data["budget"] = {
        "links": [],
        "amount": data.pop("budget_amount", 0),
        "type": data.pop("budget_type", "daily"),
        "evenDeliveryEnabled": bool(data.pop("even_delivery_enabled", True)),
        "impressionCap": data.pop("impression_cap", None),
        "impressionCapType": data.pop("impression_cap_type", "none"),
        "schedule": {
            "flightDates": {
                "from": data.pop("flight_start", None),
                "to": data.pop("flight_end", None),
            }
        }
    }
    return data


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
    for r in results:
        r["links"] = []
        r["budget"] = {
            "amount": r.pop("budget_amount", 0),
            "type": r.pop("budget_type", "daily"),
            "evenDeliveryEnabled": bool(r.pop("even_delivery_enabled", True)),
            "impressionCap": r.pop("impression_cap", None),
            "impressionCapType": r.pop("impression_cap_type", "none"),
        }
    return _list_response(results, total)


@router.patch("/advertisers/{advertiser_id}/campaigns/{campaign_id}")
def update_campaign(advertiser_id: int, campaign_id: int, body: dict,
                    conn=Depends(get_db)):
    """Update selected fields for a campaign."""
    row = _q(conn,
             "SELECT * FROM bn_campaigns WHERE campaign_id = %s AND advertiser_id = %s",
             (campaign_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign cannot be found.")

    allowed = {"name", "status", "defaultBid", "maxBid", "notes"}
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

    # Return updated campaign
    updated = _q(conn, "SELECT * FROM bn_campaigns WHERE campaign_id = %s",
                 (campaign_id,)).fetchone()
    data = dict(updated)
    data["links"] = []
    data["budget"] = {
        "amount": data.pop("budget_amount", 0),
        "type": data.pop("budget_type", "daily"),
        "evenDeliveryEnabled": bool(data.pop("even_delivery_enabled", True)),
        "impressionCap": data.pop("impression_cap", None),
        "impressionCapType": data.pop("impression_cap_type", "none"),
    }
    return data


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
    Supports by=DAY for daily breakdowns, date range filtering, pagination,
    and sorting.
    """
    # Verify advertiser exists
    adv_row = _q(conn, "SELECT * FROM bn_advertisers WHERE advertiser_id = %s",
                 (advertiser_id,)).fetchone()
    if not adv_row:
        raise HTTPException(404, "Advertiser cannot be found.")

    # Build stat query conditions
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

    if by and by.upper() == "DAY":
        # Return daily statistics grouped by campaign and date
        count_sql = (
            f"SELECT COUNT(*) FROM (SELECT DISTINCT s.campaign_id, s.date "
            f"FROM bn_campaign_stats s "
            f"JOIN bn_campaigns c ON s.campaign_id = c.campaign_id "
            f"{where}) sub"
        )
        total_count = _q(conn, count_sql, params).fetchone()["count"]

        order_col = sortBy or "date"
        direction = "ASC" if sortDirection.lower() == "asc" else "DESC"
        offset = (page - 1) * pageSize

        sql = (
            f"SELECT s.campaign_id, c.name as campaign_name, c.status, "
            f"c.default_bid, c.review_status, s.date, "
            f"s.impressions as impressionsWon, s.impressions as auctionsWon, "
            f"CAST(s.impressions * 1.3 AS INTEGER) as auctionsBid, "
            f"s.clicks, s.spend as auctionsSpend, s.spend as totalSpend, "
            f"0.0 as dataSpend, s.conversions as clickThruConversions, "
            f"0 as viewthruConversions, s.ctr as clickthruRate, "
            f"s.cpm as effectiveCPM, s.cpc as totalEffectiveCPC "
            f"FROM bn_campaign_stats s "
            f"JOIN bn_campaigns c ON s.campaign_id = c.campaign_id "
            f"{where} ORDER BY {order_col} {direction} LIMIT %s OFFSET %s"
        )
        rows = _q(conn, sql, params + [pageSize, offset]).fetchall()

        # Group by campaign for daily format
        campaigns_daily = {}
        for r in rows:
            cid = r["campaign_id"]
            if cid not in campaigns_daily:
                campaigns_daily[cid] = {
                    "links": [],
                    "entity": {
                        "links": [{"href": f"http://api.sitescout.com/advertisers/{advertiser_id}/campaigns/{cid}", "rel": "self"}],
                        "campaignId": cid,
                        "name": r["campaign_name"],
                        "status": r["status"],
                        "reviewStatus": r["review_status"],
                        "defaultBid": r["default_bid"],
                    },
                    "statsList": [],
                    "totals": _empty_stats(),
                }
            stats = _build_stats_obj(r)
            campaigns_daily[cid]["statsList"].append({
                "date": r["date"].replace("-", ""),
                "stats": stats,
            })
            _accumulate_totals(campaigns_daily[cid]["totals"], stats)

        results = list(campaigns_daily.values())
        # Compute dateRange
        from_date = dateFrom or _past_date(0).replace("-", "")
        to_date = dateTo or _past_date(0).replace("-", "")
    else:
        # Aggregate stats per campaign (no daily breakdown)
        count_sql = (
            f"SELECT COUNT(*) FROM (SELECT DISTINCT s.campaign_id "
            f"FROM bn_campaign_stats s "
            f"JOIN bn_campaigns c ON s.campaign_id = c.campaign_id "
            f"{where}) sub"
        )
        total_count = _q(conn, count_sql, params).fetchone()["count"]

        order_col = sortBy or "impressionsWon"
        direction = "ASC" if sortDirection.lower() == "asc" else "DESC"
        offset = (page - 1) * pageSize

        sql = (
            f"SELECT s.campaign_id, c.name as campaign_name, c.status, "
            f"c.default_bid, c.review_status, "
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
            f"{where} GROUP BY s.campaign_id, c.name, c.status, c.default_bid, c.review_status "
            f"ORDER BY {order_col} {direction} LIMIT %s OFFSET %s"
        )
        rows = _q(conn, sql, params + [pageSize, offset]).fetchall()

        results = []
        for r in rows:
            row = dict(r)
            imps = row["impressionsWon"] or 0
            clicks = row["clicks"] or 0
            spend = row["auctionsSpend"] or 0
            ctc = row["clickThruConversions"] or 0
            vtc = row["viewthruConversions"] or 0

            stats = {
                "auctionsBid": row["auctionsBid"],
                "auctionsWon": row["auctionsWon"],
                "impressionsWon": imps,
                "clicks": clicks,
                "clickThruConversions": ctc,
                "viewthruConversions": vtc,
                "totalConversions": ctc + vtc,
                "auctionsSpend": round(spend, 2),
                "dataSpend": 0.0,
                "totalSpend": round(spend, 2),
                "effectiveCPM": round((spend / imps) * 1000, 6) if imps else 0.0,
                "totalEffectiveCPM": round((spend / imps) * 1000, 6) if imps else 0.0,
                "totalEffectiveCPC": round(spend / clicks, 6) if clicks else 0.0,
                "clickthruRate": round(clicks / imps, 6) if imps else 0.0,
                "winRate": round(row["auctionsWon"] / row["auctionsBid"], 6) if row["auctionsBid"] else 0.0,
                "revenue": 0.0,
            }
            results.append({
                "entity": {
                    "links": [{"href": f"http://api.sitescout.com/advertisers/{advertiser_id}/campaigns/{row['campaign_id']}", "rel": "self"}],
                    "campaignId": row["campaign_id"],
                    "name": row["campaign_name"],
                    "status": row["status"],
                    "reviewStatus": row["review_status"],
                    "defaultBid": row["default_bid"],
                },
                "stats": stats,
                "links": [],
            })

        from_date = dateFrom or _past_date(6).replace("-", "")
        to_date = dateTo or _past_date(0).replace("-", "")

    # Build the full response envelope
    response = {
        "links": [
            {"href": f"http://api.sitescout.com/advertisers/{advertiser_id}/stats", "rel": "self"},
        ],
        "totalCount": total_count,
        "pagination": {
            "page": page,
            "pageSize": pageSize,
        },
        "sorting": {
            "sortBy": sortBy or ("date" if by and by.upper() == "DAY" else "impressionsWon"),
            "sortDirection": sortDirection,
        },
        "results": results,
        "dateRange": {
            "from": from_date.replace("-", ""),
            "to": to_date.replace("-", ""),
            "timezone": timezone,
        },
        "fromCache": useCache,
    }

    if filter:
        response["filter"] = filter
    if status:
        response["status"] = status

    # Compute totals across all results
    totals = _empty_stats()
    for r in results:
        s = r.get("stats") or r.get("totals", {})
        _accumulate_totals(totals, s)
    # Recompute derived fields on totals
    if totals["impressionsWon"]:
        totals["effectiveCPM"] = round((totals["auctionsSpend"] / totals["impressionsWon"]) * 1000, 6)
        totals["totalEffectiveCPM"] = totals["effectiveCPM"]
        totals["clickthruRate"] = round(totals["clicks"] / totals["impressionsWon"], 6)
    if totals["clicks"]:
        totals["totalEffectiveCPC"] = round(totals["totalSpend"] / totals["clicks"], 6)
    if totals["auctionsBid"]:
        totals["winRate"] = round(totals["auctionsWon"] / totals["auctionsBid"], 6)
    response["totals"] = totals

    return response


def _empty_stats():
    """Return a zeroed-out stats object."""
    return {
        "auctionsBid": 0,
        "auctionsWon": 0,
        "impressionsWon": 0,
        "clicks": 0,
        "clickThruConversions": 0,
        "viewthruConversions": 0,
        "totalConversions": 0,
        "auctionsSpend": 0.0,
        "dataSpend": 0.0,
        "totalSpend": 0.0,
        "effectiveCPM": 0.0,
        "totalEffectiveCPM": 0.0,
        "totalEffectiveCPC": 0.0,
        "clickthruRate": 0.0,
        "winRate": 0.0,
        "revenue": 0.0,
    }


def _build_stats_obj(row):
    """Build a stats object from a single row."""
    imps = row["impressionsWon"] or 0
    clicks = row["clicks"] or 0
    spend = row["auctionsSpend"] or 0
    ctc = row.get("clickThruConversions", 0) or 0
    vtc = row.get("viewthruConversions", 0) or 0
    return {
        "auctionsBid": row["auctionsBid"],
        "auctionsWon": row["auctionsWon"],
        "impressionsWon": imps,
        "clicks": clicks,
        "clickThruConversions": ctc,
        "viewthruConversions": vtc,
        "totalConversions": ctc + vtc,
        "auctionsSpend": round(spend, 2),
        "dataSpend": 0.0,
        "totalSpend": round(spend, 2),
        "effectiveCPM": round((spend / imps) * 1000, 6) if imps else 0.0,
        "totalEffectiveCPM": round((spend / imps) * 1000, 6) if imps else 0.0,
        "totalEffectiveCPC": round(spend / clicks, 6) if clicks else 0.0,
        "clickthruRate": round(clicks / imps, 6) if imps else 0.0,
        "winRate": round(row["auctionsWon"] / row["auctionsBid"], 6) if row["auctionsBid"] else 0.0,
        "revenue": 0.0,
    }


def _accumulate_totals(totals, stats):
    """Add stats values into a running totals dict."""
    for key in ("auctionsBid", "auctionsWon", "impressionsWon", "clicks",
                "clickThruConversions", "viewthruConversions", "totalConversions"):
        totals[key] += stats.get(key, 0) or 0
    for key in ("auctionsSpend", "dataSpend", "totalSpend", "revenue"):
        totals[key] += stats.get(key, 0.0) or 0.0


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
    for r in results:
        r["links"] = []
    return _list_response(results, total)


@router.get("/advertisers/{advertiser_id}/brands/{brand_id}/creatives/{creative_id}")
def get_creative(advertiser_id: int, brand_id: int, creative_id: int,
                 conn=Depends(get_db)):
    """Retrieve a single creative."""
    row = _q(conn,
             "SELECT * FROM bn_creatives WHERE creative_id = %s AND brand_id = %s",
             (creative_id, brand_id)).fetchone()
    if not row:
        raise HTTPException(404, "Creative cannot be found.")
    data = dict(row)
    data["links"] = []
    return data
