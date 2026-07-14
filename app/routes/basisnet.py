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

@router.get("/advertisers/{advertiser_id}/stats/campaigns")
def get_campaign_stats(advertiser_id: int,
                       campaignIds: Optional[str] = None,
                       conn=Depends(get_db)):
    """Retrieve campaign-level statistics."""
    conditions = ["s.advertiser_id = %s"]
    params = [advertiser_id]

    if campaignIds:
        ids = [int(x.strip()) for x in campaignIds.split(",")]
        placeholders = ",".join(["%s"] * len(ids))
        conditions.append(f"s.campaign_id IN ({placeholders})")
        params.extend(ids)

    where = "WHERE " + " AND ".join(conditions)
    rows = _q(conn,
              f"SELECT * FROM bn_campaign_stats s {where}",
              tuple(params)).fetchall()
    return [dict(r) for r in rows]


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
