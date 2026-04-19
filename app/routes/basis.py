"""All Basis API mock endpoints under /basis/v1."""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.database import get_db
from app.helpers import paginate, list_response, single_response

router = APIRouter(prefix="/basis/v1")


def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


# ──────────────────────────── Auth (placeholder) ────────────────────────────

@router.post("/oauth/token")
def generate_token():
    return {
        "access_token": "mock-access-token-abc123",
        "token_type": "Bearer",
        "expires_in": 3600,
    }


# ──────────────────────────── Me / Agency ────────────────────────────────────

@router.get("/me")
def get_me(conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM users LIMIT 1").fetchone()
    if not row:
        raise HTTPException(404, "No user found")
    return single_response(dict(row))


@router.get("/agency")
def get_agency(conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM agencies LIMIT 1").fetchone()
    if not row:
        raise HTTPException(404, "No agency found")
    return single_response(dict(row))


# ──────────────────────────── Clients ────────────────────────────────────────

@router.get("/clients")
def list_clients(cursor: Optional[str] = None, query: Optional[str] = None,
                 conn=Depends(get_db)):
    data, meta = paginate(conn, "clients", cursor, query=query,
                          query_columns=["name", "contact_name", "billing_name"])
    return list_response(data, meta)


@router.get("/clients/{client_id}")
def get_client(client_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM clients WHERE id = %s", (client_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Client not found")
    return single_response(dict(row))


# ──────────────────────────── Brands ─────────────────────────────────────────

@router.get("/brands")
def list_brands(cursor: Optional[str] = None, query: Optional[str] = None,
                client_id: Optional[str] = None, conn=Depends(get_db)):
    where, params = "", ()
    if client_id:
        where, params = "client_id = %s", (client_id,)
    data, meta = paginate(conn, "brands", cursor, query=query,
                          query_columns=["name"], where_clause=where, where_params=params)
    # Attach verticals
    for brand in data:
        verts = _q(conn,
            "SELECT v.id, v.name, bv.subvertical FROM brand_verticals bv "
            "JOIN verticals v ON v.id = bv.vertical_id WHERE bv.brand_id = %s",
            (brand["id"],)
        ).fetchall()
        brand["verticals"] = [dict(v) for v in verts]
    return list_response(data, meta)


@router.get("/brands/{brand_id}")
def get_brand(brand_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM brands WHERE id = %s", (brand_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Brand not found")
    data = dict(row)
    verts = _q(conn,
        "SELECT v.id, v.name, bv.subvertical FROM brand_verticals bv "
        "JOIN verticals v ON v.id = bv.vertical_id WHERE bv.brand_id = %s",
        (brand_id,)
    ).fetchall()
    data["verticals"] = [dict(v) for v in verts]
    return single_response(data)


# ──────────────────────────── Campaigns ──────────────────────────────────────

@router.get("/campaigns")
def list_campaigns(cursor: Optional[str] = None, query: Optional[str] = None,
                   client_id: Optional[str] = None, status: Optional[str] = None,
                   conn=Depends(get_db)):
    conditions, params = [], []
    if client_id:
        conditions.append("client_id = %s")
        params.append(client_id)
    if status:
        conditions.append("status = %s")
        params.append(status)
    where = " AND ".join(conditions) if conditions else ""
    data, meta = paginate(conn, "campaigns", cursor, query=query,
                          query_columns=["name", "ugcid", "initiative_name"],
                          where_clause=where, where_params=tuple(params))
    # Attach KPIs
    for camp in data:
        kpis = _q(conn,
            "SELECT k.id, k.name, k.goal_type, ck.goal_value FROM campaign_kpis ck "
            "JOIN kpis k ON k.id = ck.kpi_id WHERE ck.campaign_id = %s",
            (camp["id"],)
        ).fetchall()
        camp["kpi_objectives"] = [dict(k) for k in kpis]
    return list_response(data, meta)


@router.get("/campaigns/{campaign_id}")
def get_campaign(campaign_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM campaigns WHERE id = %s", (campaign_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    data = dict(row)
    kpis = _q(conn,
        "SELECT k.id, k.name, k.goal_type, ck.goal_value FROM campaign_kpis ck "
        "JOIN kpis k ON k.id = ck.kpi_id WHERE ck.campaign_id = %s",
        (campaign_id,)
    ).fetchall()
    data["kpi_objectives"] = [dict(k) for k in kpis]
    return single_response(data)


# ──────────────────────────── Line Items ─────────────────────────────────────

@router.get("/campaigns/{campaign_id}/line_items")
def list_line_items(campaign_id: str, cursor: Optional[str] = None,
                    query: Optional[str] = None, conn=Depends(get_db)):
    data, meta = paginate(conn, "line_items", cursor, query=query,
                          query_columns=["name"],
                          where_clause="campaign_id = %s", where_params=(campaign_id,))
    return list_response(data, meta)


@router.get("/campaigns/{campaign_id}/line_items/{line_item_id}")
def get_line_item(campaign_id: str, line_item_id: str, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM line_items WHERE id = %s AND campaign_id = %s",
        (line_item_id, campaign_id)
    ).fetchone()
    if not row:
        raise HTTPException(404, "Line item not found")
    return single_response(dict(row))


# ──────────────────────────── Add-ons ────────────────────────────────────────

@router.get("/campaigns/{campaign_id}/addons")
def list_addons(campaign_id: str, cursor: Optional[str] = None,
                query: Optional[str] = None, conn=Depends(get_db)):
    data, meta = paginate(conn, "addons", cursor, query=query,
                          query_columns=["name"],
                          where_clause="campaign_id = %s", where_params=(campaign_id,))
    return list_response(data, meta)


@router.get("/campaigns/{campaign_id}/addons/{addon_id}")
def get_addon(campaign_id: str, addon_id: str, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM addons WHERE id = %s AND campaign_id = %s",
        (addon_id, campaign_id)
    ).fetchone()
    if not row:
        raise HTTPException(404, "Add-on not found")
    return single_response(dict(row))


# ──────────────────────────── Vendors ────────────────────────────────────────

@router.get("/vendors")
def list_vendors(cursor: Optional[str] = None, query: Optional[str] = None,
                 campaign_id: Optional[str] = None, conn=Depends(get_db)):
    data, meta = paginate(conn, "vendors", cursor, query=query,
                          query_columns=["name"])
    return list_response(data, meta)


@router.get("/vendors/{vendor_id}")
def get_vendor(vendor_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM vendors WHERE id = %s", (vendor_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Vendor not found")
    return single_response(dict(row))


# ──────────────────────────── Properties ─────────────────────────────────────

@router.get("/properties")
def list_properties(cursor: Optional[str] = None, query: Optional[str] = None,
                    campaign_id: Optional[str] = None, conn=Depends(get_db)):
    data, meta = paginate(conn, "properties", cursor, query=query,
                          query_columns=["name"])
    for prop in data:
        verts = _q(conn,
            "SELECT v.id, v.name FROM property_verticals pv "
            "JOIN verticals v ON v.id = pv.vertical_id WHERE pv.property_id = %s",
            (prop["id"],)
        ).fetchall()
        prop["verticals"] = [dict(v) for v in verts]
    return list_response(data, meta)


@router.get("/properties/{property_id}")
def get_property(property_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM properties WHERE id = %s", (property_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Property not found")
    data = dict(row)
    verts = _q(conn,
        "SELECT v.id, v.name FROM property_verticals pv "
        "JOIN verticals v ON v.id = pv.vertical_id WHERE pv.property_id = %s",
        (property_id,)
    ).fetchall()
    data["verticals"] = [dict(v) for v in verts]
    return single_response(data)


# ──────────────────────────── Verticals ──────────────────────────────────────

@router.get("/verticals")
def list_verticals(cursor: Optional[str] = None, query: Optional[str] = None,
                   conn=Depends(get_db)):
    data, meta = paginate(conn, "verticals", cursor, query=query,
                          query_columns=["name"])
    return list_response(data, meta)


@router.get("/verticals/{vertical_id}")
def get_vertical(vertical_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM verticals WHERE id = %s", (vertical_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Vertical not found")
    return single_response(dict(row))


# ──────────────────────────── KPIs ───────────────────────────────────────────

@router.get("/kpis")
def list_kpis(cursor: Optional[str] = None, query: Optional[str] = None,
              conn=Depends(get_db)):
    data, meta = paginate(conn, "kpis", cursor, query=query,
                          query_columns=["name"])
    return list_response(data, meta)


@router.get("/kpis/{kpi_id}")
def get_kpi(kpi_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM kpis WHERE id = %s", (kpi_id,)).fetchone()
    if not row:
        raise HTTPException(404, "KPI not found")
    return single_response(dict(row))


# ──────────────────────────── Creatives ──────────────────────────────────────

@router.get("/creatives")
def list_creatives(cursor: Optional[str] = None, conn=Depends(get_db)):
    data, meta = paginate(conn, "creatives", cursor)
    return list_response(data, meta)


@router.get("/creatives/{creative_id}")
def get_creative(creative_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM creatives WHERE id = %s", (creative_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Creative not found")
    return single_response(dict(row))


# ──────────────────────────── Conversions ────────────────────────────────────

@router.get("/conversions")
def list_conversions(cursor: Optional[str] = None, conn=Depends(get_db)):
    data, meta = paginate(conn, "conversions", cursor)
    return list_response(data, meta)


@router.get("/conversions/{conversion_id}")
def get_conversion(conversion_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM conversions WHERE id = %s", (conversion_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Conversion not found")
    return single_response(dict(row))


# ──────────────────────────── Delivery Sources ───────────────────────────────

@router.get("/delivery_sources")
def list_delivery_sources(cursor: Optional[str] = None, conn=Depends(get_db)):
    data, meta = paginate(conn, "delivery_sources", cursor)
    return list_response(data, meta)


@router.get("/delivery_sources/{source_id}")
def get_delivery_source(source_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM delivery_sources WHERE id = %s", (source_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Delivery source not found")
    return single_response(dict(row))


# ──────────────────────────── Groups ─────────────────────────────────────────

@router.get("/groups")
def list_groups(cursor: Optional[str] = None, query: Optional[str] = None,
                conn=Depends(get_db)):
    data, meta = paginate(conn, "groups_", cursor, query=query,
                          query_columns=["name"])
    for g in data:
        g["budget"] = {
            "amount": g.pop("budget_amount"),
            "type": g.pop("budget_type"),
            "flight_start": g.pop("flight_start"),
            "flight_end": g.pop("flight_end"),
            "even_delivery": bool(g.pop("even_delivery")),
        }
    return list_response(data, meta)


@router.get("/groups/{group_id}")
def get_group(group_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM groups_ WHERE id = %s", (group_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Group not found")
    data = dict(row)
    data["budget"] = {
        "amount": data.pop("budget_amount"),
        "type": data.pop("budget_type"),
        "flight_start": data.pop("flight_start"),
        "flight_end": data.pop("flight_end"),
        "even_delivery": bool(data.pop("even_delivery")),
    }
    return single_response(data)


# ──────────────────────────── Tactics ────────────────────────────────────────

@router.get("/tactics")
def list_tactics(cursor: Optional[str] = None, query: Optional[str] = None,
                 conn=Depends(get_db)):
    data, meta = paginate(conn, "tactics", cursor, query=query,
                          query_columns=["name"])
    for t in data:
        t["budget"] = {
            "amount": t.pop("budget_amount"),
            "type": t.pop("budget_type"),
            "flight_start": t.pop("flight_start"),
            "flight_end": t.pop("flight_end"),
            "pacing_priority": t.pop("pacing_priority"),
        }
    return list_response(data, meta)


@router.get("/tactics/{tactic_id}")
def get_tactic(tactic_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM tactics WHERE id = %s", (tactic_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Tactic not found")
    data = dict(row)
    data["budget"] = {
        "amount": data.pop("budget_amount"),
        "type": data.pop("budget_type"),
        "flight_start": data.pop("flight_start"),
        "flight_end": data.pop("flight_end"),
        "pacing_priority": data.pop("pacing_priority"),
    }
    return single_response(data)


# ──────────────────────────── Stats ──────────────────────────────────────────

@router.get("/stats/{scope}")
def get_stats(scope: str,
              cursor: Optional[str] = None,
              brand_id: Optional[str] = None,
              client_id: Optional[str] = None,
              campaign_id: Optional[str] = None,
              line_item_id: Optional[str] = None,
              start_date: Optional[str] = None,
              end_date: Optional[str] = None,
              conn=Depends(get_db)):
    # Normalize common plural/alternate forms
    scope_aliases = {"line_items": "line_item"}
    scope = scope_aliases.get(scope, scope)

    valid_scopes = ["line_item", "daily_by_line_item", "daily", "daily_by_conversion"]
    if scope not in valid_scopes:
        raise HTTPException(400, f"Invalid scope. Must be one of: {valid_scopes}")

    conditions, params = [], []

    if line_item_id:
        conditions.append("s.line_item_id = %s")
        params.append(line_item_id)
    if campaign_id:
        conditions.append("s.campaign_id = %s")
        params.append(campaign_id)
    if client_id:
        conditions.append("c.client_id = %s")
        params.append(client_id)
    if start_date:
        conditions.append("s.date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("s.date <= %s")
        params.append(end_date)

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    if scope == "line_item":
        sql = f"""
            SELECT s.line_item_id,
                   SUM(s.impressions) as impressions, SUM(s.clicks) as clicks,
                   SUM(s.spend) as spend, AVG(s.viewability) as viewability,
                   SUM(s.video_completions) as video_completions,
                   SUM(s.click_conversions) as click_conversions,
                   SUM(s.view_conversions) as view_conversions,
                   SUM(s.conversion_revenue) as conversion_revenue
            FROM stats s
            LEFT JOIN campaigns c ON s.campaign_id = c.id
            {where}
            GROUP BY s.line_item_id
        """
    elif scope == "daily":
        sql = f"""
            SELECT s.date,
                   SUM(s.impressions) as impressions, SUM(s.clicks) as clicks,
                   SUM(s.spend) as spend, AVG(s.viewability) as viewability,
                   SUM(s.video_completions) as video_completions,
                   SUM(s.click_conversions) as click_conversions,
                   SUM(s.view_conversions) as view_conversions,
                   SUM(s.conversion_revenue) as conversion_revenue
            FROM stats s
            LEFT JOIN campaigns c ON s.campaign_id = c.id
            {where}
            GROUP BY s.date ORDER BY s.date
        """
    elif scope == "daily_by_line_item":
        sql = f"""
            SELECT s.date, s.line_item_id,
                   s.impressions, s.clicks, s.spend, s.viewability,
                   s.video_completions, s.click_conversions, s.view_conversions,
                   s.conversion_revenue
            FROM stats s
            LEFT JOIN campaigns c ON s.campaign_id = c.id
            {where}
            ORDER BY s.date, s.line_item_id
        """
    else:  # daily_by_conversion
        sql = f"""
            SELECT s.date, s.line_item_id,
                   s.click_conversions, s.view_conversions, s.conversion_revenue
            FROM stats s
            LEFT JOIN campaigns c ON s.campaign_id = c.id
            {where}
            ORDER BY s.date
        """

    rows = _q(conn, sql, params).fetchall()
    data = [dict(r) for r in rows]

    # Add computed metrics
    for row in data:
        if "impressions" in row and row["impressions"] and row["impressions"] > 0:
            row["ecpm"] = round(row["spend"] / row["impressions"] * 1000, 4) if row.get("spend") else None
            row["ctr"] = round(row["clicks"] / row["impressions"], 6) if row.get("clicks") else None
        if "clicks" in row and row["clicks"] and row["clicks"] > 0:
            row["ecpc"] = round(row["spend"] / row["clicks"], 4) if row.get("spend") else None

    metadata = {"cursor": None, "page_size": len(data), "total": len(data)}
    return list_response(data, metadata)
