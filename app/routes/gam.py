"""Google Ad Manager (GAM) REST API v1 mock endpoints under /gam/v1.

Mirrors the Google Ad Manager API (Beta) at:
https://developers.google.com/ad-manager/api/beta/reference/rest

Resource hierarchy:
  /v1/networks — list/get networks
  /v1/networks/{networkCode}/companies — list/get companies
  /v1/networks/{networkCode}/orders — list/get orders
  /v1/networks/{networkCode}/lineItems — list/get line items
  /v1/networks/{networkCode}/adUnits — list/get ad units
  /v1/networks/{networkCode}/placements — list/get placements
  /v1/networks/{networkCode}/reports — list/get/run reports

Pagination: pageSize (default 50, max 1000), pageToken, nextPageToken, totalSize.
Get-by-name: GET /v1/{name} where name = "networks/{code}/resource/{id}".
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.database import get_db

router = APIRouter(prefix="/gam/v1")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _paginate(conn, sql, params, page_size=50, page_token=None, skip=0):
    """Token-based (offset) pagination matching GAM API style.

    Returns (rows, next_page_token, total_size).
    """
    page_size = min(page_size, 1000)

    # Count total
    count_sql = f"SELECT COUNT(*) AS count FROM ({sql}) _t"
    total = _q(conn, count_sql, params).fetchone()["count"]

    offset = int(page_token) if page_token else skip
    full_sql = f"{sql} LIMIT %s OFFSET %s"
    rows = _q(conn, full_sql, (*params, page_size, offset)).fetchall()
    next_token = None
    if len(rows) == page_size and (offset + page_size) < total:
        next_token = str(offset + page_size)
    return [dict(r) for r in rows], next_token, total


def _resource_name(network_code, resource_type, resource_id):
    """Build a GAM resource name string."""
    return f"networks/{network_code}/{resource_type}/{resource_id}"


# ── Networks ─────────────────────────────────────────────────────────────────

@router.get("/networks")
def list_networks(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM gam_networks").fetchall()
    networks = []
    for r in rows:
        n = dict(r)
        n["name"] = f"networks/{n['network_code']}"
        networks.append(n)
    return {"networks": networks}


@router.get("/networks/{network_code}")
def get_network(network_code: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM gam_networks WHERE network_code = %s",
             (network_code,)).fetchone()
    if not row:
        raise HTTPException(404, "Network not found")
    data = dict(row)
    data["name"] = f"networks/{network_code}"
    return data


# ── Companies ────────────────────────────────────────────────────────────────

def _format_company(row, network_code):
    d = dict(row)
    d["name"] = _resource_name(network_code, "companies", d["company_id"])
    return d


@router.get("/networks/{network_code}/companies")
def list_companies(network_code: str,
                   pageSize: int = 50,
                   pageToken: Optional[str] = None,
                   filter: Optional[str] = None,
                   orderBy: Optional[str] = None,
                   skip: int = 0,
                   conn=Depends(get_db)):
    data, next_token, total = _paginate(
        conn,
        "SELECT * FROM gam_companies WHERE network_code = %s",
        (network_code,), pageSize, pageToken, skip)
    return {
        "companies": [_format_company(r, network_code) for r in data],
        "nextPageToken": next_token,
        "totalSize": total,
    }


@router.get("/networks/{network_code}/companies/{company_id}")
def get_company(network_code: str, company_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM gam_companies WHERE company_id = %s AND network_code = %s",
        (company_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Company not found")
    return _format_company(row, network_code)


# ── Orders ───────────────────────────────────────────────────────────────────

def _format_order(row, network_code):
    d = dict(row)
    d["name"] = _resource_name(network_code, "orders", d["order_id"])
    # Advertiser as resource name reference
    if d.get("advertiser_id"):
        d["advertiser"] = _resource_name(network_code, "companies", d["advertiser_id"])
    if d.get("trafficker"):
        d["trafficker"] = f"networks/{network_code}/users/{d['trafficker']}"
    if d.get("salesperson"):
        d["salesperson"] = f"networks/{network_code}/users/{d['salesperson']}"
    return d


@router.get("/networks/{network_code}/orders")
def list_orders(network_code: str,
                pageSize: int = 50,
                pageToken: Optional[str] = None,
                filter: Optional[str] = None,
                orderBy: Optional[str] = None,
                skip: int = 0,
                conn=Depends(get_db)):
    data, next_token, total = _paginate(
        conn,
        "SELECT * FROM gam_orders WHERE network_code = %s",
        (network_code,), pageSize, pageToken, skip)
    return {
        "orders": [_format_order(r, network_code) for r in data],
        "nextPageToken": next_token,
        "totalSize": total,
    }


@router.get("/networks/{network_code}/orders/{order_id}")
def get_order(network_code: str, order_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM gam_orders WHERE order_id = %s AND network_code = %s",
        (order_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Order not found")
    return _format_order(row, network_code)


# ── Line Items ───────────────────────────────────────────────────────────────

def _format_line_item(row, network_code):
    d = dict(row)
    d["name"] = _resource_name(network_code, "lineItems", d["line_item_id"])
    # order as resource name
    if d.get("order_id"):
        d["order"] = _resource_name(network_code, "orders", d["order_id"])
    # Nested goal object
    d["goal"] = {
        "goalType": d.pop("goal_type", "LIFETIME"),
        "unitType": "IMPRESSIONS",
        "units": str(d.pop("goal_units", 0)),
    }
    # Nested rate (Money)
    d["rate"] = {
        "currencyCode": d.pop("cost_currency_code", "USD"),
        "amountMicros": str(d.pop("cost_amount_micros", 0)),
    }
    # Nested budget (Money) — derived from rate * goal
    goal_units = int(d["goal"]["units"]) if d["goal"]["units"] else 0
    rate_micros = int(d["rate"]["amountMicros"]) if d["rate"]["amountMicros"] else 0
    budget_micros = (goal_units * rate_micros) // 1000 if goal_units else 0
    d["budget"] = {
        "currencyCode": d["rate"]["currencyCode"],
        "amountMicros": str(budget_micros),
    }
    # Rename fields to match API camelCase
    d["displayName"] = d.pop("display_name", d.get("displayName", ""))
    d["lineItemType"] = d.pop("line_item_type", d.get("lineItemType", "STANDARD"))
    d["startTime"] = d.pop("start_time", None)
    d["endTime"] = d.pop("end_time", None)
    d["unlimitedEndTime"] = d.pop("unlimited_end_time", False)
    d["creativeRotationType"] = d.pop("creative_rotation_type", "EVEN")
    d["deliveredImpressions"] = d.pop("delivered_impressions", 0)
    d["deliveredClicks"] = d.pop("delivered_clicks", 0)
    d["discountPercentage"] = d.pop("discount_percentage", 0)
    d["updateTime"] = d.pop("update_time", None)
    # Clean up DB-only fields
    d.pop("cost_type", None)
    return d


@router.get("/networks/{network_code}/lineItems")
def list_line_items(network_code: str,
                    pageSize: int = 50,
                    pageToken: Optional[str] = None,
                    filter: Optional[str] = None,
                    orderBy: Optional[str] = None,
                    skip: int = 0,
                    conn=Depends(get_db)):
    data, next_token, total = _paginate(
        conn,
        "SELECT * FROM gam_line_items WHERE network_code = %s",
        (network_code,), pageSize, pageToken, skip)
    return {
        "lineItems": [_format_line_item(r, network_code) for r in data],
        "nextPageToken": next_token,
        "totalSize": total,
    }


@router.get("/networks/{network_code}/lineItems/{line_item_id}")
def get_line_item(network_code: str, line_item_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM gam_line_items WHERE line_item_id = %s AND network_code = %s",
        (line_item_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Line item not found")
    return _format_line_item(row, network_code)


# ── Ad Units ─────────────────────────────────────────────────────────────────

def _format_ad_unit(row, network_code):
    d = dict(row)
    d["name"] = _resource_name(network_code, "adUnits", d["ad_unit_id"])
    # adUnitSizes as array of Size objects
    sizes_str = d.pop("ad_unit_sizes", "")
    ad_unit_sizes = []
    if sizes_str:
        for s in sizes_str.split(","):
            s = s.strip()
            if "x" in s:
                w, h = s.split("x")
                ad_unit_sizes.append({
                    "size": {"width": int(w), "height": int(h), "sizeType": "PIXEL"},
                    "environmentType": "BROWSER",
                })
            elif s == "FLUID" or s == "1x1":
                ad_unit_sizes.append({
                    "size": {"width": 1, "height": 1, "sizeType": "FLUID"},
                    "environmentType": "BROWSER",
                })
    d["adUnitSizes"] = ad_unit_sizes
    # parentAdUnit reference
    parent_id = d.pop("parent_ad_unit_id", None)
    if parent_id:
        d["parentAdUnit"] = _resource_name(network_code, "adUnits", parent_id)
    else:
        d["parentAdUnit"] = ""
    d["displayName"] = d.pop("display_name", d.get("displayName", ""))
    d["adUnitCode"] = d.pop("ad_unit_code", d.get("adUnitCode", ""))
    d["adUnitId"] = str(d.pop("ad_unit_id", ""))
    d["status"] = d.get("status", "ACTIVE")
    d["explicitlyTargeted"] = d.pop("explicitly_targeted", False)
    d["targetWindow"] = d.pop("target_window", "BLANK")
    d["updateTime"] = d.pop("update_time", None)
    d["hasChildren"] = False
    d.pop("network_code", None)
    return d


@router.get("/networks/{network_code}/adUnits")
def list_ad_units(network_code: str,
                  pageSize: int = 50,
                  pageToken: Optional[str] = None,
                  filter: Optional[str] = None,
                  orderBy: Optional[str] = None,
                  skip: int = 0,
                  conn=Depends(get_db)):
    data, next_token, total = _paginate(
        conn,
        "SELECT * FROM gam_ad_units WHERE network_code = %s",
        (network_code,), pageSize, pageToken, skip)
    return {
        "adUnits": [_format_ad_unit(r, network_code) for r in data],
        "nextPageToken": next_token,
        "totalSize": total,
    }


@router.get("/networks/{network_code}/adUnits/{ad_unit_id}")
def get_ad_unit(network_code: str, ad_unit_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM gam_ad_units WHERE ad_unit_id = %s AND network_code = %s",
        (ad_unit_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Ad unit not found")
    return _format_ad_unit(row, network_code)


# ── Placements ───────────────────────────────────────────────────────────────

def _format_placement(row, network_code):
    d = dict(row)
    d["name"] = _resource_name(network_code, "placements", d["placement_id"])
    d["displayName"] = d.pop("display_name", d.get("displayName", ""))
    # targeted ad units as resource name list
    targeted_str = d.pop("targeted_ad_unit_ids", "")
    if targeted_str:
        d["targetedAdUnits"] = [
            _resource_name(network_code, "adUnits", uid.strip())
            for uid in targeted_str.split(",") if uid.strip()
        ]
    else:
        d["targetedAdUnits"] = []
    d["status"] = d.get("status", "ACTIVE")
    d["updateTime"] = d.pop("update_time", None)
    d.pop("network_code", None)
    d.pop("placement_id", None)
    return d


@router.get("/networks/{network_code}/placements")
def list_placements(network_code: str,
                    pageSize: int = 50,
                    pageToken: Optional[str] = None,
                    filter: Optional[str] = None,
                    orderBy: Optional[str] = None,
                    skip: int = 0,
                    conn=Depends(get_db)):
    data, next_token, total = _paginate(
        conn,
        "SELECT * FROM gam_placements WHERE network_code = %s",
        (network_code,), pageSize, pageToken, skip)
    return {
        "placements": [_format_placement(r, network_code) for r in data],
        "nextPageToken": next_token,
        "totalSize": total,
    }


@router.get("/networks/{network_code}/placements/{placement_id}")
def get_placement(network_code: str, placement_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM gam_placements WHERE placement_id = %s AND network_code = %s",
        (placement_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Placement not found")
    return _format_placement(row, network_code)


# ── Creatives ────────────────────────────────────────────────────────────────

def _format_creative(row, network_code):
    d = dict(row)
    d["name"] = _resource_name(network_code, "creatives", d["creative_id"])
    d["displayName"] = d.pop("display_name", d.get("displayName", ""))
    d["advertiser"] = _resource_name(network_code, "companies", d.get("advertiser_id", ""))
    d["creativeType"] = d.pop("creative_type", "THIRD_PARTY")
    if d.get("width") and d.get("height"):
        d["size"] = {"width": d.pop("width"), "height": d.pop("height"), "sizeType": "PIXEL"}
    else:
        d.pop("width", None)
        d.pop("height", None)
    d["previewUrl"] = d.pop("preview_url", None)
    d["deliveryStatus"] = d.pop("delivery_status", "ACTIVE")
    d["updateTime"] = d.pop("update_time", None)
    d.pop("network_code", None)
    d.pop("creative_id", None)
    d.pop("advertiser_id", None)
    return d


@router.get("/networks/{network_code}/creatives")
def list_creatives(network_code: str,
                   pageSize: int = 50,
                   pageToken: Optional[str] = None,
                   filter: Optional[str] = None,
                   orderBy: Optional[str] = None,
                   skip: int = 0,
                   conn=Depends(get_db)):
    data, next_token, total = _paginate(
        conn,
        "SELECT * FROM gam_creatives WHERE network_code = %s",
        (network_code,), pageSize, pageToken, skip)
    return {
        "creatives": [_format_creative(r, network_code) for r in data],
        "nextPageToken": next_token,
        "totalSize": total,
    }


@router.get("/networks/{network_code}/creatives/{creative_id}")
def get_creative(network_code: str, creative_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM gam_creatives WHERE creative_id = %s AND network_code = %s",
        (creative_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Creative not found")
    return _format_creative(row, network_code)


# ── Reports ──────────────────────────────────────────────────────────────────

def _format_report(row, network_code):
    d = dict(row)
    d["name"] = _resource_name(network_code, "reports", d["report_id"])
    d["displayName"] = d.pop("display_name", d.get("displayName", ""))
    d["reportType"] = d.pop("report_type", "HISTORICAL")
    d["updateTime"] = d.pop("update_time", None)
    d.pop("network_code", None)
    d.pop("report_id", None)
    return d


@router.get("/networks/{network_code}/reports")
def list_reports(network_code: str,
                 pageSize: int = 50,
                 pageToken: Optional[str] = None,
                 filter: Optional[str] = None,
                 orderBy: Optional[str] = None,
                 skip: int = 0,
                 conn=Depends(get_db)):
    data, next_token, total = _paginate(
        conn,
        "SELECT * FROM gam_reports WHERE network_code = %s",
        (network_code,), pageSize, pageToken, skip)
    return {
        "reports": [_format_report(r, network_code) for r in data],
        "nextPageToken": next_token,
        "totalSize": total,
    }


@router.get("/networks/{network_code}/reports/{report_id}")
def get_report(network_code: str, report_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM gam_reports WHERE report_id = %s AND network_code = %s",
        (report_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Report not found")
    return _format_report(row, network_code)


@router.post("/networks/{network_code}/reports/{report_id}:run")
def run_report(network_code: str, report_id: int, conn=Depends(get_db)):
    """Mock report execution — returns a long-running operation."""
    row = _q(conn,
        "SELECT * FROM gam_reports WHERE report_id = %s AND network_code = %s",
        (report_id, network_code)).fetchone()
    if not row:
        raise HTTPException(404, "Report not found")
    return {
        "name": f"networks/{network_code}/operations/reports/runs/{report_id}",
        "done": True,
        "result": _format_report(row, network_code),
    }
