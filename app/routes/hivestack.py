"""Hivestack API v2 mock endpoints — all resources under /hivestack.

Mirrors: https://apps.hivestack.com/api/v2/
Base path here: /hivestack (prefixed in router)

Sections:
  Ad Serving      – /nirvana/api/v1/units/{uuid}/schedulevast  (VAST)
  Accounts        – /accounts
  Advertisers     – /advertisers
  Campaigns       – /campaigns
  Line Items      – /lineitems
  Creatives       – /creatives  (+ approvals, associations)
  Avails          – /avails
  Networks        – /networks
  Sites           – /sites
  Units           – /units
  Unit Packs      – /unitpacks
  Demographics    – /demographics
  Languages       – /languages
  Locations       – /locations
  Mediatypes      – /mediatypes
  Unit Languages  – /unitlanguages
  Unit Properties – /unitproperties
  Concentrations  – /concentrations
  Report Defs     – /reportdefinitions
  Report Execs    – /reportexecutions
  Custom Reports  – /customreports
  Media Owners    – /mediaowners
  Events          – /customevents
  Deals (DSP)     – /dsp/creatives, /dsp/deals
  OpenRTB         – /openrtb/bidrequests
  Tracking        – /win  /bill  /imp
  Sellers.json    – /sellers.json
"""

import uuid as _uuid_mod
from datetime import datetime
from typing import Optional, List, Any

from fastapi import APIRouter, Depends, HTTPException, Body
from fastapi.responses import Response
from pydantic import BaseModel

from app.database import get_db

router = APIRouter(prefix="/hivestack")


# ── Shared helpers ────────────────────────────────────────────────────────────

def _now():
    return datetime.utcnow().isoformat() + "Z"

def _uuid():
    return str(_uuid_mod.uuid4())

def _cur(conn):
    return conn.cursor()

def _one(conn, sql, params=()):
    c = conn.cursor()
    c.execute(sql, params)
    return c.fetchone()

def _all(conn, sql, params=()):
    c = conn.cursor()
    c.execute(sql, params)
    return c.fetchall()

def _odata(conn, table, top=100, skip=0, filter_sql="", filter_params=(), count=False, order="id"):
    """Generic OData-style $top/$skip/$count list helper."""
    where = f"WHERE {filter_sql}" if filter_sql else ""
    c = conn.cursor()
    c.execute(f"SELECT COUNT(*) FROM {table} {where}", filter_params)
    total = c.fetchone()["count"]
    c.execute(f"SELECT * FROM {table} {where} ORDER BY {order} LIMIT %s OFFSET %s",
              list(filter_params) + [top, skip])
    rows = [dict(r) for r in c.fetchall()]
    resp = {"value": rows}
    if count:
        resp["@odata.count"] = total
    return resp

def _404(resource, id_):
    raise HTTPException(404, f"{resource} not found: {id_}")

def _mock_vast(w=1920, h=1080):
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<VAST version="2.0"><Ad id="mock-001"><InLine>'
        "<AdSystem>AdBridge Mock</AdSystem>"
        "<AdTitle><![CDATA[Mock DOOH Ad]]></AdTitle>"
        "<Impression><![CDATA[https://mock.adbridge.local/vast-imp]]></Impression>"
        "<Creatives><Creative><Linear><Duration>00:00:15</Duration><MediaFiles>"
        f'<MediaFile width="{w}" height="{h}" type="video/mp4" delivery="progressive">'
        "<![CDATA[https://mock.adbridge.local/creative/sample.mp4]]></MediaFile>"
        "</MediaFiles></Linear></Creative></Creatives>"
        "</InLine></Ad></VAST>"
    )


# ══════════════════════════════════════════════════════════════════════════════
# AD SERVING  (nirvana/api/v1 style paths kept under /hivestack/adserving)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/adserving/units/{uuid}/schedulevast", response_class=Response)
def schedule_vast_by_uuid(uuid: str, conn=Depends(get_db)):
    """Request an ad for a single screen by UUID — returns VAST 2.0 XML."""
    row = _one(conn, "SELECT * FROM hs_units WHERE id = %s", (uuid,))
    w, h = (row["width"], row["height"]) if row else (1920, 1080)
    return Response(content=_mock_vast(w, h), media_type="application/xml")


@router.get("/adserving/units/external/{screen_id}/schedulevast", response_class=Response)
def schedule_vast_by_screen_id(screen_id: str, conn=Depends(get_db)):
    """Request an ad for a single screen by external screen ID — returns VAST 2.0 XML."""
    row = _one(conn, "SELECT * FROM hs_units WHERE external_id = %s", (screen_id,))
    w, h = (row["width"], row["height"]) if row else (1920, 1080)
    return Response(content=_mock_vast(w, h), media_type="application/xml")


@router.post("/adserving/syncgroup/schedulevast", response_class=Response)
def schedule_vast_sync_group(body: dict = Body(...), conn=Depends(get_db)):
    """Request ads for a sync group (multiple screens) — returns VAST 2.0 XML."""
    return Response(content=_mock_vast(1920, 1080), media_type="application/xml")


@router.get("/adserving/units/{uuid}/creatives")
def get_upcoming_creatives_by_uuid(uuid: str, conn=Depends(get_db)):
    """Creatives that might play in the next 24 hours for a screen (by UUID)."""
    rows = _all(conn, "SELECT * FROM hs_creatives WHERE status = 'approved' LIMIT 5")
    return {"value": [dict(r) for r in rows]}


@router.get("/adserving/units/external/{screen_id}/creatives")
def get_upcoming_creatives_by_screen_id(screen_id: str, conn=Depends(get_db)):
    """Creatives that might play in the next 24 hours for a screen (by external ID)."""
    rows = _all(conn, "SELECT * FROM hs_creatives WHERE status = 'approved' LIMIT 5")
    return {"value": [dict(r) for r in rows]}


# ══════════════════════════════════════════════════════════════════════════════
# AVAILS
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/avails")
def check_avails_by_targeting(body: dict = Body(...), conn=Depends(get_db)):
    """Check avails for a targeting specification."""
    return {
        "available_impressions": 125000,
        "available_units": 8,
        "estimated_reach": 45000,
        "currency": "USD",
        "cpm_floor": 1.50,
    }


@router.get("/avails/lineitems/{lineitem_id}")
def check_avails_by_lineitem(lineitem_id: str, conn=Depends(get_db)):
    """Check avails for an existing line item."""
    row = _one(conn, "SELECT * FROM hs_lineitems WHERE id = %s", (lineitem_id,))
    if not row:
        _404("Line item", lineitem_id)
    return {
        "lineitem_id": lineitem_id,
        "available_impressions": 98000,
        "available_units": 6,
        "currency": "USD",
    }


# ══════════════════════════════════════════════════════════════════════════════
# ACCOUNTS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/accounts")
def list_accounts(top: int = 100, skip: int = 0, count: bool = False, conn=Depends(get_db)):
    return _odata(conn, "hs_accounts", top, skip, count=count)


@router.post("/accounts", status_code=201)
def create_account(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_accounts (id, name, type, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Account"), body.get("type", "buyer"),
         body.get("status", "active"), now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_accounts WHERE id = %s", (id_,)))


@router.get("/accounts/{account_id}")
def get_account(account_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_accounts WHERE id = %s", (account_id,))
    if not row:
        _404("Account", account_id)
    return dict(row)


@router.put("/accounts/{account_id}")
def update_account(account_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_accounts WHERE id = %s", (account_id,))
    if not row:
        _404("Account", account_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_accounts SET name=%s, type=%s, status=%s, updated_at=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("type", row["type"]),
         body.get("status", row["status"]), _now(), account_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_accounts WHERE id = %s", (account_id,)))


# ══════════════════════════════════════════════════════════════════════════════
# ADVERTISERS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/advertisers")
def list_advertisers(top: int = 100, skip: int = 0, filter: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if filter:
        fsql, fparams = "name ILIKE %s", (f"%{filter}%",)
    return _odata(conn, "hs_advertisers", top, skip, fsql, fparams)


@router.post("/advertisers", status_code=201)
def create_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_advertisers (id, name, account_id, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Advertiser"), body.get("account_id"),
         body.get("status", "active"), now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_advertisers WHERE id = %s", (id_,)))


@router.get("/advertisers/{advertiser_id}")
def get_advertiser(advertiser_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_advertisers WHERE id = %s", (advertiser_id,))
    if not row:
        _404("Advertiser", advertiser_id)
    return dict(row)


@router.put("/advertisers/{advertiser_id}")
def update_advertiser(advertiser_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_advertisers WHERE id = %s", (advertiser_id,))
    if not row:
        _404("Advertiser", advertiser_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_advertisers SET name=%s, status=%s, updated_at=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("status", row["status"]), _now(), advertiser_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_advertisers WHERE id = %s", (advertiser_id,)))


# ══════════════════════════════════════════════════════════════════════════════
# CAMPAIGNS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/campaigns")
def list_campaigns(top: int = 100, skip: int = 0, filter: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if filter:
        fsql, fparams = "name ILIKE %s", (f"%{filter}%",)
    return _odata(conn, "hs_campaigns", top, skip, fsql, fparams)


@router.post("/campaigns", status_code=201)
def create_campaign(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_campaigns (id, name, advertiser_id, status, start_date, end_date, budget, currency, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Campaign"), body.get("advertiser_id"),
         body.get("status", "draft"), body.get("start_date"), body.get("end_date"),
         body.get("budget", 0), body.get("currency", "USD"), now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_campaigns WHERE id = %s", (id_,)))


@router.get("/campaigns/{campaign_id}")
def get_campaign(campaign_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        _404("Campaign", campaign_id)
    return dict(row)


@router.put("/campaigns/{campaign_id}")
def update_campaign(campaign_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        _404("Campaign", campaign_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_campaigns SET name=%s, status=%s, start_date=%s, end_date=%s, budget=%s, updated_at=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("status", row["status"]),
         body.get("start_date", row["start_date"]), body.get("end_date", row["end_date"]),
         body.get("budget", row["budget"]), _now(), campaign_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_campaigns WHERE id = %s", (campaign_id,)))


# ══════════════════════════════════════════════════════════════════════════════
# LINE ITEMS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/lineitems")
def list_lineitems(top: int = 20, skip: int = 0, filter: Optional[str] = None,
                   expand: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if filter:
        fsql, fparams = "name ILIKE %s", (f"%{filter}%",)
    result = _odata(conn, "hs_lineitems", top, skip, fsql, fparams)
    if expand and "targeting" in expand:
        for item in result["value"]:
            item["targeting"] = _get_lineitem_targeting(conn, item["id"])
    return result


@router.post("/lineitems", status_code=201)
def create_lineitem(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_lineitems (id, name, campaign_id, status, start_date, end_date, "
        "budget, cpm, impressions_goal, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Line Item"), body.get("campaign_id"),
         body.get("status", "draft"), body.get("start_date"), body.get("end_date"),
         body.get("budget", 0), body.get("cpm", 1.0), body.get("impressions_goal", 0), now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_lineitems WHERE id = %s", (id_,)))


@router.get("/lineitems/{lineitem_id}")
def get_lineitem(lineitem_id: str, expand: Optional[str] = None, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_lineitems WHERE id = %s", (lineitem_id,))
    if not row:
        _404("Line item", lineitem_id)
    result = dict(row)
    if expand and "targeting" in expand:
        result["targeting"] = _get_lineitem_targeting(conn, lineitem_id)
    return result


@router.put("/lineitems/{lineitem_id}")
def update_lineitem(lineitem_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_lineitems WHERE id = %s", (lineitem_id,))
    if not row:
        _404("Line item", lineitem_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_lineitems SET name=%s, status=%s, start_date=%s, end_date=%s, "
        "budget=%s, cpm=%s, impressions_goal=%s, updated_at=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("status", row["status"]),
         body.get("start_date", row["start_date"]), body.get("end_date", row["end_date"]),
         body.get("budget", row["budget"]), body.get("cpm", row["cpm"]),
         body.get("impressions_goal", row["impressions_goal"]), _now(), lineitem_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_lineitems WHERE id = %s", (lineitem_id,)))


@router.get("/lineitems/{lineitem_id}/units")
def get_lineitem_targeted_units(lineitem_id: str, conn=Depends(get_db)):
    """Returns unit IDs targeted by the line item."""
    rows = _all(conn, "SELECT unit_id FROM hs_lineitem_units WHERE lineitem_id = %s", (lineitem_id,))
    return {"value": [r["unit_id"] for r in rows]}


def _get_lineitem_targeting(conn, lineitem_id):
    rows = _all(conn, "SELECT * FROM hs_lineitem_targeting WHERE lineitem_id = %s", (lineitem_id,))
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# CREATIVES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/creatives")
def list_creatives(top: int = 100, skip: int = 0, filter: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if filter:
        fsql, fparams = "name ILIKE %s", (f"%{filter}%",)
    return _odata(conn, "hs_creatives", top, skip, fsql, fparams)


@router.post("/creatives", status_code=201)
def create_creative(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_creatives (id, name, advertiser_id, type, status, width, height, "
        "file_url, approval_status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Creative"), body.get("advertiser_id"),
         body.get("type", "video"), body.get("status", "draft"),
         body.get("width", 1920), body.get("height", 1080),
         body.get("file_url"), "pending_review", now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_creatives WHERE id = %s", (id_,)))


@router.get("/creatives/{creative_id}")
def get_creative(creative_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_creatives WHERE id = %s", (creative_id,))
    if not row:
        _404("Creative", creative_id)
    return dict(row)


@router.put("/creatives/{creative_id}")
def update_creative(creative_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_creatives WHERE id = %s", (creative_id,))
    if not row:
        _404("Creative", creative_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_creatives SET name=%s, status=%s, approval_status=%s, updated_at=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("status", row["status"]),
         body.get("approval_status", "pending_review"), _now(), creative_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_creatives WHERE id = %s", (creative_id,)))


@router.get("/creatives/{creative_id}/units")
def get_creative_targeted_units(creative_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT unit_id FROM hs_creative_units WHERE creative_id = %s", (creative_id,))
    return {"value": [r["unit_id"] for r in rows]}


@router.post("/creatives/{creative_id}/files")
def upload_creative_files(creative_id: str, body: dict = Body(...), conn=Depends(get_db)):
    """Upload/link creative files to a creative."""
    row = _one(conn, "SELECT * FROM hs_creatives WHERE id = %s", (creative_id,))
    if not row:
        _404("Creative", creative_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_creatives SET file_url=%s, updated_at=%s WHERE id=%s",
        (body.get("file_url", row["file_url"]), _now(), creative_id),
    )
    conn.commit()
    return {"status": "ok", "creative_id": creative_id}


# ── Creative Approvals ────────────────────────────────────────────────────────

@router.get("/creativeapprovals")
def list_creative_approvals(top: int = 100, skip: int = 0, conn=Depends(get_db)):
    return _odata(conn, "hs_creative_approvals", top, skip)


@router.put("/creativeapprovals/{approval_id}")
def update_creative_approval(approval_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_creative_approvals WHERE id = %s", (approval_id,))
    if not row:
        _404("Creative approval", approval_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_creative_approvals SET approval_status=%s, updated_at=%s WHERE id=%s",
        (body.get("approval_status", "approved"), _now(), approval_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_creative_approvals WHERE id = %s", (approval_id,)))


@router.post("/creativeapprovals/{approval_id}/convert")
def convert_creative_approval_mode(approval_id: str, body: dict = Body(...), conn=Depends(get_db)):
    """Switch a media owner between approval modes."""
    return {"approval_id": approval_id, "mode": body.get("mode", "auto"), "status": "ok"}


# ── LineItem-Creative Associations ────────────────────────────────────────────

@router.get("/lineitemcreatives")
def list_lineitem_creatives(lineitem_id: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if lineitem_id:
        fsql, fparams = "lineitem_id = %s", (lineitem_id,)
    return _odata(conn, "hs_lineitem_creatives", 100, 0, fsql, fparams)


@router.get("/lineitemdealcreatives")
def list_lineitem_deal_creatives(lineitem_id: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if lineitem_id:
        fsql, fparams = "lineitem_id = %s", (lineitem_id,)
    return _odata(conn, "hs_lineitem_deal_creatives", 100, 0, fsql, fparams)


# ══════════════════════════════════════════════════════════════════════════════
# NETWORKS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/networks")
def list_networks(top: int = 500, skip: int = 0, count: bool = False, conn=Depends(get_db)):
    return _odata(conn, "hs_networks", top, skip, count=count)


@router.post("/networks", status_code=201)
def create_network(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_networks (id, name, description, created_at, updated_at) VALUES (%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Network"), body.get("description"), now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_networks WHERE id = %s", (id_,)))


@router.get("/networks/{network_id}")
def get_network(network_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_networks WHERE id = %s", (network_id,))
    if not row:
        _404("Network", network_id)
    return dict(row)


@router.put("/networks/{network_id}")
def update_network(network_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_networks WHERE id = %s", (network_id,))
    if not row:
        _404("Network", network_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_networks SET name=%s, description=%s, updated_at=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("description", row["description"]), _now(), network_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_networks WHERE id = %s", (network_id,)))


@router.delete("/networks/{network_id}", status_code=204)
def delete_network(network_id: str, conn=Depends(get_db)):
    c = _cur(conn)
    c.execute("DELETE FROM hs_networks WHERE id = %s", (network_id,))
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# SITES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/sites")
def list_sites(top: int = 500, skip: int = 0, count: bool = False, conn=Depends(get_db)):
    return _odata(conn, "hs_sites", top, skip, count=count)


@router.post("/sites", status_code=201)
def create_site(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_sites (id, name, network_id, external_id, address, city, country, created_at, updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Site"), body.get("network_id"), body.get("external_id"),
         body.get("address"), body.get("city"), body.get("country", "USA"), now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_sites WHERE id = %s", (id_,)))


@router.get("/sites/{site_id}")
def get_site(site_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_sites WHERE id = %s", (site_id,))
    if not row:
        _404("Site", site_id)
    return dict(row)


@router.put("/sites/{site_id}")
def update_site(site_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_sites WHERE id = %s", (site_id,))
    if not row:
        _404("Site", site_id)
    c = _cur(conn)
    c.execute(
        "UPDATE hs_sites SET name=%s, address=%s, city=%s, country=%s, updated_at=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("address", row["address"]),
         body.get("city", row["city"]), body.get("country", row["country"]), _now(), site_id),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_sites WHERE id = %s", (site_id,)))


@router.delete("/sites/{site_id}", status_code=204)
def delete_site(site_id: str, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_sites WHERE id = %s", (site_id,))
    conn.commit()


@router.get("/sites/external/{external_id}")
def get_site_by_external_id(external_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_sites WHERE external_id = %s", (external_id,))
    if not row:
        _404("Site", external_id)
    return dict(row)


@router.put("/sites/external/{external_id}")
def update_site_by_external_id(external_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_sites WHERE external_id = %s", (external_id,))
    if not row:
        _404("Site", external_id)
    return update_site(row["id"], body, conn)


@router.delete("/sites/external/{external_id}", status_code=204)
def delete_site_by_external_id(external_id: str, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_sites WHERE external_id = %s", (external_id,))
    conn.commit()

# ══════════════════════════════════════════════════════════════════════════════
# UNITS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/units")
def list_units(top: int = 500, skip: int = 0, count: bool = False, conn=Depends(get_db)):
    return _odata(conn, "hs_units", top, skip, count=count)

@router.post("/units", status_code=201)
def create_unit(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    now = _now()
    c = _cur(conn)
    c.execute(
        "INSERT INTO hs_units (id, name, site_id, external_id, width, height, "
        "venue_type_id, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name","New Unit"), body.get("site_id"), body.get("external_id"),
         body.get("width",1920), body.get("height",1080), body.get("venue_type_id"),
         body.get("status","active"), now, now),
    )
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_units WHERE id = %s", (id_,)))

@router.get("/units/{unit_id}")
def get_unit(unit_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_units WHERE id = %s", (unit_id,))
    if not row: _404("Unit", unit_id)
    return dict(row)

@router.put("/units/{unit_id}")
def update_unit(unit_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_units WHERE id = %s", (unit_id,))
    if not row: _404("Unit", unit_id)
    c = _cur(conn)
    c.execute("UPDATE hs_units SET name=%s, status=%s, updated_at=%s WHERE id=%s",
              (body.get("name",row["name"]), body.get("status",row["status"]), _now(), unit_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_units WHERE id = %s", (unit_id,)))

@router.delete("/units/{unit_id}", status_code=204)
def delete_unit(unit_id: str, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_units WHERE id = %s", (unit_id,))
    conn.commit()

@router.get("/units/external/{external_id}")
def get_unit_by_external_id(external_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_units WHERE external_id = %s", (external_id,))
    if not row: _404("Unit", external_id)
    return dict(row)

@router.put("/units/external/{external_id}")
def update_unit_by_external_id(external_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_units WHERE external_id = %s", (external_id,))
    if not row: _404("Unit", external_id)
    return update_unit(row["id"], body, conn)

@router.delete("/units/external/{external_id}", status_code=204)
def delete_unit_by_external_id(external_id: str, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_units WHERE external_id = %s", (external_id,))
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# UNIT PACKS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/unitpacks")
def list_unitpacks(top: int = 500, skip: int = 0, count: bool = False, conn=Depends(get_db)):
    return _odata(conn, "hs_unitpacks", top, skip, count=count)

@router.post("/unitpacks", status_code=201)
def create_unitpack(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_unitpacks (id, name, description, created_at, updated_at) VALUES (%s,%s,%s,%s,%s)",
        (id_, body.get("name", "New Pack"), body.get("description"), now, now))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_unitpacks WHERE id = %s", (id_,)))

@router.get("/unitpacks/{pack_id}")
def get_unitpack(pack_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_unitpacks WHERE id = %s", (pack_id,))
    if not row: _404("Unit pack", pack_id)
    return dict(row)

@router.put("/unitpacks/{pack_id}")
def update_unitpack(pack_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_unitpacks WHERE id = %s", (pack_id,))
    if not row: _404("Unit pack", pack_id)
    _cur(conn).execute("UPDATE hs_unitpacks SET name=%s, description=%s, updated_at=%s WHERE id=%s",
                       (body.get("name", row["name"]), body.get("description", row["description"]), _now(), pack_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_unitpacks WHERE id = %s", (pack_id,)))

@router.delete("/unitpacks/{pack_id}", status_code=204)
def delete_unitpack(pack_id: str, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_unitpacks WHERE id = %s", (pack_id,))
    conn.commit()

@router.get("/unitpacks/{pack_id}/units")
def get_unitpack_units(pack_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_units WHERE unitpack_id = %s", (pack_id,))
    return {"value": [dict(r) for r in rows]}

@router.get("/unitpacks/{pack_id}/targeting")
def get_unitpack_targeting(pack_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_unitpack_targeting WHERE unitpack_id = %s", (pack_id,))
    return {"value": [dict(r) for r in rows]}

@router.post("/unitpacks/{pack_id}/targeting", status_code=201)
def create_unitpack_targeting(pack_id: str, body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid()
    _cur(conn).execute(
        "INSERT INTO hs_unitpack_targeting (id, unitpack_id, targeting_type, targeting_value, created_at) VALUES (%s,%s,%s,%s,%s)",
        (id_, pack_id, body.get("type"), body.get("value"), _now()))
    conn.commit()
    return {"id": id_, "unitpack_id": pack_id}

@router.put("/unitpacks/{pack_id}/targeting")
def update_unitpack_targeting(pack_id: str, body: dict = Body(...), conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_unitpack_targeting WHERE unitpack_id = %s", (pack_id,))
    conn.commit()
    return create_unitpack_targeting(pack_id, body, conn)


# ══════════════════════════════════════════════════════════════════════════════
# DEMOGRAPHICS  (inventory-side)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/demographics")
def list_demographics(top: int = 100, skip: int = 0, conn=Depends(get_db)):
    return _odata(conn, "hs_demographics", top, skip)

@router.post("/demographics", status_code=201)
def create_demographic(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_demographics (id, name, code, description, created_at) VALUES (%s,%s,%s,%s,%s)",
        (id_, body.get("name"), body.get("code"), body.get("description"), now))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_demographics WHERE id = %s", (id_,)))

@router.get("/demographics/{demo_id}")
def get_demographic(demo_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_demographics WHERE id = %s", (demo_id,))
    if not row: _404("Demographic", demo_id)
    return dict(row)

@router.put("/demographics/{demo_id}")
def update_demographic(demo_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_demographics WHERE id = %s", (demo_id,))
    if not row: _404("Demographic", demo_id)
    _cur(conn).execute("UPDATE hs_demographics SET name=%s, code=%s, description=%s WHERE id=%s",
                       (body.get("name", row["name"]), body.get("code", row["code"]),
                        body.get("description", row["description"]), demo_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_demographics WHERE id = %s", (demo_id,)))


# ══════════════════════════════════════════════════════════════════════════════
# LANGUAGES / LOCATIONS / MEDIATYPES  (reference data)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/languages")
def list_languages(conn=Depends(get_db)):
    return {"value": [dict(r) for r in _all(conn, "SELECT * FROM hs_languages ORDER BY name")]}

@router.get("/languages/{lang_id}")
def get_language(lang_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_languages WHERE id = %s", (lang_id,))
    if not row: _404("Language", lang_id)
    return dict(row)

@router.get("/locations")
def list_locations(top: int = 100, skip: int = 0, conn=Depends(get_db)):
    return _odata(conn, "hs_locations", top, skip, order="name")

@router.get("/locations/{loc_id}")
def get_location(loc_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_locations WHERE id = %s", (loc_id,))
    if not row: _404("Location", loc_id)
    return dict(row)

@router.get("/mediatypes")
def list_mediatypes(conn=Depends(get_db)):
    return {"value": [dict(r) for r in _all(conn, "SELECT * FROM hs_mediatypes ORDER BY name")]}


# ══════════════════════════════════════════════════════════════════════════════
# UNIT LANGUAGES / UNIT PROPERTIES
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/unitlanguages")
def list_unit_languages(unit_id: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if unit_id:
        fsql, fparams = "unit_id = %s", (unit_id,)
    return _odata(conn, "hs_unit_languages", 500, 0, fsql, fparams)

@router.post("/unitlanguages", status_code=201)
def create_unit_language(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_unit_languages (id, unit_id, language_id, created_at) VALUES (%s,%s,%s,%s)",
        (id_, body.get("unit_id"), body.get("language_id"), now))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_unit_languages WHERE id = %s", (id_,)))

@router.get("/unitlanguages/{ul_id}")
def get_unit_language(ul_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_unit_languages WHERE id = %s", (ul_id,))
    if not row: _404("Unit language", ul_id)
    return dict(row)

@router.put("/unitlanguages/{ul_id}")
def update_unit_language(ul_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_unit_languages WHERE id = %s", (ul_id,))
    if not row: _404("Unit language", ul_id)
    _cur(conn).execute("UPDATE hs_unit_languages SET language_id=%s WHERE id=%s",
                       (body.get("language_id", row["language_id"]), ul_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_unit_languages WHERE id = %s", (ul_id,)))

@router.delete("/unitlanguages/{ul_id}", status_code=204)
def delete_unit_language(ul_id: str, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_unit_languages WHERE id = %s", (ul_id,))
    conn.commit()

@router.get("/unitproperties")
def list_unit_properties(unit_id: Optional[str] = None, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if unit_id:
        fsql, fparams = "unit_id = %s", (unit_id,)
    return _odata(conn, "hs_unit_properties", 500, 0, fsql, fparams)

@router.post("/unitproperties", status_code=201)
def create_unit_property(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_unit_properties (id, unit_id, key, value, created_at) VALUES (%s,%s,%s,%s,%s)",
        (id_, body.get("unit_id"), body.get("key"), body.get("value"), now))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_unit_properties WHERE id = %s", (id_,)))

@router.get("/unitproperties/{prop_id}")
def get_unit_property(prop_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_unit_properties WHERE id = %s", (prop_id,))
    if not row: _404("Unit property", prop_id)
    return dict(row)

@router.put("/unitproperties/{prop_id}")
def update_unit_property(prop_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_unit_properties WHERE id = %s", (prop_id,))
    if not row: _404("Unit property", prop_id)
    _cur(conn).execute("UPDATE hs_unit_properties SET key=%s, value=%s WHERE id=%s",
                       (body.get("key", row["key"]), body.get("value", row["value"]), prop_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_unit_properties WHERE id = %s", (prop_id,)))

@router.delete("/unitproperties/{prop_id}", status_code=204)
def delete_unit_property(prop_id: str, conn=Depends(get_db)):
    _cur(conn).execute("DELETE FROM hs_unit_properties WHERE id = %s", (prop_id,))
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# HOURLY CONCENTRATIONS
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/concentrations")
def upsert_concentrations(body: dict = Body(...), conn=Depends(get_db)):
    """Insert or update hourly audience concentration data for a unit."""
    unit_id = body.get("unit_id")
    entries = body.get("data", [])
    c = _cur(conn)
    for entry in entries:
        c.execute(
            "INSERT INTO hs_concentrations (unit_id, day_of_week, hour, audience_count, updated_at) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (unit_id, day_of_week, hour) DO UPDATE "
            "SET audience_count=EXCLUDED.audience_count, updated_at=EXCLUDED.updated_at",
            (unit_id, entry.get("day_of_week"), entry.get("hour"),
             entry.get("audience_count", 0), _now()))
    conn.commit()
    return {"status": "ok", "unit_id": unit_id, "records_upserted": len(entries)}

@router.post("/concentrations/bulk")
def upsert_concentrations_bulk(body: dict = Body(...), conn=Depends(get_db)):
    """Update audience data for multiple units at once."""
    units = body.get("units", [])
    total = 0
    c = _cur(conn)
    for unit_block in units:
        uid = unit_block.get("unit_id")
        for entry in unit_block.get("data", []):
            c.execute(
                "INSERT INTO hs_concentrations (unit_id, day_of_week, hour, audience_count, updated_at) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (unit_id, day_of_week, hour) DO UPDATE "
                "SET audience_count=EXCLUDED.audience_count, updated_at=EXCLUDED.updated_at",
                (uid, entry.get("day_of_week"), entry.get("hour"),
                 entry.get("audience_count", 0), _now()))
            total += 1
    conn.commit()
    return {"status": "ok", "records_upserted": total}


# ══════════════════════════════════════════════════════════════════════════════
# REPORT DEFINITIONS / EXECUTIONS / CUSTOM REPORTS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/reportdefinitions")
def list_report_definitions(conn=Depends(get_db)):
    return {"value": [dict(r) for r in _all(conn, "SELECT * FROM hs_report_definitions ORDER BY name")]}

@router.get("/reportdefinitions/{def_id}")
def get_report_definition(def_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_report_definitions WHERE id = %s", (def_id,))
    if not row: _404("Report definition", def_id)
    return dict(row)

@router.post("/reportdefinitions/{def_id}/run")
def run_report_definition(def_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_report_definitions WHERE id = %s", (def_id,))
    if not row: _404("Report definition", def_id)
    exec_id = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_report_executions (id, definition_id, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s)",
        (exec_id, def_id, "running", now, now))
    conn.commit()
    return {"execution_id": exec_id, "status": "running"}

@router.get("/reportexecutions")
def list_report_executions(conn=Depends(get_db)):
    return {"value": [dict(r) for r in _all(conn, "SELECT * FROM hs_report_executions ORDER BY created_at DESC")]}

@router.get("/reportexecutions/{exec_id}")
def get_report_execution(exec_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_report_executions WHERE id = %s", (exec_id,))
    if not row: _404("Report execution", exec_id)
    result = dict(row)
    if result.get("status") == "running":
        result["status"] = "completed"
        result["download_url"] = f"https://mock.adbridge.local/reports/{exec_id}.csv"
    return result

@router.post("/customreports")
def create_custom_report(body: dict = Body(...), conn=Depends(get_db)):
    exec_id = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_report_executions (id, definition_id, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s)",
        (exec_id, None, "running", now, now))
    conn.commit()
    return {"execution_id": exec_id, "status": "running"}

@router.get("/customreports/{exec_id}")
def get_custom_report(exec_id: str, conn=Depends(get_db)):
    return get_report_execution(exec_id, conn)


# ══════════════════════════════════════════════════════════════════════════════
# MEDIA OWNERS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/mediaowners")
def list_media_owners(top: int = 100, skip: int = 0, conn=Depends(get_db)):
    return _odata(conn, "hs_publishers", top, skip)


# ══════════════════════════════════════════════════════════════════════════════
# EVENT MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/customevents")
def list_custom_events(conn=Depends(get_db)):
    return {"value": [dict(r) for r in _all(conn, "SELECT * FROM hs_custom_events ORDER BY created_at DESC")]}

@router.post("/customevents", status_code=201)
def create_custom_event(body: dict = Body(...), conn=Depends(get_db)):
    id_ = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_custom_events (id, name, description, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name"), body.get("description"), body.get("status", "active"), now, now))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_custom_events WHERE id = %s", (id_,)))

@router.get("/customevents/{event_id}")
def get_custom_event(event_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_custom_events WHERE id = %s", (event_id,))
    if not row: _404("Custom event", event_id)
    return dict(row)

@router.put("/customevents/{event_id}")
def update_custom_event(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_custom_events WHERE id = %s", (event_id,))
    if not row: _404("Custom event", event_id)
    _cur(conn).execute("UPDATE hs_custom_events SET name=%s, status=%s, updated_at=%s WHERE id=%s",
                       (body.get("name", row["name"]), body.get("status", row["status"]), _now(), event_id))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_custom_events WHERE id = %s", (event_id,)))

# Event Data Management
@router.get("/eventdata/active")
def get_active_events(conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_custom_events WHERE status = 'active'")
    return {"value": [dict(r) for r in rows]}

@router.get("/eventdata/{event_id}/units")
def get_event_units(event_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_event_data WHERE event_id=%s AND entity_type='unit'", (event_id,))
    return {"value": [dict(r) for r in rows]}

@router.get("/eventdata/{event_id}/sites")
def get_event_sites(event_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_event_data WHERE event_id=%s AND entity_type='site'", (event_id,))
    return {"value": [dict(r) for r in rows]}

@router.get("/eventdata/{event_id}/locations")
def get_event_locations(event_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_event_data WHERE event_id=%s AND entity_type='location'", (event_id,))
    return {"value": [dict(r) for r in rows]}

@router.get("/eventdata/{event_id}/global")
def get_event_global(event_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_custom_events WHERE id = %s", (event_id,))
    if not row: _404("Event", event_id)
    return {"event_id": event_id, "global_status": row["status"], "active_units": 0}

@router.post("/eventdata/{event_id}/units/status")
def update_event_unit_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    _upsert_event_data(conn, event_id, "unit", body)
    return {"status": "ok"}

@router.post("/eventdata/{event_id}/sites/status")
def update_event_site_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    _upsert_event_data(conn, event_id, "site", body)
    return {"status": "ok"}

@router.post("/eventdata/{event_id}/locations/status")
def update_event_location_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    _upsert_event_data(conn, event_id, "location", body)
    return {"status": "ok"}

@router.post("/eventdata/{event_id}/global/status")
def update_event_global_data(event_id: str, body: dict = Body(...), conn=Depends(get_db)):
    return {"event_id": event_id, "status": "ok"}

def _upsert_event_data(conn, event_id, entity_type, body):
    id_ = _uuid()
    _cur(conn).execute(
        "INSERT INTO hs_event_data (id, event_id, entity_type, entity_id, status, updated_at) VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (event_id, entity_type, entity_id) DO UPDATE SET status=EXCLUDED.status, updated_at=EXCLUDED.updated_at",
        (id_, event_id, entity_type, body.get("entity_id", ""), body.get("status", "active"), _now()))
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# DSP — CREATIVE SUBMISSION & DEALS
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/dsp/creatives", status_code=201)
def dsp_submit_creative(body: dict = Body(...), conn=Depends(get_db)):
    """DSP creative submission endpoint."""
    id_ = _uuid(); now = _now()
    _cur(conn).execute(
        "INSERT INTO hs_creatives (id, name, advertiser_id, type, status, width, height, "
        "file_url, approval_status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (id_, body.get("name", "DSP Creative"), body.get("advertiser_id"),
         body.get("type", "video"), "active",
         body.get("width", 1920), body.get("height", 1080),
         body.get("file_url"), "pending_review", now, now))
    conn.commit()
    return dict(_one(conn, "SELECT * FROM hs_creatives WHERE id = %s", (id_,)))

@router.get("/dsp/creatives")
def dsp_list_creatives(top: int = 100, skip: int = 0, conn=Depends(get_db)):
    return _odata(conn, "hs_creatives", top, skip)

@router.get("/dsp/creatives/{creative_id}")
def dsp_get_creative(creative_id: str, conn=Depends(get_db)):
    return get_creative(creative_id, conn)

@router.get("/dsp/creatives/{creative_id}/approvals")
def dsp_get_creative_approvals(creative_id: str, conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_creative_approvals WHERE creative_id = %s", (creative_id,))
    return {"value": [dict(r) for r in rows]}

@router.get("/dsp/deals")
def dsp_list_deals(top: int = 100, skip: int = 0, conn=Depends(get_db)):
    return _odata(conn, "hs_deals", top, skip)

@router.get("/dsp/deals/{deal_id}")
def dsp_get_deal(deal_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_deals WHERE id = %s", (deal_id,))
    if not row: _404("Deal", deal_id)
    return dict(row)


# ══════════════════════════════════════════════════════════════════════════════
# OPENRTB 2.5 BID ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/openrtb/bidrequests")
def receive_bid_request(body: dict = Body(...), conn=Depends(get_db)):
    """Receive an OpenRTB 2.5 bid request, store it, return a mock bid response."""
    import json
    req_id = body.get("id", _uuid())
    _cur(conn).execute(
        "INSERT INTO hs_bid_requests (id, payload, received_at) VALUES (%s,%s,%s) "
        "ON CONFLICT (id) DO NOTHING",
        (req_id, json.dumps(body), _now()))
    conn.commit()
    imps = body.get("imp", [{}])
    cur_list = body.get("cur", ["USD"])
    currency = cur_list[0] if cur_list else "USD"
    bids = []
    for imp in imps:
        floor = imp.get("bidfloor", 1.0) or 1.0
        deal_id = None
        pmp = imp.get("pmp", {})
        if pmp and pmp.get("deals"):
            deal_id = pmp["deals"][0].get("id")
        bids.append({
            "id": str(abs(hash(imp.get("id", "1"))))[:16],
            "impid": imp.get("id", "1"),
            "price": floor,
            "adm": _mock_vast(),
            "adomain": ["mock-advertiser.com"],
            "crid": f"CRID-{_uuid()[:8]}",
            "cat": ["IAB3"],
            "dealid": deal_id,
            "nurl": "https://mock.adbridge.local/hivestack/win?auction_id=${AUCTION_ID}&price=${AUCTION_PRICE}",
            "burl": "https://mock.adbridge.local/hivestack/bill?total=${TOTAL_IMP}&price=${AUCTION_PRICE}",
            "lurl": "https://mock.adbridge.local/hivestack/loss?reason=${AUCTION_LOSS}",
            "ext": {
                "impurls": ["https://mock.adbridge.local/hivestack/imp?total=${TOTAL_IMP}&target=${TARGET_IMP}&price=${TOTAL_PRICE}"],
                "targetdemo": ["GNDR-*", "AGE-18-54"],
            },
        })
    return {"id": req_id, "cur": currency, "bidid": _uuid(),
            "seatbid": [{"bid": bids, "seat": "mock-seat-001"}]}

@router.get("/openrtb/bidrequests")
def list_bid_requests(top: int = 25, skip: int = 0, conn=Depends(get_db)):
    return _odata(conn, "hs_bid_requests", top, skip, order="received_at")

@router.get("/openrtb/bidrequests/{request_id}")
def get_bid_request(request_id: str, conn=Depends(get_db)):
    row = _one(conn, "SELECT * FROM hs_bid_requests WHERE id = %s", (request_id,))
    if not row: _404("Bid request", request_id)
    return dict(row)


# ══════════════════════════════════════════════════════════════════════════════
# TRACKING CALLBACKS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/win")
def win_notice(auction_id: Optional[str] = None, price: Optional[float] = None, conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, price, recorded_at) VALUES (%s,%s,%s,%s)",
        (auction_id, "win", price, _now()))
    conn.commit()
    return {"status": "ok", "event": "win", "auction_id": auction_id}

@router.get("/bill")
def billing_notice(auction_id: Optional[str] = None, total: Optional[float] = None,
                   price: Optional[float] = None, conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, total_imp, price, recorded_at) VALUES (%s,%s,%s,%s,%s)",
        (auction_id, "bill", total, price, _now()))
    conn.commit()
    return {"status": "ok", "event": "bill", "auction_id": auction_id}

@router.get("/imp")
def impression_notice(auction_id: Optional[str] = None, total: Optional[float] = None,
                      target: Optional[float] = None, price: Optional[float] = None,
                      conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, total_imp, target_imp, price, recorded_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (auction_id, "impression", total, target, price, _now()))
    conn.commit()
    return {"status": "ok", "event": "impression", "total_imp": total, "target_imp": target}

@router.get("/loss")
def loss_notice(auction_id: Optional[str] = None, reason: Optional[str] = None, conn=Depends(get_db)):
    _cur(conn).execute(
        "INSERT INTO hs_impressions (bid_request_id, event_type, recorded_at) VALUES (%s,%s,%s)",
        (auction_id, f"loss:{reason}", _now()))
    conn.commit()
    return {"status": "ok", "event": "loss"}

@router.get("/impressions")
def list_impressions(event_type: Optional[str] = None, top: int = 50, skip: int = 0, conn=Depends(get_db)):
    fsql, fparams = ("", ())
    if event_type:
        fsql, fparams = "event_type = %s", (event_type,)
    return _odata(conn, "hs_impressions", top, skip, fsql, fparams, order="recorded_at")


# ══════════════════════════════════════════════════════════════════════════════
# SELLERS.JSON
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/sellers.json")
def sellers_json(conn=Depends(get_db)):
    rows = _all(conn, "SELECT * FROM hs_publishers ORDER BY id")
    sellers = [{"seller_id": r["id"], "name": r["name"], "domain": r["domain"],
                "seller_type": r.get("seller_type", "PUBLISHER"),
                "ext": {"impression_multiplier_verifier": {"domain": r["domain"], "seller_id": r["id"]}}}
               for r in rows]
    return {"sellers": sellers, "contact_email": "support@hivestack.com", "version": "1.0"}

