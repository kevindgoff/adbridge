"""Triton Digital Booking (TAP) API mock endpoints under /triton-booking."""

from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Optional
from pydantic import BaseModel

from app.database import get_db

router = APIRouter(prefix="/triton-booking")


class AdvertiserCreate(BaseModel):
    name: str
    domain: Optional[str] = None
    iab_category_code: Optional[str] = None
    currency_iso_code: str = "USD"
    external_id: Optional[str] = None
    status: str = "active"
    agency: Optional[dict] = None
    ad_separation_override: Optional[str] = None
    reseller_id: Optional[int] = None
    type: str = "direct"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _paginate(conn, sql, params, start=0, limit=1000, sort=None, sort_default="id"):
    cur = conn.cursor()
    if sort:
        direction = "DESC" if sort.startswith("-") else "ASC"
        col = sort.lstrip("-")
        sql += f" ORDER BY {col} {direction}"
    else:
        sql += f" ORDER BY {sort_default}"
    cur.execute(f"SELECT COUNT(*) FROM ({sql}) AS _sub", params)
    total = cur.fetchone()["count"]
    cur.execute(sql + " LIMIT %s OFFSET %s", (*params, limit, start))
    rows = cur.fetchall()
    return [dict(r) for r in rows], total


def _search_response(data, total, start=0, limit=1000):
    return {"data": data, "total": total, "start": start, "limit": limit}


def _fetch_one(conn, sql, params):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchone()


def _format_advertiser(row):
    agency = None
    if row.get("agency_name") or row.get("agency_external_id"):
        agency = {"name": row.pop("agency_name", None), "external_id": row.pop("agency_external_id", None)}
    else:
        row.pop("agency_name", None)
        row.pop("agency_external_id", None)
    row["agency"] = agency
    return row


def _format_flight(row):
    goal = {}
    gi = row.pop("goal_impressions", None)
    gs = row.pop("goal_spots", None)
    if gi is not None:
        goal["impressions"] = gi
    if gs is not None:
        goal["spots"] = gs
    row["goal"] = goal
    return row


# ── Audits ───────────────────────────────────────────────────────────────────

@router.get("/audits")
def search_audits(start: int = 0, limit: int = 1000, sort: Optional[str] = None,
                  conn=Depends(get_db)):
    data, total = _paginate(conn, "SELECT * FROM tap_audits", (), start, limit, sort)
    return _search_response(data, total, start, limit)


# ── Advertisers ──────────────────────────────────────────────────────────────

@router.get("/advertisers")
def search_advertisers(start: int = 0, limit: int = 1000, sort: Optional[str] = None,
                       name: Optional[str] = None, status: Optional[str] = None,
                       conn=Depends(get_db)):
    conditions, params = [], []
    if name:
        conditions.append("name ILIKE %s")
        params.append(f"%{name}%")
    if status:
        conditions.append("status = %s")
        params.append(status)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    data, total = _paginate(conn, f"SELECT * FROM tap_advertisers{where}", tuple(params), start, limit, sort)
    return _search_response([_format_advertiser(d) for d in data], total, start, limit)


@router.post("/advertisers")
def create_advertiser(body: AdvertiserCreate, conn=Depends(get_db)):
    from app.database import _now
    now = _now()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tap_advertisers (name,domain,iab_category_code,currency_iso_code,"
        "external_id,status,agency_name,agency_external_id,ad_separation_override,"
        "reseller_id,type,created_by,creation_time,last_updated_time) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (body.name, body.domain, body.iab_category_code, body.currency_iso_code,
         body.external_id, body.status,
         body.agency.get("name") if body.agency else None,
         body.agency.get("external_id") if body.agency else None,
         body.ad_separation_override, body.reseller_id,
         body.type, "api@triton.com", now, now),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_advertisers WHERE id = %s", (new_id,))
    return _format_advertiser(dict(row))


@router.get("/advertisers/{advertiser_id}")
def get_advertiser(advertiser_id: int, conn=Depends(get_db)):
    row = _fetch_one(conn, "SELECT * FROM tap_advertisers WHERE id = %s", (advertiser_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Advertiser not found"}]})
    return _format_advertiser(dict(row))


@router.put("/advertisers/{advertiser_id}")
def update_advertiser(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    row = _fetch_one(conn, "SELECT * FROM tap_advertisers WHERE id = %s", (advertiser_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Advertiser not found"}]})
    row = dict(row)
    cur = conn.cursor()
    cur.execute(
        "UPDATE tap_advertisers SET name=%s,domain=%s,iab_category_code=%s,currency_iso_code=%s,"
        "external_id=%s,status=%s,agency_name=%s,agency_external_id=%s,ad_separation_override=%s,"
        "reseller_id=%s,type=%s,last_updated_time=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("domain", row["domain"]),
         body.get("iab_category_code", row["iab_category_code"]),
         body.get("currency_iso_code", row["currency_iso_code"]),
         body.get("external_id", row["external_id"]),
         body.get("status", row["status"]),
         body.get("agency", {}).get("name", row["agency_name"]) if body.get("agency") else row["agency_name"],
         body.get("agency", {}).get("external_id", row["agency_external_id"]) if body.get("agency") else row["agency_external_id"],
         body.get("ad_separation_override", row["ad_separation_override"]),
         body.get("reseller_id", row["reseller_id"]),
         body.get("type", row["type"]), _now(), advertiser_id),
    )
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_advertisers WHERE id = %s", (advertiser_id,))
    return _format_advertiser(dict(row))


@router.patch("/advertisers/{advertiser_id}")
def patch_advertiser(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return update_advertiser(advertiser_id, body, conn)


# ── Billing ──────────────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/billing")
def get_billing(advertiser_id: int, conn=Depends(get_db)):
    row = _fetch_one(conn, "SELECT * FROM tap_billing WHERE advertiser_id = %s", (advertiser_id,))
    if not row:
        return {}
    return dict(row)


@router.post("/advertisers/{advertiser_id}/billing")
def create_billing(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    now = _now()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tap_billing (advertiser_id,billing_contact,billing_email,"
        "billing_address,payment_terms,creation_time,last_updated_time) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (advertiser_id, body.get("billing_contact"), body.get("billing_email"),
         body.get("billing_address"), body.get("payment_terms", "net30"), now, now),
    )
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_billing WHERE advertiser_id = %s", (advertiser_id,))
    return dict(row)


@router.put("/advertisers/{advertiser_id}/billing")
def update_billing(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    row = _fetch_one(conn, "SELECT * FROM tap_billing WHERE advertiser_id = %s", (advertiser_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Billing not found"}]})
    row = dict(row)
    cur = conn.cursor()
    cur.execute(
        "UPDATE tap_billing SET billing_contact=%s,billing_email=%s,billing_address=%s,"
        "payment_terms=%s,last_updated_time=%s WHERE advertiser_id=%s",
        (body.get("billing_contact", row["billing_contact"]),
         body.get("billing_email", row["billing_email"]),
         body.get("billing_address", row["billing_address"]),
         body.get("payment_terms", row["payment_terms"]), _now(), advertiser_id),
    )
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_billing WHERE advertiser_id = %s", (advertiser_id,))
    return dict(row)


@router.delete("/advertisers/{advertiser_id}/billing")
def delete_billing(advertiser_id: int, conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute("DELETE FROM tap_billing WHERE advertiser_id = %s", (advertiser_id,))
    conn.commit()
    return None


# ── Campaigns ────────────────────────────────────────────────────────────────

@router.get("/campaigns")
def search_campaigns(start: int = 0, limit: int = 1000, sort: Optional[str] = None,
                     name: Optional[str] = None, advertiser_id: Optional[int] = None,
                     status: Optional[str] = None, conn=Depends(get_db)):
    conditions, params = [], []
    if name:
        conditions.append("name ILIKE %s")
        params.append(f"%{name}%")
    if advertiser_id:
        conditions.append("advertiser_id = %s")
        params.append(advertiser_id)
    if status:
        conditions.append("status = %s")
        params.append(status)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    data, total = _paginate(conn, f"SELECT * FROM tap_campaigns{where}", tuple(params), start, limit, sort)
    return _search_response(data, total, start, limit)


@router.post("/campaigns")
def create_campaign(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    now = _now()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tap_campaigns (name,advertiser_id,status,external_id,notes,"
        "team_id,start_date,end_date,account_exec,trafficked_by,created_by,"
        "creation_time,last_updated_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (body.get("name"), body.get("advertiser_id"), body.get("status", "draft"),
         body.get("external_id"), body.get("notes"), body.get("team_id"),
         body.get("start_date"), body.get("end_date"), body.get("account_exec"),
         body.get("trafficked_by"), "api@triton.com", now, now),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_campaigns WHERE id = %s", (new_id,))
    return dict(row)


@router.get("/campaigns/{campaign_id}")
def get_campaign(campaign_id: int, conn=Depends(get_db)):
    row = _fetch_one(conn, "SELECT * FROM tap_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Campaign not found"}]})
    return dict(row)


@router.put("/campaigns/{campaign_id}")
def update_campaign(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    row = _fetch_one(conn, "SELECT * FROM tap_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Campaign not found"}]})
    row = dict(row)
    cur = conn.cursor()
    cur.execute(
        "UPDATE tap_campaigns SET name=%s,advertiser_id=%s,status=%s,external_id=%s,notes=%s,"
        "team_id=%s,start_date=%s,end_date=%s,account_exec=%s,trafficked_by=%s,last_updated_time=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("advertiser_id", row["advertiser_id"]),
         body.get("status", row["status"]), body.get("external_id", row["external_id"]),
         body.get("notes", row["notes"]), body.get("team_id", row["team_id"]),
         body.get("start_date", row["start_date"]), body.get("end_date", row["end_date"]),
         body.get("account_exec", row["account_exec"]), body.get("trafficked_by", row["trafficked_by"]),
         _now(), campaign_id),
    )
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_campaigns WHERE id = %s", (campaign_id,))
    return dict(row)


@router.patch("/campaigns/{campaign_id}")
def patch_campaign(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return update_campaign(campaign_id, body, conn)


@router.post("/campaigns/{campaign_id}/copy")
def copy_campaign(campaign_id: int, conn=Depends(get_db)):
    from app.database import _now
    now = _now()
    row = _fetch_one(conn, "SELECT * FROM tap_campaigns WHERE id = %s", (campaign_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Campaign not found"}]})
    d = dict(row)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tap_campaigns (name,advertiser_id,status,external_id,notes,"
        "team_id,start_date,end_date,account_exec,trafficked_by,created_by,"
        "creation_time,last_updated_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (f"{d['name']} (Copy)", d["advertiser_id"], "draft", None, d["notes"],
         d["team_id"], d["start_date"], d["end_date"], d["account_exec"],
         d["trafficked_by"], "api@triton.com", now, now),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    new_row = _fetch_one(conn, "SELECT * FROM tap_campaigns WHERE id = %s", (new_id,))
    return dict(new_row)


# ── Flights ──────────────────────────────────────────────────────────────────

@router.get("/flights")
def search_flights(start: int = 0, limit: int = 1000, sort: Optional[str] = None,
                   name: Optional[str] = None, campaign_id: Optional[int] = None,
                   advertiser_id: Optional[int] = None, status: Optional[str] = None,
                   delivery_method: Optional[str] = None, position: Optional[str] = None,
                   delivery_status: Optional[str] = None, conn=Depends(get_db)):
    conditions, params = [], []
    filters = [
        ("name ILIKE %s", name, f"%{name}%" if name else None),
        ("campaign_id = %s", campaign_id, campaign_id),
        ("advertiser_id = %s", advertiser_id, advertiser_id),
        ("status = %s", status, status),
        ("delivery_method = %s", delivery_method, delivery_method),
        ("position = %s", position, position),
        ("delivery_status = %s", delivery_status, delivery_status),
    ]
    for clause, val, param in filters:
        if val:
            conditions.append(clause)
            params.append(param)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    data, total = _paginate(conn, f"SELECT * FROM tap_flights{where}", tuple(params), start, limit, sort)
    return _search_response([_format_flight(d) for d in data], total, start, limit)


@router.post("/flights")
def create_flight(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    now = _now()
    goal = body.get("goal", {})
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tap_flights (name,campaign_id,advertiser_id,status,type,"
        "start_date,end_date,pricing_model,priority,position,delivery_method,"
        "pacing,run_on_network,open_measurement_enabled,goal_impressions,"
        "goal_spots,external_id,account_exec,trafficked_by,delivery_status,"
        "created_by,creation_time,last_updated_time) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (body.get("name"), body.get("campaign_id"), body.get("advertiser_id"),
         body.get("status", "draft"), body.get("type", "direct"),
         body.get("start_date"), body.get("end_date"),
         body.get("pricing_model", "cpm"), body.get("priority", 8),
         body.get("position", "preroll"), body.get("delivery_method", "live"),
         body.get("pacing", "even"),
         bool(body.get("run_on_network")), bool(body.get("open_measurement_enabled")),
         goal.get("impressions"), goal.get("spots"),
         body.get("external_id"), body.get("account_exec"), body.get("trafficked_by"),
         "pending", "api@triton.com", now, now),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_flights WHERE id = %s", (new_id,))
    return _format_flight(dict(row))


@router.get("/flights/{flight_id}")
def get_flight(flight_id: int, conn=Depends(get_db)):
    row = _fetch_one(conn, "SELECT * FROM tap_flights WHERE id = %s", (flight_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Flight not found"}]})
    return _format_flight(dict(row))


@router.put("/flights/{flight_id}")
def update_flight(flight_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    row = _fetch_one(conn, "SELECT * FROM tap_flights WHERE id = %s", (flight_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Flight not found"}]})
    row = dict(row)
    goal = body.get("goal", {})
    cur = conn.cursor()
    cur.execute(
        "UPDATE tap_flights SET name=%s,status=%s,type=%s,start_date=%s,end_date=%s,"
        "pricing_model=%s,priority=%s,position=%s,delivery_method=%s,pacing=%s,"
        "run_on_network=%s,open_measurement_enabled=%s,goal_impressions=%s,"
        "goal_spots=%s,external_id=%s,account_exec=%s,trafficked_by=%s,"
        "last_updated_time=%s WHERE id=%s",
        (body.get("name", row["name"]), body.get("status", row["status"]),
         body.get("type", row["type"]),
         body.get("start_date", row["start_date"]), body.get("end_date", row["end_date"]),
         body.get("pricing_model", row["pricing_model"]),
         body.get("priority", row["priority"]),
         body.get("position", row["position"]),
         body.get("delivery_method", row["delivery_method"]),
         body.get("pacing", row["pacing"]),
         bool(body.get("run_on_network", row["run_on_network"])),
         bool(body.get("open_measurement_enabled", row["open_measurement_enabled"])),
         goal.get("impressions", row["goal_impressions"]),
         goal.get("spots", row["goal_spots"]),
         body.get("external_id", row["external_id"]),
         body.get("account_exec", row["account_exec"]),
         body.get("trafficked_by", row["trafficked_by"]),
         _now(), flight_id),
    )
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_flights WHERE id = %s", (flight_id,))
    return _format_flight(dict(row))


@router.patch("/flights/{flight_id}")
def patch_flight(flight_id: int, body: dict = Body(...), conn=Depends(get_db)):
    return update_flight(flight_id, body, conn)


@router.post("/flights/{flight_id}/copy")
def copy_flight(flight_id: int, conn=Depends(get_db)):
    from app.database import _now
    now = _now()
    row = _fetch_one(conn, "SELECT * FROM tap_flights WHERE id = %s", (flight_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Flight not found"}]})
    d = dict(row)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tap_flights (name,campaign_id,advertiser_id,status,type,"
        "start_date,end_date,pricing_model,priority,position,delivery_method,"
        "pacing,run_on_network,open_measurement_enabled,goal_impressions,"
        "goal_spots,external_id,account_exec,trafficked_by,delivery_status,"
        "created_by,creation_time,last_updated_time) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (f"{d['name']} (Copy)", d["campaign_id"], d["advertiser_id"], "draft", d["type"],
         d["start_date"], d["end_date"], d["pricing_model"], d["priority"],
         d["position"], d["delivery_method"], d["pacing"],
         d["run_on_network"], d["open_measurement_enabled"],
         d["goal_impressions"], d["goal_spots"], None,
         d["account_exec"], d["trafficked_by"], "pending", "api@triton.com", now, now),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    new_row = _fetch_one(conn, "SELECT * FROM tap_flights WHERE id = %s", (new_id,))
    return _format_flight(dict(new_row))


# ── Creatives ────────────────────────────────────────────────────────────────

@router.get("/flights/{flight_id}/creative")
def get_creative(flight_id: int, conn=Depends(get_db)):
    row = _fetch_one(conn, "SELECT * FROM tap_creatives WHERE flight_id = %s", (flight_id,))
    if not row:
        return {}
    return dict(row)


@router.post("/flights/{flight_id}/creative")
def create_creative(flight_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    now = _now()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tap_creatives (flight_id,creative_url,ad_duration_in_seconds,"
        "status,overtone_id,deal_id,reseller_contract_id,creation_time,last_updated_time) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (flight_id, body.get("creative_url"), body.get("ad_duration_in_seconds"),
         "active", body.get("overtone_id"), body.get("deal_id"),
         body.get("reseller_contract_id"), now, now),
    )
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_creatives WHERE flight_id = %s", (flight_id,))
    return dict(row)


@router.put("/flights/{flight_id}/creative")
def update_creative(flight_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    row = _fetch_one(conn, "SELECT * FROM tap_creatives WHERE flight_id = %s", (flight_id,))
    if not row:
        raise HTTPException(404, {"status": 404, "errors": [{"message": "Creative not found"}]})
    row = dict(row)
    cur = conn.cursor()
    cur.execute(
        "UPDATE tap_creatives SET creative_url=%s,ad_duration_in_seconds=%s,status=%s,"
        "overtone_id=%s,deal_id=%s,reseller_contract_id=%s,last_updated_time=%s WHERE flight_id=%s",
        (body.get("creative_url", row["creative_url"]),
         body.get("ad_duration_in_seconds", row["ad_duration_in_seconds"]),
         body.get("status", row["status"]),
         body.get("overtone_id", row["overtone_id"]),
         body.get("deal_id", row["deal_id"]),
         body.get("reseller_contract_id", row["reseller_contract_id"]),
         _now(), flight_id),
    )
    conn.commit()
    row = _fetch_one(conn, "SELECT * FROM tap_creatives WHERE flight_id = %s", (flight_id,))
    return dict(row)


@router.delete("/flights/{flight_id}/creative")
def delete_creative(flight_id: int, conn=Depends(get_db)):
    cur = conn.cursor()
    cur.execute("DELETE FROM tap_creatives WHERE flight_id = %s", (flight_id,))
    conn.commit()
    return None


# ── Cappings ─────────────────────────────────────────────────────────────────

@router.get("/flights/cappings")
def search_cappings(start: int = 0, limit: int = 1000, sort: Optional[str] = None,
                    flight_id: Optional[int] = None, conn=Depends(get_db)):
    conditions, params = [], []
    if flight_id:
        conditions.append("flight_id = %s")
        params.append(flight_id)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    data, total = _paginate(conn, f"SELECT * FROM tap_cappings{where}", tuple(params), start, limit, sort)
    return _search_response(data, total, start, limit)


# ── Contending Metadata ──────────────────────────────────────────────────────

@router.get("/contending")
def get_contending(id: Optional[str] = None, conn=Depends(get_db)):
    cur = conn.cursor()
    conditions, params = [], []
    if id:
        flight_ids = [int(x.strip()) for x in id.split(",")]
        placeholders = ",".join(["%s"] * len(flight_ids))
        conditions.append(f"f.id IN ({placeholders})")
        params.extend(flight_ids)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    cur.execute(
        f"SELECT f.*, c.name as campaign_name, a.name as advertiser_name "
        f"FROM tap_flights f "
        f"JOIN tap_campaigns c ON f.campaign_id = c.id "
        f"JOIN tap_advertisers a ON f.advertiser_id = a.id {where}",
        params,
    )
    rows = cur.fetchall()
    data = []
    for row in rows:
        d = dict(row)
        data.append({
            "source": "booking_api",
            "flight": {
                "id": d["id"], "name": d["name"], "status": d["status"],
                "start_date": d["start_date"], "end_date": d["end_date"],
                "goal": {"impressions": d["goal_impressions"], "spots": d["goal_spots"]},
                "priority": d["priority"],
            },
            "campaign": {"id": d["campaign_id"], "name": d["campaign_name"]},
            "advertiser": {"id": d["advertiser_id"], "name": d["advertiser_name"]},
            "delivery": {
                "impressions": d.get("goal_impressions", 0),
                "spots": d.get("goal_spots"),
                "on_track_indicator": "on_track",
            },
        })
    return {"data": data}
