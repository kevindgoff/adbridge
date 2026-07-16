"""
Radio Workflow Partner API mock endpoints under /radioworkflow.

Base URL: https://api.radioworkflow.com
Auth: OAuth2 Bearer token (mocked via AdBridge X-API-Key pattern)

Radio Workflow is an AI-powered traffic, billing, and production platform
for radio stations. This mock covers the Partner API surface including
stations, accounts, talent, production orders, logs, ad bank, and more.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.database import get_db

router = APIRouter(prefix="/radioworkflow")


def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _rw_response(data, status=200):
    """Standard Radio Workflow response envelope."""
    return {"status": status, "data": data}


def _rw_single(conn, table, id_value, id_col="core_id"):
    """Fetch a single row by ID and return in RW envelope, or 404."""
    row = _q(conn, f"SELECT * FROM {table} WHERE {id_col} = %s", (id_value,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail={"status": 404, "error": "Not found"})
    return _rw_response(dict(row))


# ═══════════════════════════════════════════════════════════════════════════════
# STATIONS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/stations")
def list_stations(conn=Depends(get_db)):
    """List all stations configured in the instance."""
    rows = _q(conn, "SELECT * FROM rw_stations ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


@router.get("/stations/{station_id}")
def get_station(station_id: int, conn=Depends(get_db)):
    """Get a single station by ID."""
    return _rw_single(conn, "rw_stations", station_id)


# ═══════════════════════════════════════════════════════════════════════════════
# ACCOUNTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/accounts")
def list_accounts(
    q: Optional[str] = Query(None, description="Search accounts by name"),
    conn=Depends(get_db),
):
    """List advertiser/agency accounts."""
    if q:
        rows = _q(
            conn,
            "SELECT * FROM rw_accounts WHERE description ILIKE %s ORDER BY core_id",
            (f"%{q}%",),
        ).fetchall()
    else:
        rows = _q(conn, "SELECT * FROM rw_accounts ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


@router.get("/accounts/{account_id}")
def get_account(account_id: int, conn=Depends(get_db)):
    """Get a single account by ID."""
    return _rw_single(conn, "rw_accounts", account_id)


# ═══════════════════════════════════════════════════════════════════════════════
# TALENT
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/talent")
def list_talent(conn=Depends(get_db)):
    """List all production talent."""
    rows = _q(conn, "SELECT * FROM rw_talent ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


@router.get("/talent/{talent_id}")
def get_talent(talent_id: int, conn=Depends(get_db)):
    """Get a single talent entry by ID."""
    return _rw_single(conn, "rw_talent", talent_id)


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCTION TYPES
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/prodTypes")
def list_prod_types(conn=Depends(get_db)):
    """List all production types (New Spot, Extend Spot, Live Liner, etc.)."""
    rows = _q(conn, "SELECT * FROM rw_prod_types ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCTION ORDERS (primary focus)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/production")
def list_production(
    status: Optional[str] = Query(None, description="Filter by status (pending, in_progress, completed, cancelled)"),
    station_id: Optional[int] = Query(None, description="Filter by station ID"),
    account_id: Optional[int] = Query(None, description="Filter by account ID"),
    include_instructions: Optional[int] = Query(0, description="1 to include instruction set from traffic order"),
    conn=Depends(get_db),
):
    """List production orders. Filterable by status, station, account."""
    sql = "SELECT * FROM rw_production WHERE 1=1"
    params = []

    if status:
        sql += " AND status = %s"
        params.append(status)
    if station_id:
        sql += " AND station_id = %s"
        params.append(station_id)
    if account_id:
        sql += " AND account_id = %s"
        params.append(account_id)

    sql += " ORDER BY core_id DESC"
    rows = _q(conn, sql, params).fetchall()

    results = [dict(r) for r in rows]

    # If include_instructions=1, attach instructions for each production order
    if include_instructions == 1:
        for prod in results:
            instr_rows = _q(
                conn,
                "SELECT * FROM rw_instructions WHERE production_id = %s ORDER BY line_number",
                (prod["core_id"],),
            ).fetchall()
            prod["instructions"] = [dict(ir) for ir in instr_rows]

    return _rw_response(results)


@router.get("/production/{production_id}")
def get_production(
    production_id: int,
    include_instructions: Optional[int] = Query(0, description="1 to include instruction set"),
    conn=Depends(get_db),
):
    """Get a single production order by ID."""
    row = _q(conn, "SELECT * FROM rw_production WHERE core_id = %s", (production_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail={"status": 404, "error": "Production order not found"})

    result = dict(row)

    if include_instructions == 1:
        instr_rows = _q(
            conn,
            "SELECT * FROM rw_instructions WHERE production_id = %s ORDER BY line_number",
            (production_id,),
        ).fetchall()
        result["instructions"] = [dict(ir) for ir in instr_rows]

    return _rw_response(result)


# ═══════════════════════════════════════════════════════════════════════════════
# INSTRUCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/instructions")
def list_instructions(
    production_id: Optional[int] = Query(None, description="Filter by production order ID"),
    conn=Depends(get_db),
):
    """List traffic instruction lines."""
    if production_id:
        rows = _q(
            conn,
            "SELECT * FROM rw_instructions WHERE production_id = %s ORDER BY line_number",
            (production_id,),
        ).fetchall()
    else:
        rows = _q(conn, "SELECT * FROM rw_instructions ORDER BY production_id, line_number").fetchall()
    return _rw_response([dict(r) for r in rows])


@router.get("/instructions/{instruction_id}")
def get_instruction(instruction_id: int, conn=Depends(get_db)):
    """Get a specific instruction line."""
    return _rw_single(conn, "rw_instructions", instruction_id)


# ═══════════════════════════════════════════════════════════════════════════════
# LOGS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/logs")
def list_logs(
    station_id: Optional[int] = Query(None, description="Filter by station"),
    status: Optional[int] = Query(None, description="0=draft, 1=locked/not reconciled, 2=locked/reconciled"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    conn=Depends(get_db),
):
    """List daily logs. Filterable by station, status, and date range."""
    sql = "SELECT * FROM rw_logs WHERE 1=1"
    params = []

    if station_id:
        sql += " AND station_id = %s"
        params.append(station_id)
    if status is not None:
        sql += " AND status = %s"
        params.append(status)
    if date_from:
        sql += " AND log_date >= %s"
        params.append(date_from)
    if date_to:
        sql += " AND log_date <= %s"
        params.append(date_to)

    sql += " ORDER BY log_date DESC, station_id"
    rows = _q(conn, sql, params).fetchall()
    return _rw_response([dict(r) for r in rows])


@router.get("/logs/{log_id}")
def get_log(log_id: int, conn=Depends(get_db)):
    """Get a single log entry by ID."""
    return _rw_single(conn, "rw_logs", log_id)


# ═══════════════════════════════════════════════════════════════════════════════
# DAYS LOGS (day-specific log entries with spot detail)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/daysLogs")
def list_days_logs(
    log_id: Optional[int] = Query(None, description="Filter by parent log ID"),
    station_id: Optional[int] = Query(None, description="Filter by station"),
    date: Optional[str] = Query(None, description="Filter by specific date (YYYY-MM-DD)"),
    conn=Depends(get_db),
):
    """List day-specific log entries (individual spot plays)."""
    sql = "SELECT * FROM rw_days_logs WHERE 1=1"
    params = []

    if log_id:
        sql += " AND log_id = %s"
        params.append(log_id)
    if station_id:
        sql += " AND station_id = %s"
        params.append(station_id)
    if date:
        sql += " AND air_date = %s"
        params.append(date)

    sql += " ORDER BY air_date, scheduled_time"
    rows = _q(conn, sql, params).fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# LOG EXPORT
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/logExport/{automation_system_id}")
def log_export(
    automation_system_id: int,
    station_id: Optional[int] = Query(None, description="Station to export for"),
    date: Optional[str] = Query(None, description="Date to export (YYYY-MM-DD)"),
    conn=Depends(get_db),
):
    """Export a log formatted for a specific automation system.

    Refer to /automationSystems for the list of supported system IDs.
    Notable: 51 = WideOrbit Radio Automation.
    """
    # Validate automation system exists
    sys_row = _q(
        conn, "SELECT * FROM rw_automation_systems WHERE system_id = %s", (automation_system_id,)
    ).fetchone()
    if not sys_row:
        raise HTTPException(
            status_code=404,
            detail={"status": 404, "error": f"Automation system {automation_system_id} not found"},
        )

    # Get log entries for export
    sql = "SELECT dl.* FROM rw_days_logs dl WHERE 1=1"
    params = []
    if station_id:
        sql += " AND dl.station_id = %s"
        params.append(station_id)
    if date:
        sql += " AND dl.air_date = %s"
        params.append(date)
    sql += " ORDER BY dl.scheduled_time"

    rows = _q(conn, sql, params).fetchall()

    # Format output as JSON log export
    export_entries = []
    for r in rows:
        entry = dict(r)
        entry["automationSystemId"] = automation_system_id
        entry["automationSystemName"] = dict(sys_row)["name"]
        entry["exportFormat"] = "json"
        export_entries.append(entry)

    return _rw_response(export_entries)


# ═══════════════════════════════════════════════════════════════════════════════
# CLOCK / LOG TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/clock/{clock_id}")
def get_clock(clock_id: int, conn=Depends(get_db)):
    """Get a clock (log template) by ID, including its break/unit structure."""
    clock = _q(conn, "SELECT * FROM rw_clocks WHERE core_id = %s", (clock_id,)).fetchone()
    if not clock:
        raise HTTPException(status_code=404, detail={"status": 404, "error": "Clock not found"})

    # Get breaks for this clock
    breaks = _q(
        conn, "SELECT * FROM rw_clock_breaks WHERE clock_id = %s ORDER BY position", (clock_id,)
    ).fetchall()

    result = dict(clock)
    result["breaks"] = [dict(b) for b in breaks]
    return _rw_response(result)


# ═══════════════════════════════════════════════════════════════════════════════
# AD BANK (Spot Inventory)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/getAdBank")
def get_ad_bank(
    created_start: Optional[str] = Query(None, description="Spots created after this date (YYYY-MM-DD)"),
    created_end: Optional[str] = Query(None, description="Spots created before this date (YYYY-MM-DD)"),
    cart_id: Optional[str] = Query(None, description="Filter by cart ID"),
    isci: Optional[str] = Query(None, description="Filter by ISCI code"),
    station_id: Optional[int] = Query(None, description="Filter by station"),
    conn=Depends(get_db),
):
    """Get spot inventory from the ad bank. Filterable by date, cart, ISCI, station."""
    sql = "SELECT * FROM rw_ad_bank WHERE 1=1"
    params = []

    if created_start:
        sql += " AND created_date >= %s"
        params.append(created_start)
    if created_end:
        sql += " AND created_date <= %s"
        params.append(created_end)
    if cart_id:
        sql += " AND cart_id = %s"
        params.append(cart_id)
    if isci:
        sql += " AND isci_code = %s"
        params.append(isci)
    if station_id:
        sql += " AND station_id = %s"
        params.append(station_id)

    sql += " ORDER BY created_date DESC"
    rows = _q(conn, sql, params).fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# NETWORK DUBS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/networkDubs")
def list_network_dubs(
    first_seen_start: Optional[str] = Query(None, description="New material first seen after (YYYY-MM-DD)"),
    first_seen_end: Optional[str] = Query(None, description="New material first seen until (YYYY-MM-DD)"),
    cart_id: Optional[str] = Query(None, description="Filter by cart ID (network spots only)"),
    title: Optional[str] = Query(None, description="Filter by title/ISCI code (network spots only)"),
    conn=Depends(get_db),
):
    """Get spot information for network dubs (network spots only)."""
    sql = "SELECT * FROM rw_network_dubs WHERE 1=1"
    params = []

    if first_seen_start:
        sql += " AND first_seen_date >= %s"
        params.append(first_seen_start)
    if first_seen_end:
        sql += " AND first_seen_date <= %s"
        params.append(first_seen_end)
    if cart_id:
        sql += " AND cart_id = %s"
        params.append(cart_id)
    if title:
        sql += " AND title ILIKE %s"
        params.append(f"%{title}%")

    sql += " ORDER BY first_seen_date DESC"
    rows = _q(conn, sql, params).fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# DISPATCH MEDIA
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/dispatchMedia")
def dispatch_media(
    station: str = Query(..., description="Station call letters or ID"),
    country: str = Query("US", description="Country code"),
    description: Optional[str] = Query(None, description="ISCI code, Key #, or short description"),
    conn=Depends(get_db),
):
    """Mock media dispatch — simulates sending audio to a station.

    In production this accepts a multipart file upload. The mock accepts
    metadata only and returns a success acknowledgment.
    """
    return _rw_response({
        "dispatched": True,
        "station": station,
        "country": country,
        "description": description,
        "message": "Media file accepted for delivery (mock)",
    })


# ═══════════════════════════════════════════════════════════════════════════════
# REVENUE TYPES
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/revenueTypes")
def list_revenue_types(conn=Depends(get_db)):
    """List revenue types with commission configuration."""
    rows = _q(conn, "SELECT * FROM rw_revenue_types ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# AVAIL TYPES
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/availTypes")
def list_avail_types(conn=Depends(get_db)):
    """List avail types (containers/buckets for spot types)."""
    rows = _q(conn, "SELECT * FROM rw_avail_types ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# SPOT TYPES
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/spotTypes")
def list_spot_types(conn=Depends(get_db)):
    """List spot types (categorization of commercials/content)."""
    rows = _q(conn, "SELECT * FROM rw_spot_types ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# DAYPARTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/dayparts")
def list_dayparts(conn=Depends(get_db)):
    """List daypart definitions (e.g., AM DRIVE 6-10, PM DRIVE 3-7)."""
    rows = _q(conn, "SELECT * FROM rw_dayparts ORDER BY core_id").fetchall()
    return _rw_response([dict(r) for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# SPOT LENGTHS
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/spotLengths")
def list_spot_lengths(conn=Depends(get_db)):
    """List allowed spot lengths (in seconds)."""
    rows = _q(conn, "SELECT * FROM rw_spot_lengths ORDER BY length_seconds").fetchall()
    return _rw_response([r["length_seconds"] for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
# AUTOMATION SYSTEMS (reference data for logExport)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/automationSystems")
def list_automation_systems(conn=Depends(get_db)):
    """List supported automation systems for log export."""
    rows = _q(conn, "SELECT * FROM rw_automation_systems ORDER BY system_id").fetchall()
    return _rw_response([dict(r) for r in rows])
