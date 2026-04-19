"""Triton Digital Metrics API mock endpoints under /triton."""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from app.database import get_db

router = APIRouter(prefix="/triton")


def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


# ── Healthcheck ──────────────────────────────────────────────────────────────

@router.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}


# ── Reports ──────────────────────────────────────────────────────────────────

@router.get("/reports")
def list_reports(conn=Depends(get_db)):
    """List all saved reports (not part of official API — added for discoverability)."""
    rows = _q(conn, "SELECT * FROM triton_reports ORDER BY created_at DESC").fetchall()
    return {"reports": [dict(r) for r in rows]}


@router.get("/reports/{report_id}")
def get_report(report_id: str,
               start: Optional[str] = Query(None, description="Start date override (YYYY-MM-DD)"),
               end: Optional[str] = Query(None, description="End date override (YYYY-MM-DD)"),
               conn=Depends(get_db)):
    """Get report data by saved query ID, optionally filtering by date range."""
    report = _q(conn, "SELECT * FROM triton_reports WHERE id = %s", (report_id,)).fetchone()
    if not report:
        raise HTTPException(
            status_code=404,
            detail={"status": 404, "errors": [{"message": f"Report not found: {report_id}"}]},
        )

    sql = "SELECT * FROM triton_report_data WHERE report_id = %s"
    params = [report_id]

    if start:
        sql += " AND date >= %s"
        params.append(start)
    if end:
        sql += " AND date <= %s"
        params.append(end)

    sql += " ORDER BY date, station_name"
    rows = _q(conn, sql, params).fetchall()

    # Format response to match Triton's nested array-of-arrays structure
    data = []
    for row in rows:
        row_dict = dict(row)
        row_dict.pop("id", None)
        row_dict.pop("report_id", None)
        row_props = [
            {"name": key, "value": value, "exportValue": value}
            for key, value in row_dict.items()
        ]
        data.append(row_props)

    return {
        "data": data,
        "length": len(data),
        "updatedAt": dict(report)["updated_at"],
    }
