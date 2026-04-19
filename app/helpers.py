"""Shared helpers for pagination and response formatting."""

from typing import Optional


def paginate(conn, table: str, cursor: Optional[str], page_size: int = 25,
             query: Optional[str] = None, query_columns: Optional[list] = None,
             where_clause: str = "", where_params: tuple = (),
             order_by: str = "id"):
    """Generic cursor-based pagination over a table."""
    cur = conn.cursor()
    params = list(where_params)
    conditions = []

    if where_clause:
        conditions.append(where_clause)

    if query and query_columns:
        like_clauses = " OR ".join(f"{col} ILIKE %s" for col in query_columns)
        conditions.append(f"({like_clauses})")
        params.extend([f"%{query}%"] * len(query_columns))

    if cursor:
        conditions.append(f"{order_by} > %s")
        params.append(cursor)

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    cur.execute(f"SELECT COUNT(*) FROM {table} {where}", params)
    total = cur.fetchone()["count"]

    cur.execute(f"SELECT * FROM {table} {where} ORDER BY {order_by} LIMIT %s", params + [page_size])
    rows = cur.fetchall()

    next_cursor = None
    if len(rows) == page_size:
        last = rows[-1]
        next_cursor = str(last[order_by])

    metadata = {
        "cursor": next_cursor,
        "page_size": page_size,
        "total": total,
    }

    return [dict(r) for r in rows], metadata


def list_response(data: list, metadata: dict, included: dict = None):
    resp = {"metadata": metadata, "data": data}
    if included:
        resp["included"] = included
    return resp


def single_response(data: dict, included: dict = None):
    resp = {"data": data}
    if included:
        resp["included"] = included
    return resp
