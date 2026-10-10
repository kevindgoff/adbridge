"""Freewheel (api.freewheel.tv) mock API endpoints under /freewheel.

Spec-faithful rebuild from the authoritative OpenAPI specs in /tmp/fw-specs/*.json
(campaign/advertiser/agency/brand V3, insertion-order V3, placement V3, reporting,
creative-instance-metrics V4, marketplace V4, RFP/proposed/programmatic V4).

The real server base is https://api.freewheel.tv. Every real sub-path is preserved
VERBATIM under the AdBridge prefix /freewheel:
    real /services/v3/campaign       -> mock /freewheel/services/v3/campaign
    real /reporting/v1/job/{id}      -> mock /freewheel/reporting/v1/job/{id}

RESPONSE FORMAT (XML / JSON content negotiation):
  The authoritative specs declare the response media type per operation. The
  v3 /services/v3 surface is predominantly `application/xml`; a subset of v4
  operations (creative-instance metrics, proposed IO/placement/ad) declare BOTH
  application/json AND application/xml; the rest declare application/json or
  `*/*` (unpinned). This mock mirrors that faithfully via a custom route class
  (_ContentNegotiatedRoute) driven by the per-operation map _XML_DEFAULT_OPS /
  _DUAL_OPS below:
    * xml-only operations  -> serve XML unless the client sends Accept: application/json
    * json+xml operations  -> serve JSON unless the client sends Accept: application/xml
    * everything else      -> JSON
  Handlers always build and return plain dicts (so the JSON path and the
  generated OpenAPI schema are unchanged); the route class transcodes the
  serialized body to XML only when negotiation selects it. XML uses a
  deterministic convention (dict keys -> elements, lists -> repeated singularised
  item elements), since the specs carry no OpenAPI `xml` element metadata.

Documented divergences from the real API (see platform_api_sources.yml):
  * AUTH: real Freewheel uses session-token auth; this mock normalises to the
    repo-wide X-API-Key gate (app/main.py), like every other AdBridge mock.
  * XML SHAPE: real Freewheel's exact XML element/attribute layout is not pinned
    in the specs (no OpenAPI `xml` objects); this mock emits a conventional
    field-mirrors-element XML, not byte-identical to production XML.
  * ASYNC REPORTING: the audience reporting GETs return a real HTTP 202 + job
    handle; the job is marked COMPLETED immediately so the first poll of
    GET /reporting/v1/job/{job_id} returns 200 with rows. Real Freewheel runs the
    job asynchronously and the first poll may return a still-pending status.
    Reporting responses are JSON (the reporting spec declares application/json).
"""

import json
import uuid
from typing import Optional
from xml.sax.saxutils import escape as _xml_escape

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from app.database import get_db

router = APIRouter(prefix="/freewheel")


# ── Content negotiation (XML / JSON) ────────────────────────────────────────
#
# The authoritative Freewheel OpenAPI specs declare the response media type per
# operation. The v3 /services/v3 surface is predominantly `application/xml` on
# reads and several writes; a subset of v4 operations (creative-instance metrics,
# proposed IO/placement/ad) declare BOTH application/json AND application/xml.
# The remaining v4 operations declare application/json or `*/*` (unpinned).
#
# This mock honours that per-operation default while remaining content-negotiable:
#   * default="xml"  -> serve XML unless the client sends `Accept: application/json`
#   * default="json" -> serve JSON unless the client sends `Accept: application/xml`
# so a client written against real Freewheel's v3 XML works out of the box, and a
# JSON-only test harness can still force JSON with an Accept header.
#
# The specs carry no OpenAPI `xml` object metadata (no custom element names or
# wrapping hints), so XML is emitted with a deterministic convention: dict keys
# become child elements, lists repeat a singularised item element, and scalars are
# text. This matches real Freewheel's element-mirrors-field XML shape.

_XML_DECL = '<?xml version="1.0" encoding="UTF-8"?>'

# Authoritative per-operation response media types, derived from the specs.
# (METHOD, path-suffix-after-/freewheel). XML-only ops default to XML; dual ops
# default to JSON and switch to XML on Accept: application/xml. The IO dual-path
# (insertion_order vs insertion_orders) is normalised below so both forms match.
_XML_DEFAULT_OPS = {
    ("DELETE", "/services/v3/advertisers/{advertiser_id}/brands/{brand_id}"),
    ("GET", "/services/v3/advertisers"),
    ("GET", "/services/v3/advertisers/{advertiser_id}/brands"),
    ("GET", "/services/v3/advertisers/{advertiser_id}/brands/{brand_id}"),
    ("GET", "/services/v3/advertisers/{advertiser_id}/parent_agencies"),
    ("GET", "/services/v3/agencies"),
    ("GET", "/services/v3/agencies/{agency_id}"),
    ("GET", "/services/v3/agencies/{agency_id}/child_advertisers"),
    ("GET", "/services/v3/agencies/{agency_id}/child_agencies"),
    ("GET", "/services/v3/agencies/{agency_id}/parent_agencies"),
    ("GET", "/services/v3/campaign/{campaign_id}"),
    ("GET", "/services/v3/campaign/{campaign_id}/insertion_orders"),
    ("GET", "/services/v3/campaigns"),
    ("GET", "/services/v3/insertion_order/{insertion_order_id}/placements"),
    ("GET", "/services/v3/insertion_orders"),
    ("GET", "/services/v3/placement/{placement_id}"),
    ("GET", "/services/v3/placements"),
    ("POST", "/services/v3/advertisers"),
    ("POST", "/services/v3/advertisers/{advertiser_id}/brands"),
    ("POST", "/services/v3/campaign/{campaign_id}/insertion_order"),
    ("PUT", "/services/v3/advertisers/{advertiser_id}"),
    ("PUT", "/services/v3/advertisers/{advertiser_id}/activate"),
    ("PUT", "/services/v3/advertisers/{advertiser_id}/brands/{brand_id}"),
    ("PUT", "/services/v3/advertisers/{advertiser_id}/deactivate"),
    ("PUT", "/services/v3/agencies/{agency_id}"),
    ("PUT", "/services/v3/agencies/{agency_id}/activate_relationships"),
    ("PUT", "/services/v3/agencies/{agency_id}/add_relationships"),
    ("PUT", "/services/v3/agencies/{agency_id}/deactivate_relationships"),
    # IO update: spec path is insertion_order(s); both singular/plural map here.
    ("PUT", "/services/v3/insertion_order/{insertion_order_id}"),
    ("PUT", "/services/v3/insertion_orders/{insertion_order_id}"),
    ("PUT", "/services/v3/insertion_order/{insertion_order_id}/unbook"),
    ("PUT", "/services/v3/placement/{placement_id}"),
    ("PUT", "/services/v3/placement/{placement_id}/activate"),
    ("PUT", "/services/v3/placement/{placement_id}/cancel"),
    ("PUT", "/services/v3/placement/{placement_id}/deactivate"),
    ("PUT", "/services/v3/placement/{placement_id}/extend"),
}

_DUAL_OPS = {
    ("GET", "/services/v4/campaigns/{campaign_id}/proposed_insertion_orders"),
    ("GET", "/services/v4/proposed_insertion_orders"),
    ("GET", "/services/v4/proposed_insertion_orders/{proposed_io_id}"),
    ("GET", "/services/v4/proposed_insertion_orders/{proposed_io_id}/proposed_placements"),
    ("GET", "/services/v4/proposed_placements"),
    ("GET", "/services/v4/proposed_placements/{proposed_placement_id}"),
    ("PATCH", "/services/v4/proposed_ads/{proposed_ad_id}"),
    ("PATCH", "/services/v4/proposed_insertion_orders/{proposed_io_id}"),
    ("PATCH", "/services/v4/proposed_placements/{proposed_placement_id}"),
    ("POST", "/services/v4/campaigns/{campaign_id}/proposed_insertion_orders"),
    ("POST", "/services/v4/creative_instances/{creative_instance_id}/creative_metrics"),
    ("POST", "/services/v4/proposed_insertion_orders/{proposed_io_id}/proposed_placements"),
    ("POST", "/services/v4/proposed_placements/{proposed_placement_id}/proposed_ads"),
    ("PUT", "/services/v3/advertisers/{advertiser_id}/add_relationships"),
    ("PUT", "/services/v4/creative_instances/{creative_instance_id}/creative_metrics/{creative_metric_id}"),
}

# Root element name per top-level path segment, for nicer XML roots.
_XML_ROOT = "response"


def _singular(name: str) -> str:
    if name.endswith("ies"):
        return name[:-3] + "y"
    if name.endswith("s") and not name.endswith("ss"):
        return name[:-1]
    return name


def _xml_el(tag: str, value) -> str:
    """Serialise one value as an XML element (recursive)."""
    tag = tag or "item"
    if value is None:
        return f"<{tag}/>"
    if isinstance(value, bool):
        return f"<{tag}>{'true' if value else 'false'}</{tag}>"
    if isinstance(value, dict):
        inner = "".join(_xml_el(k, v) for k, v in value.items())
        return f"<{tag}>{inner}</{tag}>"
    if isinstance(value, (list, tuple)):
        item_tag = _singular(tag)
        inner = "".join(_xml_el(item_tag, v) for v in value)
        return f"<{tag}>{inner}</{tag}>"
    return f"<{tag}>{_xml_escape(str(value))}</{tag}>"


def _to_xml(data, root: str = _XML_ROOT) -> str:
    """Render a dict/list payload to an XML document string."""
    if isinstance(data, dict):
        body = "".join(_xml_el(k, v) for k, v in data.items())
    elif isinstance(data, (list, tuple)):
        body = "".join(_xml_el(_singular(root), v) for v in data)
    else:
        body = _xml_escape(str(data))
    return f"{_XML_DECL}<{root}>{body}</{root}>"


def _default_format(method: str, path_suffix: str) -> Optional[str]:
    """Return 'xml' | 'json' | None(=unpinned/json) for an operation."""
    key = (method.upper(), path_suffix)
    if key in _XML_DEFAULT_OPS:
        return "xml"
    if key in _DUAL_OPS:
        return "json"  # negotiable to xml via Accept
    return None


def _is_xml_capable(method: str, path_suffix: str) -> bool:
    key = (method.upper(), path_suffix)
    return key in _XML_DEFAULT_OPS or key in _DUAL_OPS


def _wants_xml(accept: str, default: Optional[str]) -> bool:
    accept = (accept or "").lower()
    if "application/xml" in accept or "text/xml" in accept:
        return True
    if "application/json" in accept:
        return False
    return default == "xml"


class _ContentNegotiatedRoute(APIRoute):
    """Route class that transcodes a JSON response to XML per the spec's declared
    media types and the request's Accept header. Handlers keep returning plain
    dicts; only the serialized bytes change when XML is negotiated. Non-2xx
    responses (404 errors) and the 202 reporting handshakes are left as JSON."""

    def get_route_handler(self):
        original = super().get_route_handler()
        # path without the /freewheel prefix -> matches the spec maps
        suffix = self.path[len("/freewheel"):] if self.path.startswith("/freewheel") else self.path
        methods = self.methods or set()

        async def custom(request: Request):
            response = await original(request)
            # Only transcode successful JSON bodies for XML-capable operations.
            method = request.method.upper()
            if method not in methods:
                return response
            if not _is_xml_capable(method, suffix):
                return response
            if not (200 <= response.status_code < 300):
                return response
            media = (getattr(response, "media_type", "") or "")
            if "json" not in media:
                return response
            default = _default_format(method, suffix)
            if not _wants_xml(request.headers.get("accept", ""), default):
                return response
            try:
                payload = json.loads(bytes(response.body))
            except Exception:
                return response
            xml = _to_xml(payload, root=_XML_ROOT)
            return Response(content=xml, media_type="application/xml",
                            status_code=response.status_code)

        return custom


router = APIRouter(prefix="/freewheel", route_class=_ContentNegotiatedRoute)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _next_id(conn, table, pk):
    row = _q(conn, f"SELECT MAX({pk}) AS max_id FROM {table}").fetchone()
    return (row["max_id"] or 0) + 1


def _apply_updates(conn, table, pk, pk_val, body, allowed):
    updates = {k: v for k, v in body.items() if k in allowed}
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE {table} SET {set_clause} WHERE {pk} = %s",
           (*updates.values(), pk_val))
        conn.commit()


# ═══════════════════════════════════════════════════════════════════════════
# CAPABILITY 1: CAMPAIGN MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

# ── Campaign API V3 ────────────────────────────────────────────────────────

@router.post("/services/v3/campaign")
def create_campaign(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    cid = _next_id(conn, "fw_campaigns", "campaign_id")
    _q(conn,
        "INSERT INTO fw_campaigns (campaign_id, name, description, advertiser_id, agency_id, "
        "external_id, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (cid, body.get("name", "New Campaign"), body.get("description"),
         body.get("advertiser_id"), body.get("agency_id"), body.get("external_id"),
         "draft", _now(), _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_campaigns WHERE campaign_id = %s", (cid,)).fetchone())


@router.get("/services/v3/campaigns")
def list_campaigns(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_campaigns ORDER BY campaign_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v3/campaign/{campaign_id}")
def get_campaign(campaign_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_campaigns WHERE campaign_id = %s", (campaign_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    return dict(row)


@router.put("/services/v3/campaign/{campaign_id}")
def update_campaign(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_campaigns WHERE campaign_id = %s", (campaign_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    _apply_updates(conn, "fw_campaigns", "campaign_id", campaign_id, body,
                   {"name", "description", "advertiser_id", "agency_id", "external_id", "status"})
    return dict(_q(conn, "SELECT * FROM fw_campaigns WHERE campaign_id = %s", (campaign_id,)).fetchone())


@router.delete("/services/v3/campaign/{campaign_id}")
def delete_campaign(campaign_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_campaigns WHERE campaign_id = %s", (campaign_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    _q(conn, "DELETE FROM fw_campaigns WHERE campaign_id = %s", (campaign_id,))
    conn.commit()
    return {"deleted": True, "campaign_id": campaign_id}


@router.get("/services/v3/campaign/{campaign_id}/insertion_orders")
def list_campaign_insertion_orders(campaign_id: int, conn=Depends(get_db)):
    campaign = _q(conn, "SELECT 1 FROM fw_campaigns WHERE campaign_id = %s", (campaign_id,)).fetchone()
    if not campaign:
        raise HTTPException(404, "Campaign not found")
    rows = _q(conn,
        "SELECT * FROM fw_insertion_orders WHERE campaign_id = %s ORDER BY insertion_order_id",
        (campaign_id,)).fetchall()
    return {"items": [dict(r) for r in rows]}


# ── Advertiser API V3 ──────────────────────────────────────────────────────

@router.get("/services/v3/advertisers")
def list_advertisers(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_advertisers ORDER BY advertiser_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.post("/services/v3/advertisers")
def create_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    adv_id = _next_id(conn, "fw_advertisers", "advertiser_id")
    _q(conn,
        "INSERT INTO fw_advertisers (advertiser_id, name, external_id, agency_id, status, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (adv_id, body.get("name", "New Advertiser"), body.get("external_id"),
         body.get("agency_id"), "active", _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (adv_id,)).fetchone())


@router.get("/services/v3/advertisers/{advertiser_id}")
def get_advertiser(advertiser_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    return dict(row)


@router.put("/services/v3/advertisers/{advertiser_id}")
def update_advertiser(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    _apply_updates(conn, "fw_advertisers", "advertiser_id", advertiser_id, body,
                   {"name", "external_id", "agency_id", "status"})
    return dict(_q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone())


def _set_advertiser_status(advertiser_id, status, conn):
    row = _q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    _q(conn, "UPDATE fw_advertisers SET status = %s WHERE advertiser_id = %s", (status, advertiser_id))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone())


@router.put("/services/v3/advertisers/{advertiser_id}/activate")
def activate_advertiser(advertiser_id: int, conn=Depends(get_db)):
    return _set_advertiser_status(advertiser_id, "active", conn)


@router.put("/services/v3/advertisers/{advertiser_id}/deactivate")
def deactivate_advertiser(advertiser_id: int, conn=Depends(get_db)):
    return _set_advertiser_status(advertiser_id, "inactive", conn)


def _advertiser_relationships(advertiser_id, body, status, conn):
    row = _q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    related = (body or {}).get("agency_ids") or (body or {}).get("relationships") or []
    for parent_id in related:
        _q(conn,
            "INSERT INTO fw_relationships (parent_type, parent_id, child_type, child_id, status) "
            "VALUES ('agency',%s,'advertiser',%s,%s)", (parent_id, advertiser_id, status))
    conn.commit()
    rels = _q(conn,
        "SELECT * FROM fw_relationships WHERE child_type = 'advertiser' AND child_id = %s",
        (advertiser_id,)).fetchall()
    return {"advertiser_id": advertiser_id, "relationships": [dict(r) for r in rels]}


@router.put("/services/v3/advertisers/{advertiser_id}/add_relationships")
def advertiser_add_relationships(advertiser_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    return _advertiser_relationships(advertiser_id, body, "active", conn)


@router.put("/services/v3/advertisers/{advertiser_id}/activate_relationships")
def advertiser_activate_relationships(advertiser_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    _q(conn, "UPDATE fw_relationships SET status = 'active' WHERE child_type = 'advertiser' AND child_id = %s",
       (advertiser_id,))
    conn.commit()
    rels = _q(conn, "SELECT * FROM fw_relationships WHERE child_type = 'advertiser' AND child_id = %s",
              (advertiser_id,)).fetchall()
    return {"advertiser_id": advertiser_id, "relationships": [dict(r) for r in rels]}


@router.put("/services/v3/advertisers/{advertiser_id}/deactivate_relationships")
def advertiser_deactivate_relationships(advertiser_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    _q(conn, "UPDATE fw_relationships SET status = 'inactive' WHERE child_type = 'advertiser' AND child_id = %s",
       (advertiser_id,))
    conn.commit()
    rels = _q(conn, "SELECT * FROM fw_relationships WHERE child_type = 'advertiser' AND child_id = %s",
              (advertiser_id,)).fetchall()
    return {"advertiser_id": advertiser_id, "relationships": [dict(r) for r in rels]}


@router.get("/services/v3/advertisers/{advertiser_id}/brands")
def list_advertiser_brands(advertiser_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    rows = _q(conn, "SELECT * FROM fw_brands WHERE advertiser_id = %s ORDER BY brand_id",
              (advertiser_id,)).fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v3/advertisers/{advertiser_id}/parent_agencies")
def list_advertiser_parent_agencies(advertiser_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    agencies = []
    if row["agency_id"]:
        agency = _q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (row["agency_id"],)).fetchone()
        if agency:
            agencies.append(dict(agency))
    return {"items": agencies}


# ── Agency API V3 ──────────────────────────────────────────────────────────

@router.get("/services/v3/agencies")
def list_agencies(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_agencies ORDER BY agency_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.post("/services/v3/agencies")
def create_agency(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    agy_id = _next_id(conn, "fw_agencies", "agency_id")
    _q(conn,
        "INSERT INTO fw_agencies (agency_id, name, external_id, status, parent_agency_id, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (agy_id, body.get("name", "New Agency"), body.get("external_id"), "active",
         body.get("parent_agency_id"), _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (agy_id,)).fetchone())


@router.get("/services/v3/agencies/{agency_id}")
def get_agency(agency_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    return dict(row)


@router.put("/services/v3/agencies/{agency_id}")
def update_agency(agency_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    _apply_updates(conn, "fw_agencies", "agency_id", agency_id, body,
                   {"name", "external_id", "status", "parent_agency_id"})
    return dict(_q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone())


def _set_agency_status(agency_id, status, conn):
    row = _q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    _q(conn, "UPDATE fw_agencies SET status = %s WHERE agency_id = %s", (status, agency_id))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone())


@router.put("/services/v3/agencies/{agency_id}/activate")
def activate_agency(agency_id: int, conn=Depends(get_db)):
    return _set_agency_status(agency_id, "active", conn)


@router.put("/services/v3/agencies/{agency_id}/deactivate")
def deactivate_agency(agency_id: int, conn=Depends(get_db)):
    return _set_agency_status(agency_id, "inactive", conn)


def _agency_relationships(agency_id, body, status, conn):
    row = _q(conn, "SELECT 1 FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    related = (body or {}).get("advertiser_ids") or (body or {}).get("relationships") or []
    for child_id in related:
        _q(conn,
            "INSERT INTO fw_relationships (parent_type, parent_id, child_type, child_id, status) "
            "VALUES ('agency',%s,'advertiser',%s,%s)", (agency_id, child_id, status))
    conn.commit()
    rels = _q(conn, "SELECT * FROM fw_relationships WHERE parent_type = 'agency' AND parent_id = %s",
              (agency_id,)).fetchall()
    return {"agency_id": agency_id, "relationships": [dict(r) for r in rels]}


@router.put("/services/v3/agencies/{agency_id}/add_relationships")
def agency_add_relationships(agency_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    return _agency_relationships(agency_id, body, "active", conn)


@router.put("/services/v3/agencies/{agency_id}/activate_relationships")
def agency_activate_relationships(agency_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    _q(conn, "UPDATE fw_relationships SET status = 'active' WHERE parent_type = 'agency' AND parent_id = %s",
       (agency_id,))
    conn.commit()
    rels = _q(conn, "SELECT * FROM fw_relationships WHERE parent_type = 'agency' AND parent_id = %s",
              (agency_id,)).fetchall()
    return {"agency_id": agency_id, "relationships": [dict(r) for r in rels]}


@router.put("/services/v3/agencies/{agency_id}/deactivate_relationships")
def agency_deactivate_relationships(agency_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    _q(conn, "UPDATE fw_relationships SET status = 'inactive' WHERE parent_type = 'agency' AND parent_id = %s",
       (agency_id,))
    conn.commit()
    rels = _q(conn, "SELECT * FROM fw_relationships WHERE parent_type = 'agency' AND parent_id = %s",
              (agency_id,)).fetchall()
    return {"agency_id": agency_id, "relationships": [dict(r) for r in rels]}


@router.get("/services/v3/agencies/{agency_id}/child_advertisers")
def list_agency_child_advertisers(agency_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    rows = _q(conn, "SELECT * FROM fw_advertisers WHERE agency_id = %s ORDER BY advertiser_id",
              (agency_id,)).fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v3/agencies/{agency_id}/child_agencies")
def list_agency_child_agencies(agency_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    rows = _q(conn, "SELECT * FROM fw_agencies WHERE parent_agency_id = %s ORDER BY agency_id",
              (agency_id,)).fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v3/agencies/{agency_id}/parent_agencies")
def list_agency_parent_agencies(agency_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s", (agency_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Agency not found")
    parents = []
    if row["parent_agency_id"]:
        parent = _q(conn, "SELECT * FROM fw_agencies WHERE agency_id = %s",
                    (row["parent_agency_id"],)).fetchone()
        if parent:
            parents.append(dict(parent))
    return {"items": parents}


# ── Brand API V3 ───────────────────────────────────────────────────────────

@router.get("/services/v3/advertisers/{advertiser_id}/brands/{brand_id}")
def get_brand(advertiser_id: int, brand_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_brands WHERE brand_id = %s AND advertiser_id = %s",
             (brand_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Brand not found")
    return dict(row)


@router.post("/services/v3/advertisers/{advertiser_id}/brands")
def create_brand(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    adv = _q(conn, "SELECT 1 FROM fw_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not adv:
        raise HTTPException(404, "Advertiser not found")
    brand_id = _next_id(conn, "fw_brands", "brand_id")
    _q(conn,
        "INSERT INTO fw_brands (brand_id, advertiser_id, name, external_id, status, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (brand_id, advertiser_id, body.get("name", "New Brand"),
         body.get("external_id"), "active", _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_brands WHERE brand_id = %s", (brand_id,)).fetchone())


@router.put("/services/v3/advertisers/{advertiser_id}/brands/{brand_id}")
def update_brand(advertiser_id: int, brand_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_brands WHERE brand_id = %s AND advertiser_id = %s",
             (brand_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Brand not found")
    _apply_updates(conn, "fw_brands", "brand_id", brand_id, body,
                   {"name", "external_id", "status"})
    return dict(_q(conn, "SELECT * FROM fw_brands WHERE brand_id = %s", (brand_id,)).fetchone())


@router.delete("/services/v3/advertisers/{advertiser_id}/brands/{brand_id}")
def delete_brand(advertiser_id: int, brand_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_brands WHERE brand_id = %s AND advertiser_id = %s",
             (brand_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Brand not found")
    _q(conn, "DELETE FROM fw_brands WHERE brand_id = %s", (brand_id,))
    conn.commit()
    return {"deleted": True, "brand_id": brand_id}


# ═══════════════════════════════════════════════════════════════════════════
# CAPABILITY 2: ORDER PUSHES (Insertion Order V3 + Placement V3)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/services/v3/campaign/{campaign_id}/insertion_order")
def create_insertion_order(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    campaign = _q(conn, "SELECT 1 FROM fw_campaigns WHERE campaign_id = %s", (campaign_id,)).fetchone()
    if not campaign:
        raise HTTPException(404, "Campaign not found")
    io_id = _next_id(conn, "fw_insertion_orders", "insertion_order_id")
    _q(conn,
        "INSERT INTO fw_insertion_orders (insertion_order_id, campaign_id, name, description, "
        "client_po, brand_id, external_id, primary_sales_person, primary_trafficker, currency, "
        "status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (io_id, campaign_id, body.get("name", "New IO"), body.get("description"),
         body.get("client_po"), body.get("brand_id"), body.get("external_id"),
         body.get("primary_sales_person"), body.get("primary_trafficker"),
         body.get("currency", "USD"), "draft", _now(), _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_insertion_orders WHERE insertion_order_id = %s", (io_id,)).fetchone())


@router.get("/services/v3/insertion_orders")
def list_insertion_orders(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_insertion_orders ORDER BY insertion_order_id").fetchall()
    return {"items": [dict(r) for r in rows]}


# Dual path: accept BOTH `insertion_order` and `insertion_orders` singular/plural.
@router.get("/services/v3/insertion_order/{insertion_order_id}")
@router.get("/services/v3/insertion_orders/{insertion_order_id}")
def get_insertion_order(insertion_order_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_insertion_orders WHERE insertion_order_id = %s",
             (insertion_order_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Insertion order not found")
    return dict(row)


@router.put("/services/v3/insertion_order/{insertion_order_id}")
@router.put("/services/v3/insertion_orders/{insertion_order_id}")
def update_insertion_order(insertion_order_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_insertion_orders WHERE insertion_order_id = %s",
             (insertion_order_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Insertion order not found")
    _apply_updates(conn, "fw_insertion_orders", "insertion_order_id", insertion_order_id, body,
                   {"name", "description", "client_po", "brand_id", "external_id",
                    "primary_sales_person", "primary_trafficker", "currency", "status"})
    return dict(_q(conn, "SELECT * FROM fw_insertion_orders WHERE insertion_order_id = %s",
                   (insertion_order_id,)).fetchone())


@router.delete("/services/v3/insertion_order/{insertion_order_id}")
@router.delete("/services/v3/insertion_orders/{insertion_order_id}")
def delete_insertion_order(insertion_order_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_insertion_orders WHERE insertion_order_id = %s",
             (insertion_order_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Insertion order not found")
    _q(conn, "DELETE FROM fw_insertion_orders WHERE insertion_order_id = %s", (insertion_order_id,))
    conn.commit()
    return {"deleted": True, "insertion_order_id": insertion_order_id}


def _set_io_status(insertion_order_id, status, conn):
    row = _q(conn, "SELECT * FROM fw_insertion_orders WHERE insertion_order_id = %s",
             (insertion_order_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Insertion order not found")
    _q(conn, "UPDATE fw_insertion_orders SET status = %s WHERE insertion_order_id = %s",
       (status, insertion_order_id))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_insertion_orders WHERE insertion_order_id = %s",
                   (insertion_order_id,)).fetchone())


@router.put("/services/v3/insertion_order/{insertion_order_id}/book")
def book_insertion_order(insertion_order_id: int, conn=Depends(get_db)):
    return _set_io_status(insertion_order_id, "booked", conn)


@router.put("/services/v3/insertion_order/{insertion_order_id}/unbook")
def unbook_insertion_order(insertion_order_id: int, conn=Depends(get_db)):
    return _set_io_status(insertion_order_id, "draft", conn)


@router.put("/services/v3/insertion_order/{insertion_order_id}/propose")
def propose_insertion_order(insertion_order_id: int, conn=Depends(get_db)):
    return _set_io_status(insertion_order_id, "proposed", conn)


@router.get("/services/v3/insertion_order/{insertion_order_id}/placements")
def list_io_placements(insertion_order_id: int, conn=Depends(get_db)):
    io = _q(conn, "SELECT 1 FROM fw_insertion_orders WHERE insertion_order_id = %s",
            (insertion_order_id,)).fetchone()
    if not io:
        raise HTTPException(404, "Insertion order not found")
    rows = _q(conn, "SELECT * FROM fw_placements WHERE insertion_order_id = %s ORDER BY placement_id",
              (insertion_order_id,)).fetchall()
    return {"items": [dict(r) for r in rows]}


# ── Placement API V3 (real spec is XML; mocked as JSON — documented divergence) ──

@router.post("/services/v3/placement/create")
def create_placement(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now, _past_date, _future_date
    io_id = body.get("insertion_order_id")
    if io_id is not None:
        io = _q(conn, "SELECT 1 FROM fw_insertion_orders WHERE insertion_order_id = %s", (io_id,)).fetchone()
        if not io:
            raise HTTPException(404, "Insertion order not found")
    pid = _next_id(conn, "fw_placements", "placement_id")
    _q(conn,
        "INSERT INTO fw_placements (placement_id, insertion_order_id, name, description, external_id, "
        "start_date, end_date, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (pid, io_id, body.get("name", "New Placement"), body.get("description"),
         body.get("external_id"), body.get("start_date", _past_date(0)),
         body.get("end_date", _future_date(30)), "active", _now(), _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (pid,)).fetchone())


@router.get("/services/v3/placements")
def list_placements(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_placements ORDER BY placement_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v3/placement/{placement_id}")
def get_placement(placement_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Placement not found")
    return dict(row)


@router.put("/services/v3/placement/{placement_id}")
def update_placement(placement_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Placement not found")
    _apply_updates(conn, "fw_placements", "placement_id", placement_id, body,
                   {"name", "description", "external_id", "start_date", "end_date", "status"})
    return dict(_q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone())


@router.delete("/services/v3/placement/{placement_id}")
def delete_placement(placement_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Placement not found")
    _q(conn, "DELETE FROM fw_placements WHERE placement_id = %s", (placement_id,))
    conn.commit()
    return {"deleted": True, "placement_id": placement_id}


def _set_placement_status(placement_id, status, conn):
    row = _q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Placement not found")
    _q(conn, "UPDATE fw_placements SET status = %s WHERE placement_id = %s", (status, placement_id))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone())


@router.put("/services/v3/placement/{placement_id}/activate")
def activate_placement(placement_id: int, conn=Depends(get_db)):
    return _set_placement_status(placement_id, "active", conn)


@router.put("/services/v3/placement/{placement_id}/deactivate")
def deactivate_placement(placement_id: int, conn=Depends(get_db)):
    return _set_placement_status(placement_id, "inactive", conn)


@router.put("/services/v3/placement/{placement_id}/cancel")
def cancel_placement(placement_id: int, conn=Depends(get_db)):
    return _set_placement_status(placement_id, "cancelled", conn)


@router.put("/services/v3/placement/{placement_id}/extend")
def extend_placement(placement_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    from app.database import _future_date
    row = _q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Placement not found")
    new_end = (body or {}).get("end_date", _future_date(60))
    _q(conn, "UPDATE fw_placements SET end_date = %s WHERE placement_id = %s", (new_end, placement_id))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_placements WHERE placement_id = %s", (placement_id,)).fetchone())


# ═══════════════════════════════════════════════════════════════════════════
# CAPABILITY 3: DELIVERY REPORTING (ASYNC JOB MODEL) + Creative Metrics V4
# ═══════════════════════════════════════════════════════════════════════════

def _build_report_rows(conn, sales_channel, start_date, end_date):
    """Aggregate seeded daily delivery stats into report rows (per day)."""
    sql = ("SELECT stat_date, sales_channel, "
           "SUM(impressions) AS impressions, SUM(clicks) AS clicks, "
           "SUM(spend) AS spend, SUM(revenue) AS revenue, "
           "AVG(completion_rate) AS completion_rate "
           "FROM fw_delivery_stats")
    conditions = []
    params = []
    if sales_channel:
        conditions.append("sales_channel = %s")
        params.append(sales_channel)
    if start_date:
        conditions.append("stat_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("stat_date <= %s")
        params.append(end_date)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " GROUP BY stat_date, sales_channel ORDER BY stat_date"
    rows = _q(conn, sql, tuple(params)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["clicks"] = int(d["clicks"] or 0)
        d["impressions"] = int(d["impressions"] or 0)
        d["spend"] = round(float(d["spend"] or 0), 2)
        d["revenue"] = round(float(d["revenue"] or 0), 2)
        d["completion_rate"] = round(float(d["completion_rate"] or 0), 4)
        result.append(d)
    return result


def _create_report_job(conn, report_type, sales_channel, start_date, end_date, filters):
    from app.database import _now
    job_id = str(uuid.uuid4())
    rows = _build_report_rows(conn, sales_channel, start_date, end_date)
    now = _now()
    # Mock: job completes immediately so the first poll returns rows.
    _q(conn,
        "INSERT INTO fw_report_jobs (job_id, report_type, status, sales_channel, start_date, "
        "end_date, filters, rows_json, created_at, completed_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (job_id, report_type, "completed", sales_channel, start_date, end_date,
         json.dumps(filters or {}), json.dumps(rows), now, now))
    conn.commit()
    return job_id


@router.get("/reporting/v1/audience/audience_items")
def report_audience_items(
    sales_channel: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    audience_item_id: Optional[str] = None,
    audience_item_name: Optional[str] = None,
    audience_item_status: Optional[str] = None,
    conn=Depends(get_db),
):
    filters = {
        "audience_item_id": audience_item_id,
        "audience_item_name": audience_item_name,
        "audience_item_status": audience_item_status,
    }
    job_id = _create_report_job(conn, "audience_items", sales_channel, start_date, end_date, filters)
    return JSONResponse(
        status_code=202,
        content={"job_id": job_id, "status": "pending",
                 "poll_url": f"/freewheel/reporting/v1/job/{job_id}"},
    )


@router.get("/reporting/v1/audience/audience_segments")
def report_audience_segments(
    sales_channel: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    audience_segment_kv: Optional[str] = None,
    conn=Depends(get_db),
):
    filters = {"audience_segment_kv": audience_segment_kv}
    job_id = _create_report_job(conn, "audience_segments", sales_channel, start_date, end_date, filters)
    return JSONResponse(
        status_code=202,
        content={"job_id": job_id, "status": "pending",
                 "poll_url": f"/freewheel/reporting/v1/job/{job_id}"},
    )


@router.get("/reporting/v1/job/{job_id}")
def get_report_job(job_id: str, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_report_jobs WHERE job_id = %s", (job_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Report job not found")
    d = dict(row)
    rows = json.loads(d.get("rows_json") or "[]")
    return {
        "job_id": d["job_id"],
        "report_type": d["report_type"],
        "status": d["status"],
        "sales_channel": d["sales_channel"],
        "start_date": d["start_date"],
        "end_date": d["end_date"],
        "row_count": len(rows),
        "rows": rows,
    }


# ── Creative Instance Metrics API V4 ───────────────────────────────────────

@router.post("/services/v4/creative_instances/{creative_instance_id}/creative_metrics")
def create_creative_metric(creative_instance_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    cm_id = _next_id(conn, "fw_creative_metrics", "creative_metric_id")
    _q(conn,
        "INSERT INTO fw_creative_metrics (creative_metric_id, creative_instance_id, metric_type, "
        "event_name, url, created_at) VALUES (%s,%s,%s,%s,%s,%s)",
        (cm_id, creative_instance_id, body.get("metric_type"), body.get("event_name"),
         body.get("url"), _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_creative_metrics WHERE creative_metric_id = %s", (cm_id,)).fetchone())


@router.put("/services/v4/creative_instances/{creative_instance_id}/creative_metrics/{creative_metric_id}")
def update_creative_metric(creative_instance_id: int, creative_metric_id: int,
                           body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM fw_creative_metrics WHERE creative_metric_id = %s AND creative_instance_id = %s",
        (creative_metric_id, creative_instance_id)).fetchone()
    if not row:
        raise HTTPException(404, "Creative metric not found")
    _apply_updates(conn, "fw_creative_metrics", "creative_metric_id", creative_metric_id, body,
                   {"metric_type", "event_name", "url"})
    return dict(_q(conn, "SELECT * FROM fw_creative_metrics WHERE creative_metric_id = %s",
                   (creative_metric_id,)).fetchone())


@router.delete("/services/v4/creative_instances/{creative_instance_id}/creative_metrics/{creative_metric_id}")
def delete_creative_metric(creative_instance_id: int, creative_metric_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM fw_creative_metrics WHERE creative_metric_id = %s AND creative_instance_id = %s",
        (creative_metric_id, creative_instance_id)).fetchone()
    if not row:
        raise HTTPException(404, "Creative metric not found")
    _q(conn, "DELETE FROM fw_creative_metrics WHERE creative_metric_id = %s", (creative_metric_id,))
    conn.commit()
    return {"deleted": True, "creative_metric_id": creative_metric_id}


# ═══════════════════════════════════════════════════════════════════════════
# MARKETPLACE (V4)
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/services/v4/available_listings")
def list_available_listings(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_available_listings ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v4/available_listings/{listing_id}")
def get_available_listing(listing_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_available_listings WHERE id = %s", (listing_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Available listing not found")
    return dict(row)


@router.post("/services/v4/available_listings/{listing_id}/purchase")
def purchase_available_listing(listing_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_available_listings WHERE id = %s", (listing_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Available listing not found")
    _q(conn, "UPDATE fw_available_listings SET status = 'purchased' WHERE id = %s", (listing_id,))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_available_listings WHERE id = %s", (listing_id,)).fetchone())


@router.post("/services/v4/available_listings/{listing_id}/reject")
def reject_available_listing(listing_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_available_listings WHERE id = %s", (listing_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Available listing not found")
    _q(conn, "UPDATE fw_available_listings SET status = 'rejected' WHERE id = %s", (listing_id,))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_available_listings WHERE id = %s", (listing_id,)).fetchone())


@router.get("/services/v4/listings")
def list_listings(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_listings ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v4/listings/{listing_id}")
def get_listing(listing_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_listings WHERE id = %s", (listing_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Listing not found")
    return dict(row)


# ── Marketplace Creative V4 ────────────────────────────────────────────────

@router.get("/services/v4/mkpl_creatives/list")
def list_mkpl_creatives(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_mkpl_creatives ORDER BY mkpl_creative_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v4/mkpl_creatives/{mkpl_creative_id}")
def get_mkpl_creative(mkpl_creative_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_mkpl_creatives WHERE mkpl_creative_id = %s",
             (mkpl_creative_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Marketplace creative not found")
    return dict(row)


@router.put("/services/v4/mkpl_creatives/{mkpl_creative_id}")
def update_mkpl_creative(mkpl_creative_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_mkpl_creatives WHERE mkpl_creative_id = %s",
             (mkpl_creative_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Marketplace creative not found")
    _apply_updates(conn, "fw_mkpl_creatives", "mkpl_creative_id", mkpl_creative_id, body,
                   {"name", "creative_type", "status"})
    return dict(_q(conn, "SELECT * FROM fw_mkpl_creatives WHERE mkpl_creative_id = %s",
                   (mkpl_creative_id,)).fetchone())


def _update_mkpl_subtype(mkpl_creative_id, creative_type, body, conn):
    row = _q(conn, "SELECT * FROM fw_mkpl_creatives WHERE mkpl_creative_id = %s",
             (mkpl_creative_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Marketplace creative not found")
    allowed = {k: v for k, v in (body or {}).items() if k in {"name", "status"}}
    allowed["creative_type"] = creative_type
    set_clause = ", ".join(f"{k} = %s" for k in allowed)
    _q(conn, f"UPDATE fw_mkpl_creatives SET {set_clause} WHERE mkpl_creative_id = %s",
       (*allowed.values(), mkpl_creative_id))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_mkpl_creatives WHERE mkpl_creative_id = %s",
                   (mkpl_creative_id,)).fetchone())


@router.put("/services/v4/mkpl_exchange_programmatic_creatives/{mkpl_creative_id}")
def update_mkpl_exchange_programmatic(mkpl_creative_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    return _update_mkpl_subtype(mkpl_creative_id, "mkpl_exchange_programmatic_creative", body, conn)


@router.put("/services/v4/mkpl_private_direct_sold_creatives/{mkpl_creative_id}")
def update_mkpl_private_direct_sold(mkpl_creative_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    return _update_mkpl_subtype(mkpl_creative_id, "mkpl_private_direct_sold_creative", body, conn)


@router.put("/services/v4/mkpl_private_programmatic_creatives/{mkpl_creative_id}")
def update_mkpl_private_programmatic(mkpl_creative_id: int, body: dict = Body(default={}), conn=Depends(get_db)):
    return _update_mkpl_subtype(mkpl_creative_id, "mkpl_private_programmatic_creative", body, conn)


# ── Marketplace orders / splits / packages (GET list + GET by id) ──────────

def _list_table(conn, table, order_col="id"):
    rows = _q(conn, f"SELECT * FROM {table} ORDER BY {order_col}").fetchall()
    return {"items": [dict(r) for r in rows]}


def _get_by_id(conn, table, id_val, not_found, id_col="id"):
    row = _q(conn, f"SELECT * FROM {table} WHERE {id_col} = %s", (id_val,)).fetchone()
    if not row:
        raise HTTPException(404, not_found)
    return dict(row)


@router.get("/services/v4/inventory_splits")
def list_inventory_splits(conn=Depends(get_db)):
    return _list_table(conn, "fw_inventory_splits")


@router.get("/services/v4/inventory_split_orders")
def list_inventory_split_orders(conn=Depends(get_db)):
    return _list_table(conn, "fw_inventory_split_orders")


@router.get("/services/v4/purchased_inventory_orders")
def list_purchased_inventory_orders(conn=Depends(get_db)):
    return _list_table(conn, "fw_purchased_inventory_orders")


@router.get("/services/v4/purchased_inventory_orders/{order_id}")
def get_purchased_inventory_order(order_id: int, conn=Depends(get_db)):
    return _get_by_id(conn, "fw_purchased_inventory_orders", order_id, "Purchased inventory order not found")


@router.get("/services/v4/sold_inventory_orders")
def list_sold_inventory_orders(conn=Depends(get_db)):
    return _list_table(conn, "fw_sold_inventory_orders")


@router.get("/services/v4/sold_inventory_orders/{order_id}")
def get_sold_inventory_order(order_id: int, conn=Depends(get_db)):
    return _get_by_id(conn, "fw_sold_inventory_orders", order_id, "Sold inventory order not found")


@router.get("/services/v4/supply_source_packages")
def list_supply_source_packages(conn=Depends(get_db)):
    return _list_table(conn, "fw_supply_source_packages")


@router.get("/services/v4/supply_source_packages/{package_id}")
def get_supply_source_package(package_id: int, conn=Depends(get_db)):
    return _get_by_id(conn, "fw_supply_source_packages", package_id, "Supply source package not found")


# ═══════════════════════════════════════════════════════════════════════════
# RFP + PROPOSED + PROGRAMMATIC (V4)
# ═══════════════════════════════════════════════════════════════════════════

# ── RFP Open API ───────────────────────────────────────────────────────────

@router.post("/services/v4/forecasts/rfp")
def create_rfp_forecast(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    rfp_id = _next_id(conn, "fw_rfp_forecasts", "id")
    _q(conn,
        "INSERT INTO fw_rfp_forecasts (id, granularity, data_json, status, created_at) "
        "VALUES (%s,%s,%s,%s,%s)",
        (rfp_id, body.get("granularity", "daily"), json.dumps(body.get("data", {})),
         "completed", _now()))
    conn.commit()
    row = dict(_q(conn, "SELECT * FROM fw_rfp_forecasts WHERE id = %s", (rfp_id,)).fetchone())
    row["data"] = json.loads(row.pop("data_json") or "{}")
    return row


@router.get("/services/v4/forecasts/rfp/{rfp_id}")
def get_rfp_forecast(rfp_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_rfp_forecasts WHERE id = %s", (rfp_id,)).fetchone()
    if not row:
        raise HTTPException(404, "RFP forecast not found")
    d = dict(row)
    d["data"] = json.loads(d.pop("data_json") or "{}")
    return d


@router.put("/services/v4/forecasts/rfp/{rfp_id}")
def update_rfp_forecast(rfp_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_rfp_forecasts WHERE id = %s", (rfp_id,)).fetchone()
    if not row:
        raise HTTPException(404, "RFP forecast not found")
    if "granularity" in body:
        _q(conn, "UPDATE fw_rfp_forecasts SET granularity = %s WHERE id = %s",
           (body["granularity"], rfp_id))
    if "data" in body:
        _q(conn, "UPDATE fw_rfp_forecasts SET data_json = %s WHERE id = %s",
           (json.dumps(body["data"]), rfp_id))
    conn.commit()
    d = dict(_q(conn, "SELECT * FROM fw_rfp_forecasts WHERE id = %s", (rfp_id,)).fetchone())
    d["data"] = json.loads(d.pop("data_json") or "{}")
    return d


@router.delete("/services/v4/forecasts/rfp/{rfp_id}")
def delete_rfp_forecast(rfp_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_rfp_forecasts WHERE id = %s", (rfp_id,)).fetchone()
    if not row:
        raise HTTPException(404, "RFP forecast not found")
    _q(conn, "DELETE FROM fw_rfp_forecasts WHERE id = %s", (rfp_id,))
    conn.commit()
    return {"deleted": True, "id": rfp_id}


# ── Proposed Insertion Order ───────────────────────────────────────────────

@router.get("/services/v4/campaigns/{campaign_id}/proposed_insertion_orders")
def list_campaign_proposed_ios(campaign_id: int, conn=Depends(get_db)):
    rows = _q(conn,
        "SELECT * FROM fw_proposed_insertion_orders WHERE campaign_id = %s ORDER BY proposed_io_id",
        (campaign_id,)).fetchall()
    return {"items": [dict(r) for r in rows]}


@router.post("/services/v4/campaigns/{campaign_id}/proposed_insertion_orders")
def create_proposed_io(campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    pio_id = _next_id(conn, "fw_proposed_insertion_orders", "proposed_io_id")
    _q(conn,
        "INSERT INTO fw_proposed_insertion_orders (proposed_io_id, campaign_id, name, status, created_at) "
        "VALUES (%s,%s,%s,%s,%s)",
        (pio_id, campaign_id, body.get("name", "New Proposed IO"), "proposed", _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_proposed_insertion_orders WHERE proposed_io_id = %s",
                   (pio_id,)).fetchone())


@router.get("/services/v4/proposed_insertion_orders")
def list_proposed_ios(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_proposed_insertion_orders ORDER BY proposed_io_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v4/proposed_insertion_orders/{proposed_io_id}")
def get_proposed_io(proposed_io_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_proposed_insertion_orders WHERE proposed_io_id = %s",
             (proposed_io_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed insertion order not found")
    return dict(row)


@router.patch("/services/v4/proposed_insertion_orders/{proposed_io_id}")
def patch_proposed_io(proposed_io_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_proposed_insertion_orders WHERE proposed_io_id = %s",
             (proposed_io_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed insertion order not found")
    _apply_updates(conn, "fw_proposed_insertion_orders", "proposed_io_id", proposed_io_id, body,
                   {"name", "status"})
    return dict(_q(conn, "SELECT * FROM fw_proposed_insertion_orders WHERE proposed_io_id = %s",
                   (proposed_io_id,)).fetchone())


@router.get("/services/v4/proposed_insertion_orders/{proposed_io_id}/proposed_placements")
def list_proposed_io_placements(proposed_io_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT 1 FROM fw_proposed_insertion_orders WHERE proposed_io_id = %s",
             (proposed_io_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed insertion order not found")
    rows = _q(conn,
        "SELECT * FROM fw_proposed_placements WHERE proposed_io_id = %s ORDER BY proposed_placement_id",
        (proposed_io_id,)).fetchall()
    return {"items": [dict(r) for r in rows]}


# ── Proposed Placement ─────────────────────────────────────────────────────

@router.post("/services/v4/proposed_insertion_orders/{proposed_io_id}/proposed_placements")
def create_proposed_placement(proposed_io_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    io = _q(conn, "SELECT 1 FROM fw_proposed_insertion_orders WHERE proposed_io_id = %s",
            (proposed_io_id,)).fetchone()
    if not io:
        raise HTTPException(404, "Proposed insertion order not found")
    ppl_id = _next_id(conn, "fw_proposed_placements", "proposed_placement_id")
    _q(conn,
        "INSERT INTO fw_proposed_placements (proposed_placement_id, proposed_io_id, name, status, created_at) "
        "VALUES (%s,%s,%s,%s,%s)",
        (ppl_id, proposed_io_id, body.get("name", "New Proposed Placement"), "proposed", _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_proposed_placements WHERE proposed_placement_id = %s",
                   (ppl_id,)).fetchone())


@router.get("/services/v4/proposed_placements")
def list_proposed_placements(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_proposed_placements ORDER BY proposed_placement_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v4/proposed_placements/{proposed_placement_id}")
def get_proposed_placement(proposed_placement_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_proposed_placements WHERE proposed_placement_id = %s",
             (proposed_placement_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed placement not found")
    return dict(row)


@router.patch("/services/v4/proposed_placements/{proposed_placement_id}")
def patch_proposed_placement(proposed_placement_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_proposed_placements WHERE proposed_placement_id = %s",
             (proposed_placement_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed placement not found")
    _apply_updates(conn, "fw_proposed_placements", "proposed_placement_id", proposed_placement_id, body,
                   {"name", "status"})
    return dict(_q(conn, "SELECT * FROM fw_proposed_placements WHERE proposed_placement_id = %s",
                   (proposed_placement_id,)).fetchone())


# ── Proposed Ad ────────────────────────────────────────────────────────────

@router.post("/services/v4/proposed_placements/{proposed_placement_id}/proposed_ads")
def create_proposed_ad(proposed_placement_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    ppl = _q(conn, "SELECT 1 FROM fw_proposed_placements WHERE proposed_placement_id = %s",
             (proposed_placement_id,)).fetchone()
    if not ppl:
        raise HTTPException(404, "Proposed placement not found")
    pad_id = _next_id(conn, "fw_proposed_ads", "proposed_ad_id")
    _q(conn,
        "INSERT INTO fw_proposed_ads (proposed_ad_id, proposed_placement_id, name, status, created_at) "
        "VALUES (%s,%s,%s,%s,%s)",
        (pad_id, proposed_placement_id, body.get("name", "New Proposed Ad"), "proposed", _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_proposed_ads WHERE proposed_ad_id = %s", (pad_id,)).fetchone())


@router.get("/services/v4/proposed_ads/{proposed_ad_id}")
def get_proposed_ad(proposed_ad_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_proposed_ads WHERE proposed_ad_id = %s", (proposed_ad_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed ad not found")
    return dict(row)


@router.patch("/services/v4/proposed_ads/{proposed_ad_id}")
def patch_proposed_ad(proposed_ad_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_proposed_ads WHERE proposed_ad_id = %s", (proposed_ad_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed ad not found")
    _apply_updates(conn, "fw_proposed_ads", "proposed_ad_id", proposed_ad_id, body, {"name", "status"})
    return dict(_q(conn, "SELECT * FROM fw_proposed_ads WHERE proposed_ad_id = %s", (proposed_ad_id,)).fetchone())


@router.delete("/services/v4/proposed_ads/{proposed_ad_id}")
def delete_proposed_ad(proposed_ad_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_proposed_ads WHERE proposed_ad_id = %s", (proposed_ad_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Proposed ad not found")
    _q(conn, "DELETE FROM fw_proposed_ads WHERE proposed_ad_id = %s", (proposed_ad_id,))
    conn.commit()
    return {"deleted": True, "proposed_ad_id": proposed_ad_id}


# ── Programmatic Deal Open API ─────────────────────────────────────────────

@router.get("/services/v4/programmatic/deals")
def list_programmatic_deals(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_programmatic_deals ORDER BY deal_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.post("/services/v4/programmatic/deals")
def create_programmatic_deal(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    deal_id = _next_id(conn, "fw_programmatic_deals", "deal_id")
    _q(conn,
        "INSERT INTO fw_programmatic_deals (deal_id, name, deal_type, description, salesperson, "
        "status, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (deal_id, body.get("name", "New Deal"), body.get("deal_type"), body.get("description"),
         body.get("salesperson"), "draft", _now()))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_programmatic_deals WHERE deal_id = %s", (deal_id,)).fetchone())


@router.get("/services/v4/programmatic/deals/{deal_id}")
def get_programmatic_deal(deal_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_programmatic_deals WHERE deal_id = %s", (deal_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Programmatic deal not found")
    return dict(row)


@router.patch("/services/v4/programmatic/deals/{deal_id}")
def patch_programmatic_deal(deal_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM fw_programmatic_deals WHERE deal_id = %s", (deal_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Programmatic deal not found")
    _apply_updates(conn, "fw_programmatic_deals", "deal_id", deal_id, body,
                   {"name", "deal_type", "description", "salesperson", "status"})
    return dict(_q(conn, "SELECT * FROM fw_programmatic_deals WHERE deal_id = %s", (deal_id,)).fetchone())


def _set_deal_status(deal_id, status, conn):
    row = _q(conn, "SELECT * FROM fw_programmatic_deals WHERE deal_id = %s", (deal_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Programmatic deal not found")
    _q(conn, "UPDATE fw_programmatic_deals SET status = %s WHERE deal_id = %s", (status, deal_id))
    conn.commit()
    return dict(_q(conn, "SELECT * FROM fw_programmatic_deals WHERE deal_id = %s", (deal_id,)).fetchone())


@router.put("/services/v4/programmatic/deals/{deal_id}/activate")
def activate_programmatic_deal(deal_id: int, conn=Depends(get_db)):
    return _set_deal_status(deal_id, "active", conn)


@router.put("/services/v4/programmatic/deals/{deal_id}/deactivate")
def deactivate_programmatic_deal(deal_id: int, conn=Depends(get_db)):
    return _set_deal_status(deal_id, "inactive", conn)


@router.get("/services/v4/programmatic/buyers")
def list_programmatic_buyers(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_programmatic_buyers ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v4/global_advertisers")
def list_global_advertisers(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_global_advertisers ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/services/v4/global_brands")
def list_global_brands(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM fw_global_brands ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}
