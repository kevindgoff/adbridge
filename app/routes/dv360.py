"""DV360 (Display & Video 360) mock API endpoints under /dv360/v4."""

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Optional

from app.database import get_db

router = APIRouter(prefix="/dv360/v4")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _q(conn, sql, params=()):
    """Execute a query via cursor (psycopg2 connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _list_resp(rows, next_page_token=None):
    return {
        item_key(rows): [dict(r) for r in rows],
        "nextPageToken": next_page_token,
    }


def item_key(rows):
    """Infer the list wrapper key from context — DV360 uses plural resource names."""
    return "items"


def _paginate(conn, sql, params, page_size=25, page_token=None):
    if page_token:
        offset = int(page_token)
    else:
        offset = 0
    full_sql = f"{sql} LIMIT %s OFFSET %s"
    rows = _q(conn, full_sql, (*params, page_size, offset)).fetchall()
    next_token = None
    if len(rows) == page_size:
        next_token = str(offset + page_size)
    return [dict(r) for r in rows], next_token


def _nest_budget(row_dict, prefix="budget_"):
    """Extract budget_ prefixed fields into a nested budget object."""
    budget = {}
    keys_to_remove = []
    for k in list(row_dict.keys()):
        if k.startswith(prefix):
            budget[k[len(prefix):]] = row_dict[k]
            keys_to_remove.append(k)
    for k in keys_to_remove:
        del row_dict[k]
    if budget:
        row_dict["budget"] = budget
    return row_dict


def _nest_pacing(row_dict):
    if "pacing_type" in row_dict:
        row_dict["pacing"] = {"type": row_dict.pop("pacing_type")}
    return row_dict


def _nest_frequency_cap(row_dict):
    mi = row_dict.pop("frequency_cap_max_impressions", None)
    tu = row_dict.pop("frequency_cap_time_unit", None)
    if mi is not None or tu is not None:
        row_dict["frequencyCap"] = {"maxImpressions": mi, "timeUnit": tu}
    return row_dict


def _nest_bid_strategy(row_dict):
    bst = row_dict.pop("bid_strategy_type", None)
    bam = row_dict.pop("bid_amount_micros", None)
    if bst or bam:
        row_dict["bidStrategy"] = {"type": bst, "fixedBidAmountMicros": bam}
    return row_dict


def _nest_dimensions(row_dict):
    w = row_dict.pop("dimensions_width", None)
    h = row_dict.pop("dimensions_height", None)
    if w is not None or h is not None:
        row_dict["dimensions"] = {"widthPixels": w, "heightPixels": h}
    return row_dict


def _format_campaign(row_dict):
    cgt = row_dict.pop("campaign_goal_type", None)
    pgt = row_dict.pop("performance_goal_type", None)
    pgam = row_dict.pop("performance_goal_amount_micros", None)
    if cgt:
        row_dict["campaignGoal"] = {
            "campaignGoalType": cgt,
            "performanceGoal": {"type": pgt, "amountMicros": pgam},
        }
    fs = row_dict.pop("campaign_flight_start", None)
    fe = row_dict.pop("campaign_flight_end", None)
    if fs or fe:
        row_dict["campaignFlight"] = {"plannedStartDate": fs, "plannedEndDate": fe}
    _nest_frequency_cap(row_dict)
    return row_dict


def _format_io(row_dict):
    _nest_pacing(row_dict)
    _nest_budget(row_dict)
    fs = row_dict.pop("flight_start", None)
    fe = row_dict.pop("flight_end", None)
    if fs or fe:
        row_dict["flight"] = {"startDate": fs, "endDate": fe}
    return row_dict


def _format_line_item(row_dict):
    _nest_pacing(row_dict)
    _nest_budget(row_dict)
    _nest_frequency_cap(row_dict)
    _nest_bid_strategy(row_dict)
    fs = row_dict.pop("flight_start", None)
    fe = row_dict.pop("flight_end", None)
    if fs or fe:
        row_dict["flight"] = {"startDate": fs, "endDate": fe}
    return row_dict


def _format_ad_group(row_dict):
    _nest_bid_strategy(row_dict)
    return row_dict


def _format_creative(row_dict):
    _nest_dimensions(row_dict)
    return row_dict


# ── Partners ─────────────────────────────────────────────────────────────────

@router.get("/partners")
def list_partners(pageSize: int = 25, pageToken: Optional[str] = None,
                  conn=Depends(get_db)):
    data, next_token = _paginate(conn, "SELECT * FROM dv360_partners", (), pageSize, pageToken)
    return {"partners": data, "nextPageToken": next_token}


@router.get("/partners/{partner_id}")
def get_partner(partner_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM dv360_partners WHERE partner_id = %s", (partner_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Partner not found")
    return dict(row)


# ── Advertisers ──────────────────────────────────────────────────────────────

@router.get("/advertisers")
def list_advertisers(partnerId: Optional[int] = None,
                     pageSize: int = 25, pageToken: Optional[str] = None,
                     filter: Optional[str] = None,
                     conn=Depends(get_db)):
    sql = "SELECT * FROM dv360_advertisers"
    params = []
    if partnerId:
        sql += " WHERE partner_id = %s"
        params.append(partnerId)
    data, next_token = _paginate(conn, sql, tuple(params), pageSize, pageToken)
    return {"advertisers": data, "nextPageToken": next_token}


@router.post("/advertisers")
def create_advertiser(body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    max_row = _q(conn, "SELECT MAX(advertiser_id) as max_id FROM dv360_advertisers").fetchone()
    adv_id = body.get("advertiserId") or (max_row["max_id"] or 200000) + 1
    _q(conn,
        "INSERT INTO dv360_advertisers VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (adv_id, body.get("partnerId", 100001), body.get("displayName", "New Advertiser"),
         "ENTITY_STATUS_ACTIVE", body.get("currencyCode", "USD"),
         body.get("domainUrl", ""), _now()),
    )
    conn.commit()
    return dict(_q(conn, "SELECT * FROM dv360_advertisers WHERE advertiser_id = %s", (adv_id,)).fetchone())


@router.get("/advertisers/{advertiser_id}")
def get_advertiser(advertiser_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM dv360_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    return dict(row)


@router.patch("/advertisers/{advertiser_id}")
def update_advertiser(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM dv360_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Advertiser not found")
    updates = {k: v for k, v in body.items() if k in ("display_name", "entity_status", "currency_code", "domain_url")}
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE dv360_advertisers SET {set_clause} WHERE advertiser_id = %s",
           (*updates.values(), advertiser_id))
        conn.commit()
    return dict(_q(conn, "SELECT * FROM dv360_advertisers WHERE advertiser_id = %s", (advertiser_id,)).fetchone())


@router.delete("/advertisers/{advertiser_id}")
def delete_advertiser(advertiser_id: int, conn=Depends(get_db)):
    _q(conn, "DELETE FROM dv360_advertisers WHERE advertiser_id = %s", (advertiser_id,))
    conn.commit()
    return {}


# ── Campaigns ────────────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/campaigns")
def list_campaigns(advertiser_id: int, pageSize: int = 25, pageToken: Optional[str] = None,
                   filter: Optional[str] = None, conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_campaigns WHERE advertiser_id = %s",
        (advertiser_id,), pageSize, pageToken)
    return {"campaigns": [_format_campaign(d) for d in data], "nextPageToken": next_token}


@router.post("/advertisers/{advertiser_id}/campaigns")
def create_campaign(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now, _past_date, _future_date
    max_row = _q(conn, "SELECT MAX(campaign_id) as max_id FROM dv360_campaigns").fetchone()
    cid = (max_row["max_id"] or 300000) + 1
    _q(conn,
        "INSERT INTO dv360_campaigns VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (cid, advertiser_id, body.get("displayName", "New Campaign"),
         body.get("entityStatus", "ENTITY_STATUS_ACTIVE"),
         body.get("campaignGoalType"), body.get("performanceGoalType"),
         body.get("performanceGoalAmountMicros"),
         body.get("campaignFlightStart", _past_date(30)),
         body.get("campaignFlightEnd", _future_date(30)),
         body.get("frequencyCapMaxImpressions"), body.get("frequencyCapTimeUnit"),
         _now()),
    )
    conn.commit()
    row = _q(conn, "SELECT * FROM dv360_campaigns WHERE campaign_id = %s", (cid,)).fetchone()
    return _format_campaign(dict(row))


@router.get("/advertisers/{advertiser_id}/campaigns/{campaign_id}")
def get_campaign(advertiser_id: int, campaign_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_campaigns WHERE campaign_id = %s AND advertiser_id = %s",
        (campaign_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    return _format_campaign(dict(row))


@router.patch("/advertisers/{advertiser_id}/campaigns/{campaign_id}")
def update_campaign(advertiser_id: int, campaign_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_campaigns WHERE campaign_id = %s AND advertiser_id = %s",
        (campaign_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Campaign not found")
    allowed = {"display_name", "entity_status", "campaign_goal_type", "performance_goal_type",
               "performance_goal_amount_micros"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE dv360_campaigns SET {set_clause} WHERE campaign_id = %s",
           (*updates.values(), campaign_id))
        conn.commit()
    row = _q(conn, "SELECT * FROM dv360_campaigns WHERE campaign_id = %s", (campaign_id,)).fetchone()
    return _format_campaign(dict(row))


@router.delete("/advertisers/{advertiser_id}/campaigns/{campaign_id}")
def delete_campaign(advertiser_id: int, campaign_id: int, conn=Depends(get_db)):
    _q(conn, "DELETE FROM dv360_campaigns WHERE campaign_id = %s AND advertiser_id = %s",
       (campaign_id, advertiser_id))
    conn.commit()
    return {}


# ── Insertion Orders ─────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/insertionOrders")
def list_insertion_orders(advertiser_id: int, pageSize: int = 25, pageToken: Optional[str] = None,
                          filter: Optional[str] = None, conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_insertion_orders WHERE advertiser_id = %s",
        (advertiser_id,), pageSize, pageToken)
    return {"insertionOrders": [_format_io(d) for d in data], "nextPageToken": next_token}


@router.post("/advertisers/{advertiser_id}/insertionOrders")
def create_insertion_order(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now, _past_date, _future_date
    max_row = _q(conn, "SELECT MAX(insertion_order_id) as max_id FROM dv360_insertion_orders").fetchone()
    io_id = (max_row["max_id"] or 400000) + 1
    _q(conn,
        "INSERT INTO dv360_insertion_orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (io_id, advertiser_id, body.get("campaignId"),
         body.get("displayName", "New IO"),
         body.get("entityStatus", "ENTITY_STATUS_ACTIVE"),
         body.get("pacingType", "PACING_TYPE_EVEN"),
         body.get("budgetType", "BUDGET_TYPE_FIXED"),
         body.get("budgetAmountMicros", 5000000),
         body.get("budgetUnit", "BUDGET_UNIT_CURRENCY"),
         body.get("flightStart", _past_date(30)),
         body.get("flightEnd", _future_date(30)),
         _now()),
    )
    conn.commit()
    row = _q(conn, "SELECT * FROM dv360_insertion_orders WHERE insertion_order_id = %s", (io_id,)).fetchone()
    return _format_io(dict(row))


@router.get("/advertisers/{advertiser_id}/insertionOrders/{insertion_order_id}")
def get_insertion_order(advertiser_id: int, insertion_order_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_insertion_orders WHERE insertion_order_id = %s AND advertiser_id = %s",
        (insertion_order_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Insertion order not found")
    return _format_io(dict(row))


@router.patch("/advertisers/{advertiser_id}/insertionOrders/{insertion_order_id}")
def update_insertion_order(advertiser_id: int, insertion_order_id: int,
                           body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_insertion_orders WHERE insertion_order_id = %s AND advertiser_id = %s",
        (insertion_order_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Insertion order not found")
    allowed = {"display_name", "entity_status", "pacing_type", "budget_type",
               "budget_amount_micros", "budget_unit"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE dv360_insertion_orders SET {set_clause} WHERE insertion_order_id = %s",
           (*updates.values(), insertion_order_id))
        conn.commit()
    row = _q(conn, "SELECT * FROM dv360_insertion_orders WHERE insertion_order_id = %s", (insertion_order_id,)).fetchone()
    return _format_io(dict(row))


@router.delete("/advertisers/{advertiser_id}/insertionOrders/{insertion_order_id}")
def delete_insertion_order(advertiser_id: int, insertion_order_id: int, conn=Depends(get_db)):
    _q(conn, "DELETE FROM dv360_insertion_orders WHERE insertion_order_id = %s AND advertiser_id = %s",
       (insertion_order_id, advertiser_id))
    conn.commit()
    return {}


# ── Line Items ───────────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/lineItems")
def list_line_items(advertiser_id: int, pageSize: int = 25, pageToken: Optional[str] = None,
                    filter: Optional[str] = None, conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_line_items WHERE advertiser_id = %s",
        (advertiser_id,), pageSize, pageToken)
    return {"lineItems": [_format_line_item(d) for d in data], "nextPageToken": next_token}


@router.post("/advertisers/{advertiser_id}/lineItems")
def create_line_item(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now, _past_date, _future_date
    max_row = _q(conn, "SELECT MAX(line_item_id) as max_id FROM dv360_line_items").fetchone()
    li_id = (max_row["max_id"] or 500000) + 1
    _q(conn,
        "INSERT INTO dv360_line_items VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (li_id, advertiser_id, body.get("campaignId"), body.get("insertionOrderId"),
         body.get("displayName", "New Line Item"),
         body.get("lineItemType", "LINE_ITEM_TYPE_DISPLAY_DEFAULT"),
         body.get("entityStatus", "ENTITY_STATUS_ACTIVE"),
         body.get("flightStart", _past_date(15)), body.get("flightEnd", _future_date(15)),
         body.get("budgetType", "LINE_ITEM_BUDGET_ALLOCATION_TYPE_FIXED"),
         body.get("budgetAmountMicros", 1000000),
         body.get("pacingType", "PACING_TYPE_EVEN"),
         body.get("frequencyCapMaxImpressions"), body.get("frequencyCapTimeUnit"),
         body.get("bidStrategyType", "BIDDING_STRATEGY_FIXED_CPM"),
         body.get("bidAmountMicros", 500000), _now()),
    )
    conn.commit()
    row = _q(conn, "SELECT * FROM dv360_line_items WHERE line_item_id = %s", (li_id,)).fetchone()
    return _format_line_item(dict(row))


@router.get("/advertisers/{advertiser_id}/lineItems/{line_item_id}")
def get_line_item(advertiser_id: int, line_item_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_line_items WHERE line_item_id = %s AND advertiser_id = %s",
        (line_item_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Line item not found")
    return _format_line_item(dict(row))


@router.patch("/advertisers/{advertiser_id}/lineItems/{line_item_id}")
def update_line_item(advertiser_id: int, line_item_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_line_items WHERE line_item_id = %s AND advertiser_id = %s",
        (line_item_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Line item not found")
    allowed = {"display_name", "entity_status", "line_item_type", "budget_type",
               "budget_amount_micros", "pacing_type", "bid_strategy_type", "bid_amount_micros"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE dv360_line_items SET {set_clause} WHERE line_item_id = %s",
           (*updates.values(), line_item_id))
        conn.commit()
    row = _q(conn, "SELECT * FROM dv360_line_items WHERE line_item_id = %s", (line_item_id,)).fetchone()
    return _format_line_item(dict(row))


@router.delete("/advertisers/{advertiser_id}/lineItems/{line_item_id}")
def delete_line_item(advertiser_id: int, line_item_id: int, conn=Depends(get_db)):
    _q(conn, "DELETE FROM dv360_line_items WHERE line_item_id = %s AND advertiser_id = %s",
       (line_item_id, advertiser_id))
    conn.commit()
    return {}


# ── Ad Groups ────────────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/adGroups")
def list_ad_groups(advertiser_id: int, pageSize: int = 25, pageToken: Optional[str] = None,
                   conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_ad_groups WHERE advertiser_id = %s",
        (advertiser_id,), pageSize, pageToken)
    return {"adGroups": [_format_ad_group(d) for d in data], "nextPageToken": next_token}


@router.get("/advertisers/{advertiser_id}/adGroups/{ad_group_id}")
def get_ad_group(advertiser_id: int, ad_group_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_ad_groups WHERE ad_group_id = %s AND advertiser_id = %s",
        (ad_group_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Ad group not found")
    return _format_ad_group(dict(row))


# ── Ad Group Ads ─────────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/adGroupAds")
def list_ad_group_ads(advertiser_id: int, pageSize: int = 25, pageToken: Optional[str] = None,
                      conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_ad_group_ads WHERE advertiser_id = %s",
        (advertiser_id,), pageSize, pageToken)
    return {"adGroupAds": data, "nextPageToken": next_token}


@router.get("/advertisers/{advertiser_id}/adGroupAds/{ad_group_ad_id}")
def get_ad_group_ad(advertiser_id: int, ad_group_ad_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_ad_group_ads WHERE ad_group_ad_id = %s AND advertiser_id = %s",
        (ad_group_ad_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Ad group ad not found")
    return dict(row)


# ── Creatives ────────────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/creatives")
def list_creatives(advertiser_id: int, pageSize: int = 25, pageToken: Optional[str] = None,
                   conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_creatives WHERE advertiser_id = %s",
        (advertiser_id,), pageSize, pageToken)
    return {"creatives": [_format_creative(d) for d in data], "nextPageToken": next_token}


@router.post("/advertisers/{advertiser_id}/creatives")
def create_creative(advertiser_id: int, body: dict = Body(...), conn=Depends(get_db)):
    from app.database import _now
    max_row = _q(conn, "SELECT MAX(creative_id) as max_id FROM dv360_creatives").fetchone()
    cr_id = (max_row["max_id"] or 800000) + 1
    _q(conn,
        "INSERT INTO dv360_creatives VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (cr_id, advertiser_id, body.get("displayName", "New Creative"),
         body.get("entityStatus", "ENTITY_STATUS_ACTIVE"),
         body.get("creativeType", "CREATIVE_TYPE_STANDARD"),
         body.get("hostingSource", "HOSTING_SOURCE_CM"),
         body.get("dimensionsWidth"), body.get("dimensionsHeight"),
         "APPROVAL_STATUS_PENDING_REVIEW", "REVIEW_STATUS_UNSPECIFIED",
         body.get("exitUrl"), _now()),
    )
    conn.commit()
    row = _q(conn, "SELECT * FROM dv360_creatives WHERE creative_id = %s", (cr_id,)).fetchone()
    return _format_creative(dict(row))


@router.get("/advertisers/{advertiser_id}/creatives/{creative_id}")
def get_creative(advertiser_id: int, creative_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_creatives WHERE creative_id = %s AND advertiser_id = %s",
        (creative_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Creative not found")
    return _format_creative(dict(row))


@router.patch("/advertisers/{advertiser_id}/creatives/{creative_id}")
def update_creative(advertiser_id: int, creative_id: int, body: dict = Body(...), conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_creatives WHERE creative_id = %s AND advertiser_id = %s",
        (creative_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Creative not found")
    allowed = {"display_name", "entity_status", "exit_url"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if updates:
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        _q(conn, f"UPDATE dv360_creatives SET {set_clause} WHERE creative_id = %s",
           (*updates.values(), creative_id))
        conn.commit()
    row = _q(conn, "SELECT * FROM dv360_creatives WHERE creative_id = %s", (creative_id,)).fetchone()
    return _format_creative(dict(row))


@router.delete("/advertisers/{advertiser_id}/creatives/{creative_id}")
def delete_creative(advertiser_id: int, creative_id: int, conn=Depends(get_db)):
    _q(conn, "DELETE FROM dv360_creatives WHERE creative_id = %s AND advertiser_id = %s",
       (creative_id, advertiser_id))
    conn.commit()
    return {}


# ── Channels ─────────────────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/channels")
def list_channels(advertiser_id: int, pageSize: int = 25, pageToken: Optional[str] = None,
                  conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_channels WHERE advertiser_id = %s",
        (advertiser_id,), pageSize, pageToken)
    return {"channels": data, "nextPageToken": next_token}


@router.get("/advertisers/{advertiser_id}/channels/{channel_id}")
def get_channel(advertiser_id: int, channel_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_channels WHERE channel_id = %s AND advertiser_id = %s",
        (channel_id, advertiser_id)).fetchone()
    if not row:
        raise HTTPException(404, "Channel not found")
    return dict(row)


# ── Targeting Options (reference data) ───────────────────────────────────────

@router.get("/targetingTypes/{targeting_type}/targetingOptions")
def list_targeting_options(targeting_type: str, pageSize: int = 25,
                           pageToken: Optional[str] = None, conn=Depends(get_db)):
    data, next_token = _paginate(
        conn, "SELECT * FROM dv360_targeting_options WHERE targeting_type = %s",
        (targeting_type,), pageSize, pageToken)
    return {"targetingOptions": data, "nextPageToken": next_token}


@router.get("/targetingTypes/{targeting_type}/targetingOptions/{targeting_option_id}")
def get_targeting_option(targeting_type: str, targeting_option_id: str, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_targeting_options WHERE targeting_option_id = %s AND targeting_type = %s",
        (targeting_option_id, targeting_type)).fetchone()
    if not row:
        raise HTTPException(404, "Targeting option not found")
    return dict(row)


# ── Assigned Targeting ───────────────────────────────────────────────────────

@router.get("/advertisers/{advertiser_id}/lineItems/{line_item_id}/assignedTargetingOptions")
def list_li_assigned_targeting(advertiser_id: int, line_item_id: int, conn=Depends(get_db)):
    rows = _q(conn,
        "SELECT * FROM dv360_assigned_targeting WHERE entity_type = 'line_item' AND entity_id = %s AND advertiser_id = %s",
        (line_item_id, advertiser_id)).fetchall()
    return {"assignedTargetingOptions": [dict(r) for r in rows]}


@router.get("/advertisers/{advertiser_id}/lineItems/{line_item_id}/targetingTypes/{targeting_type}/assignedTargetingOptions")
def list_li_targeting_by_type(advertiser_id: int, line_item_id: int, targeting_type: str, conn=Depends(get_db)):
    rows = _q(conn,
        "SELECT * FROM dv360_assigned_targeting WHERE entity_type = 'line_item' AND entity_id = %s AND advertiser_id = %s AND targeting_type = %s",
        (line_item_id, advertiser_id, targeting_type)).fetchall()
    return {"assignedTargetingOptions": [dict(r) for r in rows]}


# ── Inventory Sources ────────────────────────────────────────────────────────

@router.get("/inventorySources")
def list_inventory_sources(pageSize: int = 25, pageToken: Optional[str] = None,
                           conn=Depends(get_db)):
    data, next_token = _paginate(conn, "SELECT * FROM dv360_inventory_sources", (), pageSize, pageToken)
    return {"inventorySources": data, "nextPageToken": next_token}


@router.get("/inventorySources/{inventory_source_id}")
def get_inventory_source(inventory_source_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM dv360_inventory_sources WHERE inventory_source_id = %s",
             (inventory_source_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Inventory source not found")
    return dict(row)


# ── Floodlight ───────────────────────────────────────────────────────────────

@router.get("/floodlightGroups")
def list_floodlight_groups(conn=Depends(get_db)):
    rows = _q(conn, "SELECT * FROM dv360_floodlight_groups").fetchall()
    return {"floodlightGroups": [dict(r) for r in rows]}


@router.get("/floodlightGroups/{floodlight_group_id}")
def get_floodlight_group(floodlight_group_id: int, conn=Depends(get_db)):
    row = _q(conn, "SELECT * FROM dv360_floodlight_groups WHERE floodlight_group_id = %s",
             (floodlight_group_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Floodlight group not found")
    return dict(row)


@router.get("/floodlightGroups/{floodlight_group_id}/floodlightActivities")
def list_floodlight_activities(floodlight_group_id: int, conn=Depends(get_db)):
    rows = _q(conn,
        "SELECT * FROM dv360_floodlight_activities WHERE floodlight_group_id = %s",
        (floodlight_group_id,)).fetchall()
    return {"floodlightActivities": [dict(r) for r in rows]}


@router.get("/floodlightGroups/{floodlight_group_id}/floodlightActivities/{floodlight_activity_id}")
def get_floodlight_activity(floodlight_group_id: int, floodlight_activity_id: int, conn=Depends(get_db)):
    row = _q(conn,
        "SELECT * FROM dv360_floodlight_activities WHERE floodlight_activity_id = %s AND floodlight_group_id = %s",
        (floodlight_activity_id, floodlight_group_id)).fetchone()
    if not row:
        raise HTTPException(404, "Floodlight activity not found")
    return dict(row)
