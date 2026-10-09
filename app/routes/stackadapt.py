"""StackAdapt GraphQL API mock, served under /stackadapt.

Mirrors the StackAdapt programmatic DSP GraphQL API:
  Endpoint:  https://api.stackadapt.com/graphql
  Docs:      https://docs.stackadapt.com/graphql/reference
  (see platform_api_sources.yml for every URL this mock was built from)

This mock was reconciled against the OFFICIAL StackAdapt GraphQL schema (SDL
supplied by the user), so the operation names, argument shapes, and response
envelopes below match the real contract rather than an inferred approximation.

StackAdapt exposes ONE GraphQL endpoint. Clients POST a JSON body of the shape
    {"query": "<document>", "variables": {...}, "operationName": "<name>"}
and the server dispatches on the operation. Authentication is a token in the
`X-Authorization` header.

Because this is a mock for local integration testing — not a production GraphQL
server — the resolver does NOT parse arbitrary GraphQL. It string-matches the
first operation field in the document (and honours `operationName` when given),
then returns a realistically-shaped `{"data": {...}}` / `{"errors": [...]}`
envelope. That is enough to exercise a client's request/response plumbing while
staying faithful to the real response contract.

Supported operations (names taken verbatim from the official schema)
  ── Campaign management (queries) ───────────────────────────────────────────
    campaigns(filterBy, first, after)   → CampaignConnection
    campaign(id)                        → Campaign
    campaignGroups(filterBy, first)     → CampaignGroupConnection
    campaignGroup(id)                   → CampaignGroup
    advertisers(filterBy, first)        → AdvertiserConnection
    advertiser(id)                      → Advertiser
  ── Pull delivery reporting (queries) ───────────────────────────────────────
    campaignDelivery(dataType, date, granularity, filterBy)
                                        → CampaignDeliveryPayload
                                          (CampaignDeliveryOutcome | Progress)
    campaignGroupDelivery(...)          → CampaignGroupDeliveryPayload
    advertiserDelivery(...)             → AdvertiserDeliveryPayload
      Each Outcome has { records[] { campaign/campaignGroup/advertiser,
      granularity, metrics { … } }, totalStats { … } }. The real API is
      job-based (payload is a union that returns Progress until the stats job
      finishes); this mock returns the finished Outcome synchronously but keeps
      the __typename so clients that branch on the union still work.
  ── Order pushes / campaign management (mutations) ──────────────────────────
    upsertCampaign(input: CampaignInput)         → UpsertCampaignPayload
       CampaignInput wraps a single per-channel block:
       { native|display|video|ctv|audio|dooh: {…CampaignInput fields} }
    createCampaignGroup(input: CampaignGroupInput) → CreateCampaignGroupPayload
    updateCampaignGroup(input: CampaignGroupInput) → UpdateCampaignGroupPayload
    createAdvertiser(input: AdvertiserInput)       → CreateAdvertiserPayload
    updateAdvertiser(input: AdvertiserInput)       → UpdateAdvertiserPayload
    pauseCampaigns(input)/resumeCampaigns(input)/archiveCampaigns(input)
       → Pause/Resume/ArchiveCampaignsPayload

All mutation payloads carry `clientMutationId` and `userErrors: [{message,
path}]`, exactly like the real schema's Relay-style payloads.

Entities are backed by the sa_* tables seeded in database.py.
"""

import re
import uuid
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header
from app.database import get_db

router = APIRouter(prefix="/stackadapt")


# The six programmatic channels StackAdapt models as distinct campaign types.
# CampaignInput carries exactly one of these blocks; the __typename on a
# returned Campaign is "<Channel>Campaign".
_CHANNEL_BLOCKS = ("native", "display", "video", "ctv", "audio", "dooh")
_CHANNEL_TYPENAME = {
    "NATIVE": "NativeCampaign", "DISPLAY": "DisplayCampaign",
    "VIDEO": "VideoCampaign", "CTV": "CtvCampaign",
    "AUDIO": "AudioCampaign", "DOOH": "DoohCampaign",
}


# ── Shared helpers ────────────────────────────────────────────────────────────

def _q(conn, sql, params=()):
    """Execute a query via cursor (connections have no .execute())."""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def _one(conn, sql, params=()):
    row = _q(conn, sql, params).fetchone()
    return dict(row) if row else None


def _all(conn, sql, params=()):
    return [dict(r) for r in _q(conn, sql, params).fetchall()]


def _now():
    from app.database import _now as _db_now
    return _db_now()


def _gql_error(message, code="BAD_REQUEST"):
    """GraphQL transport-level error envelope (HTTP 200 with an `errors` array)."""
    return {"errors": [{"message": message, "extensions": {"code": code}}]}


def _user_error(message, path=None):
    """Relay-style mutation userError: {message, path}."""
    return {"message": message, "path": path or []}


def _detect_operation(query: str, operation_name: Optional[str]):
    """Return the top-level operation field name from a GraphQL document.

    Mock-grade detection: find the first identifier inside the outermost
    selection set. Honours an explicit operationName hint when the document
    defines several operations.
    """
    if not query:
        return None
    body = query
    m = re.search(r"\{(.*)\}", query, re.DOTALL)
    if m:
        body = m.group(1)
    m2 = re.search(r"[A-Za-z_][A-Za-z0-9_]*", body)
    field = m2.group(0) if m2 else None
    return operation_name or field


def _bind_arguments(query, op, variables):
    """Resolve the top-level operation's arguments into a flat dict.

    Mock-grade binding: finds `op( ... )` in the document and resolves each
    `name: value` pair. A `$var` value is looked up in `variables` by the
    variable's name (so `input: $in` with variables {"in": {...}} resolves to
    `input`); inline scalar/enum/number/bool literals are parsed directly. The
    result is merged over `variables` so a resolver can read, e.g.,
    args["granularity"] whether the client sent it inline or via a variable.
    Complex inline object/array literals are not parsed (real integrations use
    variables for those); such args simply fall back to the variables dict.
    """
    args = dict(variables)  # start from raw variables as a fallback
    m = re.search(re.escape(op) + r"\s*\(", query or "")
    if not m:
        return args
    # Walk from the opening paren to its matching close paren.
    i = m.end() - 1
    depth, start = 0, i + 1
    for j in range(i, len(query)):
        ch = query[j]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                arg_src = query[start:j]
                break
    else:
        return args

    # Split top-level "name: value" pairs (respecting nested {}/[]).
    for name, raw in _split_arg_pairs(arg_src):
        val = _coerce_literal(raw, variables)
        if val is not _UNRESOLVED:
            args[name] = val
    return args


_UNRESOLVED = object()


def _split_arg_pairs(src):
    pairs, depth, key, buf, in_key = [], 0, None, "", True
    for ch in src:
        if ch in "{[(":
            depth += 1
        elif ch in "}])":
            depth -= 1
        if depth == 0 and ch == ":" and in_key:
            key, buf, in_key = buf.strip(), "", False
            continue
        if depth == 0 and ch == "," and not in_key:
            if key:
                pairs.append((key, buf.strip()))
            key, buf, in_key = None, "", True
            continue
        buf += ch
    if key and not in_key:
        pairs.append((key, buf.strip()))
    return pairs


def _coerce_literal(raw, variables):
    raw = raw.strip()
    if not raw:
        return _UNRESOLVED
    if raw.startswith("$"):
        return variables.get(raw[1:], _UNRESOLVED)
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    if raw in ("true", "false"):
        return raw == "true"
    if raw == "null":
        return None
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    if re.fullmatch(r"-?\d+\.\d+", raw):
        return float(raw)
    # bare word → enum value (e.g. DAILY, TABLE)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw):
        return raw
    # object/array inline literal — not parsed; defer to variables fallback
    return _UNRESOLVED


def _in_clause(column, values):
    """Build a portable 'AND column IN (%s, %s, ...)' fragment + param list.

    Uses expanded %s placeholders rather than Postgres-only `= ANY(%s)`, so the
    SQL works unchanged on both the psycopg2 and SQLite backends.
    """
    placeholders = ", ".join(["%s"] * len(values))
    return f" AND {column} IN ({placeholders})", list(values)


def _connection(nodes):
    """Build a Relay CampaignConnection-style envelope from a list of nodes."""
    edges = [{"node": n, "cursor": n.get("id")} for n in nodes]
    return {
        "totalCount": len(edges),
        "edges": edges,
        "nodes": nodes,
        "pageInfo": {
            "hasNextPage": False,
            "hasPreviousPage": False,
            "startCursor": edges[0]["cursor"] if edges else None,
            "endCursor": edges[-1]["cursor"] if edges else None,
        },
    }


# ── Entity formatters (DB row → GraphQL camelCase node) ─────────────────────────

def _campaign_node(row):
    channel = (row.get("channel") or "NATIVE").upper()
    return {
        "__typename": _CHANNEL_TYPENAME.get(channel, "NativeCampaign"),
        "id": row["id"],
        "name": row["name"],
        "channelType": channel.title(),
        "campaignStatus": row.get("status") or "DRAFT",
        "isArchived": bool(row.get("is_archived")),
        "isDraft": bool(row.get("is_draft")),
        "goalType": row.get("goal_type"),
        "advertiser": {"id": row.get("advertiser_id")} if row.get("advertiser_id") else None,
        "campaignGroup": {"id": row.get("campaign_group_id")} if row.get("campaign_group_id") else None,
        "timezone": row.get("timezone") or "America/New_York",
        "createdAt": row.get("created_at"),
        "updatedAt": row.get("updated_at") or row.get("created_at"),
    }


def _campaign_group_node(row):
    return {
        "id": row["id"],
        "name": row["name"],
        "campaignGroupStatus": row.get("status") or "ACTIVE",
        "budgetType": row.get("budget_type"),
        "budgetRollover": bool(row.get("budget_rollover")),
        "isArchived": bool(row.get("is_archived")),
        "advertiser": {"id": row.get("advertiser_id")} if row.get("advertiser_id") else None,
        "createdAt": row.get("created_at"),
    }


def _advertiser_node(row):
    return {
        "id": row["id"],
        "name": row["name"],
        "description": row.get("description"),
        "isArchived": bool(row.get("is_archived")),
    }


def _metrics(row):
    """A DeliveryStatsRecord subset — the real schema's core delivery metrics.

    Field names match the official DeliveryStatsRecord type (cost, not spend;
    conversions, impressions, clicks, ctr, ecpm, revenue, atos).
    """
    impressions = int(row.get("impressions") or 0)
    clicks = int(row.get("clicks") or 0)
    cost = float(row.get("cost") or 0)
    return {
        "impressions": impressions,
        "clicks": clicks,
        "conversions": float(row.get("conversions") or 0),
        "cost": round(cost, 2),
        "revenue": round(float(row.get("revenue") or 0), 2),
        "ctr": round(clicks / impressions, 6) if impressions else 0.0,
        "ecpm": round(cost / impressions * 1000, 4) if impressions else 0.0,
        "atos": float(row.get("atos") or 0),
    }


# ── Query resolvers ─────────────────────────────────────────────────────────────

def _filter_ids(variables):
    """Pull the common CampaignFilters/AdvertiserFilters shapes from variables."""
    fb = variables.get("filterBy") or {}
    return fb


def _resolve_campaigns(conn, variables):
    fb = _filter_ids(variables)
    where, params = "WHERE 1=1", []
    if fb.get("ids"):
        frag, p = _in_clause("id", fb["ids"])
        where += frag; params += p
    if fb.get("advertiserIds"):
        frag, p = _in_clause("advertiser_id", fb["advertiserIds"])
        where += frag; params += p
    if fb.get("campaignGroupIds"):
        frag, p = _in_clause("campaign_group_id", fb["campaignGroupIds"])
        where += frag; params += p
    if fb.get("archived") is not None:
        where += " AND is_archived = %s"
        params.append(bool(fb["archived"]))
    first = int(variables.get("first", 50))
    rows = _all(conn,
                f"SELECT * FROM sa_campaigns {where} ORDER BY id LIMIT %s",
                (*params, first))
    return {"data": {"campaigns": _connection([_campaign_node(r) for r in rows])}}


def _resolve_campaign(conn, variables):
    cid = variables.get("id")
    row = _one(conn, "SELECT * FROM sa_campaigns WHERE id = %s", (cid,))
    if not row:
        return _gql_error(f"Campaign not found: {cid}", "NOT_FOUND")
    return {"data": {"campaign": _campaign_node(row)}}


def _resolve_campaign_groups(conn, variables):
    rows = _all(conn, "SELECT * FROM sa_campaign_groups ORDER BY id")
    return {"data": {"campaignGroups": _connection([_campaign_group_node(r) for r in rows])}}


def _resolve_campaign_group(conn, variables):
    gid = variables.get("id")
    row = _one(conn, "SELECT * FROM sa_campaign_groups WHERE id = %s", (gid,))
    if not row:
        return _gql_error(f"Campaign group not found: {gid}", "NOT_FOUND")
    return {"data": {"campaignGroup": _campaign_group_node(row)}}


def _resolve_advertisers(conn, variables):
    rows = _all(conn, "SELECT * FROM sa_advertisers ORDER BY id")
    return {"data": {"advertisers": _connection([_advertiser_node(r) for r in rows])}}


def _resolve_advertiser(conn, variables):
    aid = variables.get("id")
    row = _one(conn, "SELECT * FROM sa_advertisers WHERE id = %s", (aid,))
    if not row:
        return _gql_error(f"Advertiser not found: {aid}", "NOT_FOUND")
    return {"data": {"advertiser": _advertiser_node(row)}}


def _delivery_rows(conn, group_col, variables):
    """Aggregate seeded daily stats into delivery records.

    granularity TOTAL/MONTHLY/WEEKLY → cumulative per entity; DAILY/HOURLY →
    per-day rows. `date` ({from,to}) filters the stat window. Mirrors the real
    campaignDelivery/campaignGroupDelivery/advertiserDelivery semantics.
    """
    granularity = (variables.get("granularity") or "TOTAL").upper()
    date = variables.get("date") or {}
    fb = _filter_ids(variables)

    where, params = "WHERE 1=1", []
    if fb.get("ids"):
        frag, p = _in_clause(group_col, fb["ids"])
        where += frag; params += p
    if variables.get("ids"):  # advertiserDelivery takes ids at top level
        frag, p = _in_clause(group_col, variables["ids"])
        where += frag; params += p
    if date.get("from"):
        where += " AND stat_date >= %s"
        params.append(date["from"])
    if date.get("to"):
        where += " AND stat_date <= %s"
        params.append(date["to"])

    if granularity in ("DAILY", "HOURLY"):
        rows = _all(conn, f"""
            SELECT {group_col} AS gid, stat_date,
                   impressions, clicks, conversions, cost, revenue, atos
            FROM sa_delivery_stats {where}
            ORDER BY {group_col}, stat_date""", tuple(params))
        return granularity, [
            {"gid": r["gid"], "granularity": granularity, "metrics": _metrics(r),
             "statDate": r["stat_date"]}
            for r in rows]

    rows = _all(conn, f"""
        SELECT {group_col} AS gid,
               SUM(impressions) AS impressions, SUM(clicks) AS clicks,
               SUM(conversions) AS conversions, SUM(cost) AS cost,
               SUM(revenue) AS revenue, AVG(atos) AS atos
        FROM sa_delivery_stats {where}
        GROUP BY {group_col} ORDER BY {group_col}""", tuple(params))
    return granularity, [
        {"gid": r["gid"], "granularity": granularity, "metrics": _metrics(r)}
        for r in rows]


def _total_stats(records):
    agg = {"impressions": 0, "clicks": 0, "conversions": 0.0,
           "cost": 0.0, "revenue": 0.0}
    for rec in records:
        m = rec["metrics"]
        agg["impressions"] += m["impressions"]
        agg["clicks"] += m["clicks"]
        agg["conversions"] += m["conversions"]
        agg["cost"] += m["cost"]
        agg["revenue"] += m["revenue"]
    imp = agg["impressions"]
    agg["ctr"] = round(agg["clicks"] / imp, 6) if imp else 0.0
    agg["ecpm"] = round(agg["cost"] / imp * 1000, 4) if imp else 0.0
    agg["cost"] = round(agg["cost"], 2)
    agg["revenue"] = round(agg["revenue"], 2)
    agg["atos"] = 0.0
    return agg


def _delivery_payload(conn, outcome_typename, record_entity_key, group_col,
                      node_fn, root_sql, variables):
    """Build a *DeliveryPayload union — the finished Outcome branch.

    Keeps `__typename` so a client branching on `... on Progress` /
    `... on CampaignDeliveryOutcome` still resolves correctly.
    """
    _granularity, raw = _delivery_rows(conn, group_col, variables)
    records = []
    for rec in raw:
        ent = _one(conn, root_sql, (rec["gid"],))
        node = node_fn(ent) if ent else {"id": rec["gid"]}
        rec_out = {
            record_entity_key: node,
            "granularity": rec["granularity"],
            "metrics": rec["metrics"],
        }
        if "statDate" in rec:
            rec_out["date"] = rec["statDate"]
        records.append(rec_out)
    return {
        "__typename": outcome_typename,
        "records": {
            "nodes": records,
            "totalCount": len(records),
            "edges": [{"node": r, "cursor": str(i)} for i, r in enumerate(records)],
            "pageInfo": {"hasNextPage": False},
        },
        "totalStats": _total_stats(records),
    }


def _resolve_campaign_delivery(conn, variables):
    payload = _delivery_payload(
        conn, "CampaignDeliveryOutcome", "campaign", "campaign_id",
        _campaign_node, "SELECT * FROM sa_campaigns WHERE id = %s", variables)
    return {"data": {"campaignDelivery": payload}}


def _resolve_campaign_group_delivery(conn, variables):
    payload = _delivery_payload(
        conn, "CampaignGroupDeliveryOutcome", "campaignGroup", "campaign_group_id",
        _campaign_group_node,
        "SELECT * FROM sa_campaign_groups WHERE id = %s", variables)
    return {"data": {"campaignGroupDelivery": payload}}


def _resolve_advertiser_delivery(conn, variables):
    payload = _delivery_payload(
        conn, "AdvertiserDeliveryOutcome", "advertiser", "advertiser_id",
        _advertiser_node, "SELECT * FROM sa_advertisers WHERE id = %s", variables)
    return {"data": {"advertiserDelivery": payload}}


# ── Mutation resolvers ──────────────────────────────────────────────────────────

def _resolve_upsert_campaign(conn, variables):
    """upsertCampaign(input: CampaignInput) — create (no id) or update (id).

    CampaignInput wraps exactly one per-channel block. We detect the channel
    from the present block key and flatten its fields.
    """
    inp = variables.get("input") or {}
    channel_key = next((k for k in _CHANNEL_BLOCKS if k in inp), None)
    if not channel_key:
        return {"data": {"upsertCampaign": {
            "campaign": None, "clientMutationId": inp.get("clientMutationId"),
            "userErrors": [_user_error(
                "CampaignInput must contain exactly one channel block "
                f"({', '.join(_CHANNEL_BLOCKS)}).", ["input"])]}}}
    block = inp[channel_key] or {}
    channel = channel_key.upper()
    client_mutation_id = inp.get("clientMutationId") or block.get("clientMutationId")

    cid = block.get("id")
    now = _now()
    if cid:  # update
        row = _one(conn, "SELECT * FROM sa_campaigns WHERE id = %s", (cid,))
        if not row:
            return {"data": {"upsertCampaign": {
                "campaign": None, "clientMutationId": client_mutation_id,
                "userErrors": [_user_error(f"Campaign not found: {cid}",
                                           ["input", channel_key, "id"])]}}}
        fields = {"name": "name", "goalType": "goal_type",
                  "campaignGroupId": "campaign_group_id",
                  "advertiserId": "advertiser_id", "isDraft": "is_draft"}
        sets, params = ["updated_at = %s"], [now]
        for gql_key, col in fields.items():
            if gql_key in block:
                sets.append(f"{col} = %s")
                params.append(block[gql_key])
        params.append(cid)
        _q(conn, f"UPDATE sa_campaigns SET {', '.join(sets)} WHERE id = %s",
           tuple(params))
        conn.commit()
        row = _one(conn, "SELECT * FROM sa_campaigns WHERE id = %s", (cid,))
        return {"data": {"upsertCampaign": {
            "campaign": _campaign_node(row),
            "clientMutationId": client_mutation_id, "userErrors": []}}}

    # create — requiredForCreate: name, advertiserId, campaignGroupId, goalType
    missing = [f for f in ("name", "advertiserId", "campaignGroupId", "goalType")
               if not block.get(f)]
    if missing:
        return {"data": {"upsertCampaign": {
            "campaign": None, "clientMutationId": client_mutation_id,
            "userErrors": [_user_error(f"{f} is required to create a campaign.",
                                       ["input", channel_key, f])
                           for f in missing]}}}
    new_id = f"sa-c-{uuid.uuid4().hex[:12]}"
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sa_campaigns
          (id, name, channel, status, is_draft, is_archived, goal_type,
           advertiser_id, campaign_group_id, timezone, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (new_id, block["name"], channel,
         "DRAFT" if block.get("isDraft") else "PENDING",
         bool(block.get("isDraft")), False, block["goalType"],
         block["advertiserId"], block["campaignGroupId"],
         block.get("timezone", "America/New_York"), now, now))
    conn.commit()
    row = _one(conn, "SELECT * FROM sa_campaigns WHERE id = %s", (new_id,))
    return {"data": {"upsertCampaign": {
        "campaign": _campaign_node(row),
        "clientMutationId": client_mutation_id, "userErrors": []}}}


def _resolve_create_campaign_group(conn, variables, _update=False):
    inp = variables.get("input") or {}
    gid = inp.get("id")
    now = _now()
    if _update:
        if not gid:
            return _cg_payload("updateCampaignGroup", None,
                               [_user_error("id is required for update.", ["input", "id"])])
        row = _one(conn, "SELECT * FROM sa_campaign_groups WHERE id = %s", (gid,))
        if not row:
            return _cg_payload("updateCampaignGroup", None,
                               [_user_error(f"Campaign group not found: {gid}", ["input", "id"])])
        fields = {"name": "name", "budgetType": "budget_type",
                  "budgetRollover": "budget_rollover", "advertiserId": "advertiser_id"}
        sets, params = [], []
        for gql_key, col in fields.items():
            if gql_key in inp:
                sets.append(f"{col} = %s")
                params.append(inp[gql_key])
        if sets:
            params.append(gid)
            _q(conn, f"UPDATE sa_campaign_groups SET {', '.join(sets)} WHERE id = %s",
               tuple(params))
            conn.commit()
        row = _one(conn, "SELECT * FROM sa_campaign_groups WHERE id = %s", (gid,))
        return _cg_payload("updateCampaignGroup", _campaign_group_node(row), [])

    # create — requiredForCreate: name, advertiserId, flights
    missing = [f for f in ("name", "advertiserId") if not inp.get(f)]
    if missing:
        return _cg_payload("createCampaignGroup", None,
                           [_user_error(f"{f} is required.", ["input", f]) for f in missing])
    new_id = f"sa-g-{uuid.uuid4().hex[:12]}"
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sa_campaign_groups
          (id, name, advertiser_id, status, budget_type, budget_rollover,
           is_archived, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (new_id, inp["name"], inp["advertiserId"], "ACTIVE",
         inp.get("budgetType", "COST"), bool(inp.get("budgetRollover")),
         False, now))
    conn.commit()
    row = _one(conn, "SELECT * FROM sa_campaign_groups WHERE id = %s", (new_id,))
    return _cg_payload("createCampaignGroup", _campaign_group_node(row), [])


def _cg_payload(op, node, user_errors):
    return {"data": {op: {
        "campaignGroup": node, "clientMutationId": None, "userErrors": user_errors}}}


def _resolve_update_campaign_group(conn, variables):
    return _resolve_create_campaign_group(conn, variables, _update=True)


def _resolve_create_advertiser(conn, variables, _update=False):
    inp = variables.get("input") or {}
    aid = inp.get("id")
    if _update:
        if not aid:
            return _adv_payload("updateAdvertiser", None,
                                [_user_error("id is required for update.", ["input", "id"])])
        row = _one(conn, "SELECT * FROM sa_advertisers WHERE id = %s", (aid,))
        if not row:
            return _adv_payload("updateAdvertiser", None,
                                [_user_error(f"Advertiser not found: {aid}", ["input", "id"])])
        sets, params = [], []
        for gql_key, col in (("name", "name"), ("description", "description")):
            if gql_key in inp:
                sets.append(f"{col} = %s")
                params.append(inp[gql_key])
        if sets:
            params.append(aid)
            _q(conn, f"UPDATE sa_advertisers SET {', '.join(sets)} WHERE id = %s",
               tuple(params))
            conn.commit()
        row = _one(conn, "SELECT * FROM sa_advertisers WHERE id = %s", (aid,))
        return _adv_payload("updateAdvertiser", _advertiser_node(row), [])

    if not inp.get("name"):
        return _adv_payload("createAdvertiser", None,
                            [_user_error("name is required.", ["input", "name"])])
    new_id = f"sa-adv-{uuid.uuid4().hex[:10]}"
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sa_advertisers (id, name, description, is_archived)
        VALUES (%s,%s,%s,%s)""",
        (new_id, inp["name"], inp.get("description"), False))
    conn.commit()
    row = _one(conn, "SELECT * FROM sa_advertisers WHERE id = %s", (new_id,))
    return _adv_payload("createAdvertiser", _advertiser_node(row), [])


def _adv_payload(op, node, user_errors):
    return {"data": {op: {
        "advertiser": node, "clientMutationId": None, "userErrors": user_errors}}}


def _resolve_update_advertiser(conn, variables):
    return _resolve_create_advertiser(conn, variables, _update=True)


def _bulk_campaign_state(conn, variables, op, status, count_key, is_archived=None):
    """pauseCampaigns / resumeCampaigns / archiveCampaigns — skip-and-report."""
    inp = variables.get("input") or {}
    ids = inp.get("ids") or ([inp["id"]] if inp.get("id") else [])
    if not ids:
        return {"data": {op: {count_key: 0, "campaigns": [],
                              "clientMutationId": inp.get("clientMutationId"),
                              "userErrors": [_user_error("ids is required.", ["input", "ids"])]}}}
    changed, user_errors = [], []
    for cid in ids:
        row = _one(conn, "SELECT * FROM sa_campaigns WHERE id = %s", (cid,))
        if not row:
            user_errors.append(_user_error(f"Campaign not found: {cid}", ["input", "ids"]))
            continue
        sets, params = ["status = %s"], [status]
        if is_archived is not None:
            sets.append("is_archived = %s")
            params.append(is_archived)
        params.append(cid)
        _q(conn, f"UPDATE sa_campaigns SET {', '.join(sets)} WHERE id = %s", tuple(params))
        changed.append(cid)
    conn.commit()
    nodes = [_campaign_node(_one(conn, "SELECT * FROM sa_campaigns WHERE id = %s", (c,)))
             for c in changed]
    return {"data": {op: {count_key: len(changed), "campaigns": nodes,
                          "clientMutationId": inp.get("clientMutationId"),
                          "userErrors": user_errors}}}


def _resolve_pause_campaigns(conn, variables):
    return _bulk_campaign_state(conn, variables, "pauseCampaigns", "PAUSED", "pausedCount")


def _resolve_resume_campaigns(conn, variables):
    return _bulk_campaign_state(conn, variables, "resumeCampaigns", "ACTIVE", "resumedCount")


def _resolve_archive_campaigns(conn, variables):
    return _bulk_campaign_state(conn, variables, "archiveCampaigns", "ARCHIVED",
                                "archivedCount", is_archived=True)


_RESOLVERS = {
    # queries — campaign management
    "campaigns": _resolve_campaigns,
    "campaign": _resolve_campaign,
    "campaignGroups": _resolve_campaign_groups,
    "campaignGroup": _resolve_campaign_group,
    "advertisers": _resolve_advertisers,
    "advertiser": _resolve_advertiser,
    # queries — pull delivery reporting
    "campaignDelivery": _resolve_campaign_delivery,
    "campaignDeliveryAsync": _resolve_campaign_delivery,
    "campaignGroupDelivery": _resolve_campaign_group_delivery,
    "campaignGroupDeliveryAsync": _resolve_campaign_group_delivery,
    "advertiserDelivery": _resolve_advertiser_delivery,
    "advertiserDeliveryAsync": _resolve_advertiser_delivery,
    # mutations — order pushes / campaign management
    "upsertCampaign": _resolve_upsert_campaign,
    "createCampaignGroup": _resolve_create_campaign_group,
    "updateCampaignGroup": _resolve_update_campaign_group,
    "createAdvertiser": _resolve_create_advertiser,
    "updateAdvertiser": _resolve_update_advertiser,
    "pauseCampaigns": _resolve_pause_campaigns,
    "resumeCampaigns": _resolve_resume_campaigns,
    "archiveCampaigns": _resolve_archive_campaigns,
}


# ── The single GraphQL endpoint ─────────────────────────────────────────────────

@router.post("/graphql")
def graphql(body: dict = Body(...),
            x_authorization: Optional[str] = Header(None, alias="X-Authorization"),
            conn=Depends(get_db)):
    """Dispatch a GraphQL request to the matching mock resolver."""
    query = body.get("query", "")
    variables = body.get("variables") or {}
    operation_name = body.get("operationName")

    op = _detect_operation(query, operation_name)
    if not op:
        return _gql_error("Could not determine the GraphQL operation.", "BAD_REQUEST")

    resolver = _RESOLVERS.get(op)
    if not resolver:
        return _gql_error(
            f"Unknown or unsupported operation: {op}. Supported: "
            f"{', '.join(sorted(_RESOLVERS))}", "NOT_IMPLEMENTED")
    args = _bind_arguments(query, op, variables)
    return resolver(conn, args)


@router.get("/graphql")
def graphql_introspection_hint():
    """GET is not a GraphQL transport here — point callers at POST."""
    return {
        "message": "StackAdapt GraphQL mock. POST a {query, variables, "
                   "operationName} body to /stackadapt/graphql.",
        "supportedOperations": sorted(_RESOLVERS.keys()),
    }
