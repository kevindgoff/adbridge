"""Storage facts about the Hivestack v2 tables, shared by the seed/import code
(app/database.py) and the routes (app/routes/hivestack.py).

Kept in its own module because the routes already import app.database.
"""

# Columns holding JSON text: nested objects/arrays, and fields whose type the
# live sample never revealed (always null). Written with json.dumps, read with json.loads.
_UNIT_JSON = ["blacklist_unique_advertiser_ids", "facing_direction", "image_uri",
              "impression_verifier_vendor_external_id", "location", "media_type", "physical_unit_count"]

JSON_FIELDS = {
    "hs_accounts": [],
    "hs_advertisers": ["external_id", "unique_advertiser"],
    "hs_campaigns": ["campaign_properties", "cap", "pacing_budget", "pacing_budget_mode", "pacing_strategy",
                     "priority"],
    "hs_lineitems": ["cpp", "creative_ids", "demographic_ids", "external_id", "lineitem_properties",
                     "max_bid_cpm", "mediaowner_ids", "unit_ids", "cpm_net"],
    "hs_creatives": ["creative_files", "iab_categories", "inventory_targeting", "language_ids",
                     "last_played_time_utc"],
    "hs_lineitem_deal_creatives": ["local_from_date", "local_to_date"],
    "hs_networks": ["loop_length", "spot_length"],
    "hs_units": _UNIT_JSON,
    "hs_demographics": ["organization_id"],
    "hs_report_definitions": ["definition_json", "exchange_id"],
    "hs_report_executions": ["status_message"],
}

# Captured files the importer understands: (file stem, table, primary key),
# in load order (parents before children).
CAPTURE_ENTITIES = [
    ("locations", "hs_locations", "location_id"),
    ("networks", "hs_networks", "network_id"),
    ("sites", "hs_sites", "site_id"),
    ("units", "hs_units", "unit_id"),
    ("accounts", "hs_accounts", "account_id"),
    ("advertisers", "hs_advertisers", "advertiser_id"),
]

# Every current v2 table (the RTB tables hs_publishers, hs_screens, hs_deals,
# hs_bid_requests, hs_impressions, hs_custom_events, hs_event_data are not included).
V2_TABLES = [
    "hs_accounts", "hs_advertisers", "hs_campaigns", "hs_lineitems", "hs_creatives",
    "hs_creative_approvals", "hs_lineitem_creatives", "hs_lineitem_deal_creatives",
    "hs_networks", "hs_sites", "hs_units", "hs_unitpacks", "hs_unitpack_units", "hs_unitpack_targeting",
    "hs_demographics", "hs_languages", "hs_locations", "hs_mediatypes", "hs_unit_languages",
    "hs_unit_properties", "hs_concentrations", "hs_report_definitions", "hs_report_executions",
]
