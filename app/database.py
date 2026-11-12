import uuid
import os
import json
import random
import re
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ─── Delegate get_db / init_db to the backend abstraction ───────────────────
from app.db_backend import get_db, init_db  # noqa: F401 — re-exported for all importers
from app.hivestack_model import CAPTURE_ENTITIES, JSON_FIELDS


def _uuid():
    return str(uuid.uuid4())


def _now():
    return datetime.utcnow().isoformat() + "Z"


def _past_date(days_ago):
    return (datetime.utcnow() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def _future_date(days_ahead):
    return (datetime.utcnow() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agencies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    dsp_advertiser_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clients (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    billing_name TEXT,
    contact_first_name TEXT,
    contact_last_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    contact_extension TEXT,
    notes TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS brands (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    client_id TEXT NOT NULL REFERENCES clients(id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS verticals (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS brand_verticals (
    brand_id TEXT NOT NULL REFERENCES brands(id),
    vertical_id TEXT NOT NULL REFERENCES verticals(id),
    subvertical TEXT,
    PRIMARY KEY (brand_id, vertical_id)
);

CREATE TABLE IF NOT EXISTS kpis (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    goal_type TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS campaigns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    ugcid TEXT,
    initiative_name TEXT,
    status TEXT NOT NULL DEFAULT 'live',
    approved_budget REAL,
    start_date TEXT,
    end_date TEXT,
    client_id TEXT NOT NULL REFERENCES clients(id),
    brand_id TEXT REFERENCES brands(id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS campaign_kpis (
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    kpi_id TEXT NOT NULL REFERENCES kpis(id),
    goal_value REAL,
    PRIMARY KEY (campaign_id, kpi_id)
);

CREATE TABLE IF NOT EXISTS line_items (
    id TEXT PRIMARY KEY,
    lineage TEXT,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    property_id TEXT REFERENCES properties(id),
    vendor_id TEXT REFERENCES vendors(id),
    name TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'dsp',
    media_plan TEXT,
    start_date TEXT,
    end_date TEXT,
    ad_sizes TEXT,
    formats TEXT,
    platforms TEXT,
    rate_type TEXT,
    total_spend_contracted REAL,
    amount_gain_loss_contracted REAL,
    margin_pct_contracted REAL,
    media_rate REAL,
    media_contracted_units INTEGER,
    media_spend_contracted REAL,
    ad_serving_rate_type TEXT,
    ad_server TEXT,
    ad_serving_cost REAL,
    ad_serving_rate REAL,
    ad_serving_estimated_clicks INTEGER,
    ad_serving_estimated_impressions INTEGER,
    ad_serving_spend_contracted REAL,
    kpi_id TEXT REFERENCES kpis(id),
    kpi_goal REAL,
    advertising_channel TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS addons (
    id TEXT PRIMARY KEY,
    lineage TEXT,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    name TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    addon_spend_contracted REAL,
    addon_spend REAL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS vendors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS properties (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT,
    url TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS property_verticals (
    property_id TEXT NOT NULL REFERENCES properties(id),
    vertical_id TEXT NOT NULL REFERENCES verticals(id),
    PRIMARY KEY (property_id, vertical_id)
);

CREATE TABLE IF NOT EXISTS creatives (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    external_source TEXT,
    external_ref TEXT,
    media_type TEXT,
    ad_classification TEXT,
    pixel_size TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conversions (
    conversion_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    external_source TEXT,
    external_ref TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    external_source TEXT,
    external_id TEXT,
    size TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS groups_ (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    budget_amount REAL,
    budget_type TEXT,
    flight_start TEXT,
    flight_end TEXT,
    even_delivery BOOLEAN DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'active',
    pacing_control_level TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tactics (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    group_id INTEGER NOT NULL REFERENCES groups_(id),
    budget_amount REAL,
    budget_type TEXT,
    flight_start TEXT,
    flight_end TEXT,
    pacing_priority TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    default_bid REAL,
    created_at TEXT NOT NULL
);

-- ==================== DV360 Tables ====================

CREATE TABLE IF NOT EXISTS dv360_partners (
    partner_id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL,
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_advertisers (
    advertiser_id INTEGER PRIMARY KEY,
    partner_id INTEGER NOT NULL REFERENCES dv360_partners(partner_id),
    display_name TEXT NOT NULL,
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    currency_code TEXT DEFAULT 'USD',
    domain_url TEXT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_campaigns (
    campaign_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES dv360_advertisers(advertiser_id),
    display_name TEXT NOT NULL,
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    campaign_goal_type TEXT,
    performance_goal_type TEXT,
    performance_goal_amount_micros BIGINT,
    campaign_flight_start TEXT,
    campaign_flight_end TEXT,
    frequency_cap_max_impressions INTEGER,
    frequency_cap_time_unit TEXT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_insertion_orders (
    insertion_order_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES dv360_advertisers(advertiser_id),
    campaign_id INTEGER NOT NULL REFERENCES dv360_campaigns(campaign_id),
    display_name TEXT NOT NULL,
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    pacing_type TEXT DEFAULT 'PACING_TYPE_EVEN',
    budget_type TEXT DEFAULT 'BUDGET_TYPE_FIXED',
    budget_amount_micros BIGINT,
    budget_unit TEXT DEFAULT 'BUDGET_UNIT_CURRENCY',
    flight_start TEXT,
    flight_end TEXT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_line_items (
    line_item_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES dv360_advertisers(advertiser_id),
    campaign_id INTEGER NOT NULL REFERENCES dv360_campaigns(campaign_id),
    insertion_order_id INTEGER REFERENCES dv360_insertion_orders(insertion_order_id),
    display_name TEXT NOT NULL,
    line_item_type TEXT NOT NULL DEFAULT 'LINE_ITEM_TYPE_DISPLAY_DEFAULT',
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    flight_start TEXT,
    flight_end TEXT,
    budget_type TEXT DEFAULT 'LINE_ITEM_BUDGET_ALLOCATION_TYPE_FIXED',
    budget_amount_micros BIGINT,
    pacing_type TEXT DEFAULT 'PACING_TYPE_EVEN',
    frequency_cap_max_impressions INTEGER,
    frequency_cap_time_unit TEXT,
    bid_strategy_type TEXT DEFAULT 'BIDDING_STRATEGY_FIXED_CPM',
    bid_amount_micros BIGINT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_ad_groups (
    ad_group_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES dv360_advertisers(advertiser_id),
    line_item_id INTEGER NOT NULL REFERENCES dv360_line_items(line_item_id),
    display_name TEXT NOT NULL,
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    ad_group_format TEXT DEFAULT 'AD_GROUP_FORMAT_IN_STREAM',
    bid_strategy_type TEXT,
    bid_amount_micros BIGINT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_ad_group_ads (
    ad_group_ad_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES dv360_advertisers(advertiser_id),
    ad_group_id INTEGER NOT NULL REFERENCES dv360_ad_groups(ad_group_id),
    display_name TEXT NOT NULL,
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    ad_url TEXT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_creatives (
    creative_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES dv360_advertisers(advertiser_id),
    display_name TEXT NOT NULL,
    entity_status TEXT NOT NULL DEFAULT 'ENTITY_STATUS_ACTIVE',
    creative_type TEXT NOT NULL DEFAULT 'CREATIVE_TYPE_STANDARD',
    hosting_source TEXT DEFAULT 'HOSTING_SOURCE_CM',
    dimensions_width INTEGER,
    dimensions_height INTEGER,
    approval_status TEXT DEFAULT 'APPROVAL_STATUS_PENDING_REVIEW',
    review_status TEXT DEFAULT 'REVIEW_STATUS_UNSPECIFIED',
    exit_url TEXT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_targeting_options (
    targeting_option_id TEXT PRIMARY KEY,
    targeting_type TEXT NOT NULL,
    display_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_assigned_targeting (
    id SERIAL PRIMARY KEY,
    assigned_targeting_option_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    advertiser_id INTEGER NOT NULL,
    targeting_type TEXT NOT NULL,
    targeting_option_id TEXT,
    display_name TEXT
);

CREATE TABLE IF NOT EXISTS dv360_channels (
    channel_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER REFERENCES dv360_advertisers(advertiser_id),
    partner_id INTEGER REFERENCES dv360_partners(partner_id),
    display_name TEXT NOT NULL,
    positively_targeted_line_item_count INTEGER DEFAULT 0,
    negatively_targeted_line_item_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dv360_inventory_sources (
    inventory_source_id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL,
    inventory_source_type TEXT DEFAULT 'INVENTORY_SOURCE_TYPE_PRIVATE',
    exchange TEXT,
    status TEXT DEFAULT 'INVENTORY_SOURCE_STATUS_ACTIVE',
    deal_id TEXT,
    rate_type TEXT,
    rate_amount_micros BIGINT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv360_floodlight_groups (
    floodlight_group_id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL,
    lookback_window_click_days INTEGER DEFAULT 30,
    lookback_window_impression_days INTEGER DEFAULT 30,
    web_tag_type TEXT DEFAULT 'WEB_TAG_TYPE_FLOODLIGHT'
);

CREATE TABLE IF NOT EXISTS dv360_floodlight_activities (
    floodlight_activity_id INTEGER PRIMARY KEY,
    floodlight_group_id INTEGER NOT NULL REFERENCES dv360_floodlight_groups(floodlight_group_id),
    display_name TEXT NOT NULL,
    serving_status TEXT DEFAULT 'FLOODLIGHT_ACTIVITY_SERVING_STATUS_ENABLED',
    advertiser_ids TEXT
);

-- Bid Manager-style delivery reporting: daily per-entity delivery metrics.
CREATE TABLE IF NOT EXISTS dv360_delivery_stats (
    id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES dv360_advertisers(advertiser_id),
    entity_level TEXT NOT NULL,              -- campaign | insertion_order | line_item
    entity_id INTEGER NOT NULL,
    date TEXT NOT NULL,                       -- YYYY-MM-DD
    impressions BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    revenue_micros BIGINT NOT NULL DEFAULT 0, -- spend/revenue in micros of advertiser currency
    conversions BIGINT NOT NULL DEFAULT 0,
    video_completions BIGINT NOT NULL DEFAULT 0
);

-- Bid Manager-style persisted queries (reporting definitions).
CREATE TABLE IF NOT EXISTS dv360_queries (
    query_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    metrics TEXT,                             -- JSON array of metric names
    group_bys TEXT,                           -- JSON array of group-by dimensions
    date_range_start TEXT,                    -- YYYY-MM-DD
    date_range_end TEXT,                      -- YYYY-MM-DD
    created_time TEXT NOT NULL
);

-- ==================== Triton Booking (TAP) Tables ====================

CREATE TABLE IF NOT EXISTS tap_advertisers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT,
    iab_category_code TEXT,
    currency_iso_code TEXT DEFAULT 'USD',
    external_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    agency_name TEXT,
    agency_external_id TEXT,
    ad_separation_override INTEGER,
    reseller_id INTEGER,
    type TEXT DEFAULT 'direct',
    created_by TEXT,
    creation_time TEXT NOT NULL,
    last_updated_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tap_billing (
    id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL UNIQUE REFERENCES tap_advertisers(id),
    billing_contact TEXT,
    billing_email TEXT,
    billing_address TEXT,
    payment_terms TEXT DEFAULT 'net30',
    creation_time TEXT NOT NULL,
    last_updated_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tap_campaigns (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    advertiser_id INTEGER NOT NULL REFERENCES tap_advertisers(id),
    status TEXT NOT NULL DEFAULT 'draft',
    external_id TEXT,
    notes TEXT,
    team_id INTEGER,
    start_date TEXT,
    end_date TEXT,
    account_exec TEXT,
    trafficked_by TEXT,
    created_by TEXT,
    creation_time TEXT NOT NULL,
    last_updated_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tap_flights (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    campaign_id INTEGER NOT NULL REFERENCES tap_campaigns(id),
    advertiser_id INTEGER NOT NULL REFERENCES tap_advertisers(id),
    status TEXT NOT NULL DEFAULT 'draft',
    type TEXT DEFAULT 'direct',
    start_date TEXT,
    end_date TEXT,
    pricing_model TEXT DEFAULT 'cpm',
    priority INTEGER DEFAULT 8,
    position TEXT DEFAULT 'preroll',
    delivery_method TEXT DEFAULT 'live',
    pacing TEXT DEFAULT 'even',
    run_on_network BOOLEAN DEFAULT FALSE,
    open_measurement_enabled BOOLEAN DEFAULT FALSE,
    goal_impressions INTEGER,
    goal_spots INTEGER,
    external_id TEXT,
    account_exec TEXT,
    trafficked_by TEXT,
    delivery_status TEXT DEFAULT 'pending',
    created_by TEXT,
    creation_time TEXT NOT NULL,
    last_updated_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tap_creatives (
    id SERIAL PRIMARY KEY,
    flight_id INTEGER NOT NULL UNIQUE REFERENCES tap_flights(id),
    creative_url TEXT,
    ad_duration_in_seconds INTEGER,
    status TEXT DEFAULT 'active',
    overtone_id TEXT,
    deal_id TEXT,
    reseller_contract_id TEXT,
    creation_time TEXT NOT NULL,
    last_updated_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tap_cappings (
    id SERIAL PRIMARY KEY,
    flight_id INTEGER NOT NULL REFERENCES tap_flights(id),
    type TEXT NOT NULL,
    limit_value INTEGER NOT NULL,
    period TEXT,
    creation_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tap_audits (
    id SERIAL PRIMARY KEY,
    object_id INTEGER NOT NULL,
    object_type TEXT NOT NULL,
    user_email TEXT,
    impersonated_by TEXT,
    creation_time TEXT NOT NULL
);

-- ==================== Triton Metrics Tables ====================

CREATE TABLE IF NOT EXISTS triton_reports (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS triton_report_data (
    id SERIAL PRIMARY KEY,
    report_id TEXT NOT NULL REFERENCES triton_reports(id),
    date TEXT NOT NULL,
    station_name TEXT,
    market TEXT,
    country TEXT,
    sessions INTEGER DEFAULT 0,
    total_listening_hours REAL DEFAULT 0.0,
    avg_listening_duration REAL DEFAULT 0.0,
    cume INTEGER DEFAULT 0,
    impressions INTEGER DEFAULT 0,
    downloads INTEGER DEFAULT 0,
    unique_listeners INTEGER DEFAULT 0,
    peak_listeners INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stats (
    id SERIAL PRIMARY KEY,
    line_item_id TEXT REFERENCES line_items(id),
    line_item_lineage_id TEXT,
    campaign_id TEXT REFERENCES campaigns(id),
    delivery_source_id TEXT REFERENCES delivery_sources(id),
    creative_id TEXT REFERENCES creatives(id),
    conversion_id TEXT REFERENCES conversions(conversion_id),
    external_ad_ref TEXT,
    date TEXT NOT NULL,
    delivered_impressions INTEGER DEFAULT 0,
    delivered_impressions_raw REAL DEFAULT 0,
    delivered_clicks INTEGER DEFAULT 0,
    delivered_eligible_impressions INTEGER DEFAULT 0,
    delivered_measurable_impressions INTEGER DEFAULT 0,
    delivered_viewable_impressions INTEGER DEFAULT 0,
    delivered_interactions INTEGER DEFAULT 0,
    delivered_video_starts INTEGER DEFAULT 0,
    delivered_video_completions INTEGER DEFAULT 0,
    delivered_total_view_conversions INTEGER DEFAULT 0,
    delivered_total_click_conversions INTEGER DEFAULT 0,
    delivered_total_conversions INTEGER DEFAULT 0,
    delivered_cpa_view_conversions REAL DEFAULT 0,
    delivered_cpa_click_conversions REAL DEFAULT 0,
    delivered_cpa_total_conversions REAL DEFAULT 0,
    delivered_inventory_spend REAL DEFAULT 0,
    delivered_data_spend REAL DEFAULT 0,
    delivered_units INTEGER DEFAULT 0,
    total_spend REAL DEFAULT 0,
    media_spend REAL DEFAULT 0,
    ad_serving_spend REAL DEFAULT 0,
    auctions_won INTEGER DEFAULT 0,
    click_conversions INTEGER DEFAULT 0,
    click_conversion_revenue REAL DEFAULT 0,
    view_conversions INTEGER DEFAULT 0,
    view_conversion_revenue REAL DEFAULT 0,
    total_conversions INTEGER DEFAULT 0,
    total_conversion_revenue REAL DEFAULT 0
);

-- ==================== Hivestack (OpenRTB 2.5 DOOH) Tables ====================
-- RTB / tracking tables (shape owned by the mock, not covered by the v2 spec)

CREATE TABLE IF NOT EXISTS hs_publishers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT NOT NULL,
    seller_type TEXT NOT NULL DEFAULT 'PUBLISHER',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_screens (
    id TEXT PRIMARY KEY,
    screen_id TEXT NOT NULL,
    publisher_id TEXT NOT NULL REFERENCES hs_publishers(id),
    publisher_name TEXT NOT NULL,
    publisher_domain TEXT NOT NULL,
    display_manager TEXT,
    display_manager_ver TEXT,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    supports_banner BOOLEAN DEFAULT TRUE,
    supports_video BOOLEAN DEFAULT TRUE,
    audio INTEGER DEFAULT 0,
    venue_type_id INTEGER,
    geo_lat REAL,
    geo_lon REAL,
    geo_country TEXT,
    geo_region TEXT,
    geo_city TEXT,
    geo_zip TEXT,
    geo_utcoffset INTEGER,
    ifa TEXT,
    network_id TEXT,
    site_id TEXT,
    aspect_ratio_tolerance REAL DEFAULT 0.1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_deals (
    id TEXT PRIMARY KEY,
    publisher_id TEXT NOT NULL REFERENCES hs_publishers(id),
    name TEXT NOT NULL,
    bidfloor REAL NOT NULL DEFAULT 0.0,
    bidfloorcur TEXT NOT NULL DEFAULT 'USD',
    deal_type INTEGER NOT NULL DEFAULT 0,
    must_bid INTEGER NOT NULL DEFAULT 0,
    wseat TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_bid_requests (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_impressions (
    id SERIAL PRIMARY KEY,
    bid_request_id TEXT,
    event_type TEXT NOT NULL,
    total_imp REAL,
    target_imp REAL,
    price REAL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_custom_events (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_event_data (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES hs_custom_events(id),
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    updated_at TEXT NOT NULL,
    UNIQUE (event_id, entity_type, entity_id)
);

-- Hivestack API v2 resources — mirror app/specs/hivestack.openapi.json.
-- Column names equal the live API field names so real GET /api/v2/* records
-- can be inserted as-is. Integer primary keys are the live <resource>_id.
-- Nested objects/arrays, and fields whose type the live sample could not
-- determine (always null), are stored as JSON text. No foreign keys, so a
-- partial import of live data never fails on a missing parent.

CREATE TABLE IF NOT EXISTS hs_accounts (
    account_id INTEGER PRIMARY KEY,
    external_id TEXT,
    logo_url TEXT,
    name TEXT NOT NULL,
    organization_id INTEGER
);

CREATE TABLE IF NOT EXISTS hs_advertisers (
    advertiser_id INTEGER PRIMARY KEY,
    created_on_utc TEXT,
    external_id TEXT,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    organization_id INTEGER,
    unique_advertiser TEXT
);

CREATE TABLE IF NOT EXISTS hs_campaigns (
    campaign_id INTEGER PRIMARY KEY,
    account_id INTEGER,
    advertiser_id INTEGER,
    budget DOUBLE PRECISION,
    budget_cap DOUBLE PRECISION,
    campaign_properties TEXT,
    campaign_type TEXT,
    cap TEXT,
    created_on_utc TEXT,
    end_date TEXT,
    min_seconds_between_plays INTEGER,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    notes TEXT,
    organization_id INTEGER,
    pacing_budget TEXT,
    pacing_budget_mode TEXT,
    pacing_strategy TEXT,
    priority TEXT,
    start_date TEXT
);

CREATE TABLE IF NOT EXISTS hs_lineitems (
    lineitem_id INTEGER PRIMARY KEY,
    ad_duration INTEGER,
    budget DOUBLE PRECISION,
    budget_mode TEXT,
    cadence DOUBLE PRECISION,
    cadence_smooth_catchup BOOLEAN,
    campaign_id INTEGER,
    cancelled_at TEXT,
    cap DOUBLE PRECISION,
    cpp TEXT,
    created_at TEXT,
    created_by TEXT,
    created_on_utc TEXT,
    creative_ids TEXT,
    daypart TEXT,
    delivery_curve TEXT,
    demographic_ids TEXT,
    external_id TEXT,
    flight_from TEXT,
    flight_to TEXT,
    is_cpm_from_rate_card BOOLEAN,
    lineitem_properties TEXT,
    market_targeted_impressions INTEGER,
    max_bid_cpm TEXT,
    mediaowner_ids TEXT,
    min_concentration_factor DOUBLE PRECISION,
    min_seconds_between_plays INTEGER,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    notes TEXT,
    package_id INTEGER,
    priority INTEGER,
    purchase_type TEXT,
    run_state TEXT,
    state TEXT,
    submitted_at TEXT,
    unit_ids TEXT,
    cpm_net TEXT,
    deal_id TEXT
);

CREATE TABLE IF NOT EXISTS hs_creatives (
    creative_id INTEGER PRIMARY KEY,
    advertiser_id INTEGER,
    created_at TEXT,
    creative_files TEXT,
    daypart TEXT,
    iab_categories TEXT,
    inventory_targeting TEXT,
    language_ids TEXT,
    name TEXT NOT NULL,
    organization_id INTEGER,
    retired_at TEXT,
    run_state TEXT,
    state TEXT,
    submitted_at TEXT,
    uuid TEXT,
    last_played_time_utc TEXT
);

CREATE TABLE IF NOT EXISTS hs_creative_approvals (
    id TEXT PRIMARY KEY,
    creative_id INTEGER NOT NULL,
    media_owner_id TEXT,
    approval_status TEXT NOT NULL DEFAULT 'pending',
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_lineitem_creatives (
    lineitemcreative_id INTEGER PRIMARY KEY,
    lineitem_id INTEGER NOT NULL,
    creative_id INTEGER NOT NULL,
    local_from_date TEXT,
    local_to_date TEXT,
    weight INTEGER
);

CREATE TABLE IF NOT EXISTS hs_lineitem_deal_creatives (
    lineitem_id INTEGER NOT NULL,
    creative_id INTEGER NOT NULL,
    deal_id TEXT NOT NULL,
    deal_flight_from TEXT,
    deal_flight_to TEXT,
    local_from_date TEXT,
    local_to_date TEXT,
    PRIMARY KEY (lineitem_id, creative_id, deal_id)
);

CREATE TABLE IF NOT EXISTS hs_networks (
    network_id INTEGER PRIMARY KEY,
    code TEXT,
    created_on_utc TEXT,
    loop_length TEXT,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    owner_id INTEGER,
    spot_length TEXT
);

CREATE TABLE IF NOT EXISTS hs_sites (
    site_id INTEGER PRIMARY KEY,
    active BOOLEAN,
    created_on_utc TEXT,
    description TEXT,
    external_id TEXT,
    latitude DOUBLE PRECISION,
    location_id INTEGER,
    longitude DOUBLE PRECISION,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    owner_id INTEGER,
    uuid TEXT
);

CREATE TABLE IF NOT EXISTS hs_units (
    unit_id INTEGER PRIMARY KEY,
    active BOOLEAN,
    allow_audio BOOLEAN,
    allow_flash BOOLEAN,
    allow_html BOOLEAN,
    allow_image BOOLEAN,
    allow_video BOOLEAN,
    allow_zip BOOLEAN,
    available_for_adserver BOOLEAN,
    available_for_deals BOOLEAN,
    available_for_open_exchange BOOLEAN,
    available_for_storefront BOOLEAN,
    blacklist_unique_advertiser_ids TEXT,
    created_on_utc TEXT,
    description TEXT,
    enable_strict_iab_blacklisting BOOLEAN,
    enable_strict_iab_frequency_capping BOOLEAN,
    external_id TEXT,
    facing_direction TEXT,
    floor_cpm DOUBLE PRECISION,
    image_uri TEXT,
    impression_verifier_vendor TEXT,
    impression_verifier_vendor_external_id TEXT,
    latitude DOUBLE PRECISION,
    location TEXT,
    location_id INTEGER,
    longitude DOUBLE PRECISION,
    loop_length INTEGER,
    max_spot_length INTEGER,
    media_type TEXT,
    mediatype_id INTEGER,
    mediatype_name TEXT,
    min_seconds_between_ad_domain_plays INTEGER,
    min_seconds_between_creative_category_plays INTEGER,
    min_seconds_between_iab_category_plays INTEGER,
    min_spot_length INTEGER,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    network_id INTEGER,
    operating_hours TEXT,
    owner_id INTEGER,
    physical_screen_height_cm INTEGER,
    physical_screen_width_cm INTEGER,
    physical_unit_count TEXT,
    screen_height INTEGER,
    screen_width INTEGER,
    site_id INTEGER,
    spot_length INTEGER,
    timezone TEXT,
    uuid TEXT,
    weekly_impressions INTEGER,
    weekly_traffic INTEGER,
    last_heard_from_seconds_ago DOUBLE PRECISION,
    last_heard_from_utc TEXT,
    last_played_on_seconds_ago DOUBLE PRECISION,
    last_played_on_utc TEXT,
    has_audience_data BOOLEAN
);

CREATE TABLE IF NOT EXISTS hs_unitpacks (
    unitpack_id INTEGER PRIMARY KEY,
    active BOOLEAN,
    created_on_utc TEXT,
    description TEXT,
    external_id TEXT,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    owner_id INTEGER,
    prepopulation_option TEXT
);

CREATE TABLE IF NOT EXISTS hs_unitpack_units (
    unitpack_id INTEGER NOT NULL,
    unit_id INTEGER NOT NULL,
    PRIMARY KEY (unitpack_id, unit_id)
);

CREATE TABLE IF NOT EXISTS hs_unitpack_targeting (
    id TEXT PRIMARY KEY,
    unitpack_id INTEGER NOT NULL,
    targeting_type TEXT,
    targeting_value TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_demographics (
    demographic_id INTEGER PRIMARY KEY,
    code TEXT,
    created_on_utc TEXT,
    modified_on_utc TEXT,
    name TEXT NOT NULL,
    organization_id TEXT,
    show_buyers BOOLEAN,
    type TEXT
);

CREATE TABLE IF NOT EXISTS hs_languages (
    language_id INTEGER PRIMARY KEY,
    code TEXT,
    name TEXT NOT NULL
);

-- hierarchy / archived / version are not in GET /locations. They feed the
-- location object embedded in units.
CREATE TABLE IF NOT EXISTS hs_locations (
    location_id INTEGER PRIMARY KEY,
    code TEXT,
    country_id INTEGER,
    latitude TEXT,
    level INTEGER,
    longitude TEXT,
    name TEXT NOT NULL,
    region_id INTEGER,
    hierarchy TEXT,
    archived BOOLEAN,
    version TEXT
);

CREATE TABLE IF NOT EXISTS hs_mediatypes (
    mediatype_id INTEGER PRIMARY KEY,
    archived BOOLEAN,
    code TEXT,
    enumeration_id INTEGER,
    name TEXT NOT NULL,
    parent_mediatype_id INTEGER
);

CREATE TABLE IF NOT EXISTS hs_unit_languages (
    unitlanguage_id INTEGER PRIMARY KEY,
    created_on_utc TEXT,
    language_id INTEGER,
    modified_on_utc TEXT,
    unit_id INTEGER
);

CREATE TABLE IF NOT EXISTS hs_unit_properties (
    unitproperty_id INTEGER PRIMARY KEY,
    created_on_utc TEXT,
    modified_on_utc TEXT,
    name TEXT,
    unit_id INTEGER,
    value TEXT
);

CREATE TABLE IF NOT EXISTS hs_concentrations (
    unit_id INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    audience_count REAL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (unit_id, day_of_week, hour)
);

CREATE TABLE IF NOT EXISTS hs_report_definitions (
    report_definition_id INTEGER PRIMARY KEY,
    definition_json TEXT,
    exchange_id TEXT,
    last_executed_on_utc TEXT,
    name TEXT NOT NULL,
    organization_id INTEGER
);

CREATE TABLE IF NOT EXISTS hs_report_executions (
    report_execution_id INTEGER PRIMARY KEY,
    execution_finished_time_utc TEXT,
    execution_started_time_utc TEXT,
    progress DOUBLE PRECISION,
    report_definition_id INTEGER,
    report_file_url TEXT,
    status_code TEXT,
    status_message TEXT
);

-- ==================== AdsWizz Domain API v9 Tables ====================

CREATE TABLE IF NOT EXISTS aw_agencies (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    contact TEXT,
    email TEXT,
    external_reference TEXT,
    currency TEXT DEFAULT 'USD',
    timezone TEXT DEFAULT 'UTC',
    margin REAL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_advertisers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT,
    contact TEXT,
    email TEXT,
    comments TEXT,
    external_reference TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    ad_clashing BOOLEAN DEFAULT FALSE,
    agency_id INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_orders (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    advertiser_id INTEGER NOT NULL REFERENCES aw_advertisers(id),
    start_date TEXT,
    end_date TEXT,
    objective_type TEXT DEFAULT 'IMPRESSIONS',
    objective_value INTEGER,
    objective_currency TEXT DEFAULT 'USD',
    objective_unlimited BOOLEAN DEFAULT FALSE,
    managed_objective BOOLEAN DEFAULT FALSE,
    comments TEXT,
    external_reference TEXT,
    deal_id TEXT,
    archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS aw_campaigns (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    campaign_type TEXT NOT NULL DEFAULT 'STANDARD',
    advertiser_id INTEGER NOT NULL REFERENCES aw_advertisers(id),
    order_id INTEGER REFERENCES aw_orders(id),
    status TEXT NOT NULL DEFAULT 'DRAFT',
    billing TEXT DEFAULT 'UNSOLD',
    start_date TEXT,
    end_date TEXT,
    revenue_type TEXT DEFAULT 'CPM',
    revenue_value REAL,
    revenue_currency TEXT DEFAULT 'USD',
    objective_type TEXT DEFAULT 'IMPRESSIONS',
    objective_value INTEGER,
    objective_unlimited BOOLEAN DEFAULT FALSE,
    pacing_type TEXT DEFAULT 'EVENLY',
    pacing_priority INTEGER DEFAULT 5,
    comments TEXT,
    external_reference TEXT,
    category_ids TEXT,
    ad_clashing BOOLEAN DEFAULT FALSE,
    inventory_deal_id TEXT,
    archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS aw_ads (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES aw_campaigns(id),
    name TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'AUDIO',
    subtype TEXT DEFAULT 'AUDIO',
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    included_in_objective BOOLEAN DEFAULT TRUE,
    weight INTEGER DEFAULT 1,
    comments TEXT,
    external_reference TEXT,
    tracking_type TEXT,
    tracking TEXT,
    ad_unit_id INTEGER DEFAULT 0,
    creative_file_name TEXT,
    duration_ms INTEGER,
    destination_url TEXT,
    missing_creative BOOLEAN DEFAULT FALSE,
    archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_publishers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    contact TEXT,
    website TEXT,
    email TEXT,
    description TEXT,
    timezone TEXT DEFAULT 'UTC',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_zones (
    id SERIAL PRIMARY KEY,
    publisher_id INTEGER NOT NULL REFERENCES aw_publishers(id),
    name TEXT NOT NULL,
    description TEXT,
    type TEXT NOT NULL DEFAULT 'AUDIO',
    format_id TEXT,
    width INTEGER,
    height INTEGER,
    duration_min REAL,
    duration_max REAL,
    comments TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_zone_groups (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    total_capping INTEGER,
    session_capping INTEGER,
    comments TEXT,
    archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_zone_group_zones (
    zone_group_id INTEGER NOT NULL REFERENCES aw_zone_groups(id),
    zone_id INTEGER NOT NULL REFERENCES aw_zones(id),
    PRIMARY KEY (zone_group_id, zone_id)
);

CREATE TABLE IF NOT EXISTS aw_categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    parent_id INTEGER REFERENCES aw_categories(id)
);

-- ==================== AdsWizz Domain API v9 additions ====================

CREATE TABLE IF NOT EXISTS aw_multipart_uploads (
    upload_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    parts INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'INITIATED',
    creative_identifier TEXT,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS aw_audiogram_creatives (
    id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES aw_advertisers(id),
    creative_name TEXT NOT NULL,
    audio_creative_identifier TEXT NOT NULL,
    audio_file_extension TEXT NOT NULL,
    display_creative_identifier TEXT NOT NULL,
    display_file_extension TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PROCESSING',
    created_at TEXT NOT NULL
);

-- ==================== AdsWizz order push additions ====================

-- Capping rules for orders, campaigns, and ads (owner_type: order | campaign | ad)
CREATE TABLE IF NOT EXISTS aw_capping_rules (
    id SERIAL PRIMARY KEY,
    owner_type TEXT NOT NULL,
    owner_id INTEGER NOT NULL,
    unit TEXT NOT NULL,
    unit_multiplier INTEGER NOT NULL,
    cap_limit INTEGER NOT NULL
);

-- One TargetingRulesContainer per ad. List and object fields are stored as JSON text.
CREATE TABLE IF NOT EXISTS aw_ad_targeting (
    ad_id INTEGER PRIMARY KEY REFERENCES aw_ads(id),
    zone_ids TEXT,
    zone_group_ids TEXT,
    delivery_instructions TEXT,
    rules TEXT,
    updated_at TEXT NOT NULL
);

-- ==================== The Trade Desk (TTD) v3 Tables ====================

CREATE TABLE IF NOT EXISTS ttd_advertisers (
    advertiser_id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL,
    advertiser_name TEXT NOT NULL,
    description TEXT,
    currency_code TEXT DEFAULT 'USD',
    domain_url TEXT,
    category_id TEXT,
    industry_id TEXT,
    status TEXT NOT NULL DEFAULT 'Available',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ttd_campaigns (
    campaign_id TEXT PRIMARY KEY,
    advertiser_id TEXT NOT NULL REFERENCES ttd_advertisers(advertiser_id),
    campaign_name TEXT NOT NULL,
    budget_amount REAL DEFAULT 0,
    daily_budget REAL DEFAULT 0,
    start_date TEXT,
    end_date TEXT,
    campaign_goal_type TEXT DEFAULT 'CPC',
    campaign_goal_value REAL DEFAULT 1.0,
    pacing_mode TEXT DEFAULT 'PaceEvenly',
    frequency_cap INTEGER,
    frequency_period TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ttd_campaign_flights (
    flight_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES ttd_campaigns(campaign_id),
    flight_name TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    budget_in_impressions INTEGER,
    daily_target_in_impressions INTEGER,
    budget_in_advertiser_currency REAL,
    daily_target_in_advertiser_currency REAL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ttd_ad_groups (
    ad_group_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES ttd_campaigns(campaign_id),
    ad_group_name TEXT NOT NULL,
    bid_amount REAL DEFAULT 5.0,
    bid_type TEXT DEFAULT 'CPM',
    is_enabled BOOLEAN DEFAULT TRUE,
    ad_format TEXT DEFAULT 'Display',
    start_date TEXT,
    end_date TEXT,
    frequency_cap INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ttd_creatives (
    creative_id TEXT PRIMARY KEY,
    advertiser_id TEXT NOT NULL REFERENCES ttd_advertisers(advertiser_id),
    creative_name TEXT NOT NULL,
    creative_type TEXT DEFAULT 'Banner',
    width INTEGER DEFAULT 300,
    height INTEGER DEFAULT 250,
    click_url TEXT,
    landing_page_url TEXT,
    ad_server_id TEXT,
    approval_status TEXT DEFAULT 'Pending',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ttd_tracking_tags (
    tracking_tag_id TEXT PRIMARY KEY,
    advertiser_id TEXT NOT NULL REFERENCES ttd_advertisers(advertiser_id),
    tag_name TEXT NOT NULL,
    tag_type TEXT DEFAULT 'Script',
    number_of_fires INTEGER DEFAULT 0,
    server_side BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL
);

-- ==================== Google Ad Manager (GAM) Tables ====================

CREATE TABLE IF NOT EXISTS gam_networks (
    network_code TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    network_id INTEGER NOT NULL,
    property_code TEXT,
    time_zone TEXT DEFAULT 'America/New_York',
    currency_code TEXT DEFAULT 'USD',
    effective_root_ad_unit_id INTEGER,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gam_companies (
    company_id INTEGER PRIMARY KEY,
    network_code TEXT NOT NULL REFERENCES gam_networks(network_code),
    display_name TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'ADVERTISER',
    external_id TEXT,
    address TEXT,
    email TEXT,
    comment TEXT,
    credit_status TEXT DEFAULT 'ACTIVE',
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gam_orders (
    order_id INTEGER PRIMARY KEY,
    network_code TEXT NOT NULL REFERENCES gam_networks(network_code),
    display_name TEXT NOT NULL,
    advertiser_id INTEGER NOT NULL REFERENCES gam_companies(company_id),
    trafficker TEXT,
    salesperson TEXT,
    status TEXT NOT NULL DEFAULT 'DRAFT',
    start_time TEXT,
    end_time TEXT,
    unlimited_end_time BOOLEAN DEFAULT FALSE,
    currency_code TEXT DEFAULT 'USD',
    external_order_id INTEGER,
    po_number TEXT,
    notes TEXT,
    archived BOOLEAN DEFAULT FALSE,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gam_line_items (
    line_item_id INTEGER PRIMARY KEY,
    network_code TEXT NOT NULL REFERENCES gam_networks(network_code),
    order_id INTEGER NOT NULL REFERENCES gam_orders(order_id),
    display_name TEXT NOT NULL,
    line_item_type TEXT NOT NULL DEFAULT 'STANDARD',
    priority INTEGER DEFAULT 8,
    status TEXT NOT NULL DEFAULT 'DRAFT',
    start_time TEXT,
    end_time TEXT,
    unlimited_end_time BOOLEAN DEFAULT FALSE,
    cost_type TEXT DEFAULT 'CPM',
    cost_amount_micros BIGINT DEFAULT 0,
    cost_currency_code TEXT DEFAULT 'USD',
    goal_type TEXT DEFAULT 'LIFETIME',
    goal_units BIGINT DEFAULT 0,
    delivered_impressions BIGINT DEFAULT 0,
    delivered_clicks BIGINT DEFAULT 0,
    creative_rotation_type TEXT DEFAULT 'EVEN',
    discount_percentage REAL DEFAULT 0,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gam_ad_units (
    ad_unit_id INTEGER PRIMARY KEY,
    network_code TEXT NOT NULL REFERENCES gam_networks(network_code),
    display_name TEXT NOT NULL,
    ad_unit_code TEXT,
    parent_ad_unit_id INTEGER,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    ad_unit_sizes TEXT,
    target_window TEXT DEFAULT 'BLANK',
    explicitly_targeted BOOLEAN DEFAULT FALSE,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gam_placements (
    placement_id INTEGER PRIMARY KEY,
    network_code TEXT NOT NULL REFERENCES gam_networks(network_code),
    display_name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    targeted_ad_unit_ids TEXT,
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gam_creatives (
    creative_id INTEGER PRIMARY KEY,
    network_code TEXT NOT NULL REFERENCES gam_networks(network_code),
    advertiser_id INTEGER NOT NULL REFERENCES gam_companies(company_id),
    display_name TEXT NOT NULL,
    creative_type TEXT NOT NULL DEFAULT 'THIRD_PARTY',
    width INTEGER,
    height INTEGER,
    preview_url TEXT,
    delivery_status TEXT DEFAULT 'ACTIVE',
    update_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gam_reports (
    report_id INTEGER PRIMARY KEY,
    network_code TEXT NOT NULL REFERENCES gam_networks(network_code),
    display_name TEXT NOT NULL,
    report_type TEXT DEFAULT 'HISTORICAL',
    status TEXT DEFAULT 'COMPLETED',
    start_date TEXT,
    end_date TEXT,
    update_time TEXT NOT NULL
);

-- ==================== Basis DSP (BasisUIL) Tables ====================

CREATE TABLE IF NOT EXISTS bn_advertisers (
    advertiser_id SERIAL PRIMARY KEY,
    company_name TEXT NOT NULL,
    currency_code TEXT DEFAULT 'USD',
    email TEXT,
    notes TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    active BOOLEAN DEFAULT TRUE,
    balance REAL DEFAULT 10000.0,
    max_budget_amount REAL DEFAULT 1000000.0,
    min_campaign_budget_amount REAL DEFAULT 2.0,
    active_campaign_limit INTEGER DEFAULT 3000,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bn_brands (
    brand_id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES bn_advertisers(advertiser_id),
    name TEXT NOT NULL,
    notes TEXT DEFAULT '',
    landing_page_domain TEXT,
    archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bn_campaign_groups (
    campaign_group_id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES bn_advertisers(advertiser_id),
    brand_id INTEGER NOT NULL REFERENCES bn_brands(brand_id),
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'online',
    kpi_type TEXT,
    kpi_value TEXT,
    budget_amount REAL DEFAULT 0,
    budget_type TEXT DEFAULT 'none',
    even_delivery_enabled BOOLEAN DEFAULT TRUE,
    flight_start TEXT,
    flight_end TEXT,
    pacing_setting TEXT DEFAULT 'CAMPAIGN',
    advertiser_spend_type TEXT,
    advertiser_spend_rate REAL DEFAULT 0,
    notes TEXT DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bn_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES bn_advertisers(advertiser_id),
    campaign_group_id INTEGER NOT NULL REFERENCES bn_campaign_groups(campaign_group_id),
    campaign_group_name TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'offline',
    default_bid REAL DEFAULT 1.0,
    max_bid REAL,
    notes TEXT DEFAULT '',
    budget_amount REAL DEFAULT 25.0,
    budget_type TEXT DEFAULT 'daily',
    even_delivery_enabled BOOLEAN DEFAULT TRUE,
    impression_cap INTEGER,
    impression_cap_type TEXT DEFAULT 'none',
    flight_start TEXT,
    flight_end TEXT,
    campaign_type TEXT DEFAULT 'advanced',
    enabled_rop BOOLEAN DEFAULT TRUE,
    enable_cross_device BOOLEAN DEFAULT FALSE,
    review_status TEXT DEFAULT 'eligible',
    created TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bn_creatives (
    creative_id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES bn_advertisers(advertiser_id),
    brand_id INTEGER NOT NULL REFERENCES bn_brands(brand_id),
    name TEXT NOT NULL,
    creative_type TEXT DEFAULT 'display',
    width INTEGER,
    height INTEGER,
    status TEXT DEFAULT 'active',
    landing_page_url TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bn_campaign_stats (
    id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES bn_advertisers(advertiser_id),
    campaign_id INTEGER NOT NULL REFERENCES bn_campaigns(campaign_id),
    date TEXT NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    spend REAL DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    ctr REAL DEFAULT 0,
    cpm REAL DEFAULT 0,
    cpc REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bn_reports (
    id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES bn_advertisers(advertiser_id),
    description TEXT,
    report_type TEXT NOT NULL DEFAULT 'BASIC',
    aggregation TEXT NOT NULL DEFAULT 'DAILY',
    from_date TEXT NOT NULL,
    to_date TEXT NOT NULL,
    entity_ids TEXT,
    entity_type TEXT DEFAULT 'CAMPAIGN',
    conversion_pixel_ids TEXT,
    timezone TEXT DEFAULT 'EST',
    status TEXT NOT NULL DEFAULT 'QUEUED',
    start_time TEXT,
    completion_time TEXT,
    expiry_time TEXT,
    url TEXT,
    schedule_id INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bn_report_schedules (
    id SERIAL PRIMARY KEY,
    advertiser_id INTEGER NOT NULL REFERENCES bn_advertisers(advertiser_id),
    name TEXT NOT NULL,
    report_type TEXT NOT NULL DEFAULT 'BASIC',
    aggregation TEXT NOT NULL DEFAULT 'DAILY',
    entity_ids TEXT,
    entity_type TEXT DEFAULT 'CAMPAIGN',
    conversion_pixel_ids TEXT,
    schedule_start_date TEXT NOT NULL,
    schedule_end_date TEXT NOT NULL,
    time_to_run_report TEXT DEFAULT '01:00',
    frequency TEXT NOT NULL DEFAULT 'WEEKLY',
    lookback_window INTEGER DEFAULT 7,
    lookback_type TEXT DEFAULT 'DAY',
    emails TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TEXT NOT NULL
);

-- ==================== Radio Workflow Partner API Tables ====================

CREATE TABLE IF NOT EXISTS rw_stations (
    core_id INTEGER PRIMARY KEY,
    core_description TEXT NOT NULL,
    call_letters TEXT,
    band_descriptor TEXT DEFAULT '',
    play_list_url TEXT,
    inventory_url TEXT,
    log_export_path TEXT,
    media_export_path TEXT,
    automation_id TEXT,
    automation_system INTEGER DEFAULT 0,
    automation_system_import INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rw_accounts (
    core_id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    disabled INTEGER DEFAULT 0,
    integration_id TEXT,
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    country TEXT DEFAULT 'US'
);

CREATE TABLE IF NOT EXISTS rw_talent (
    core_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    role TEXT DEFAULT 'producer',
    disabled INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rw_prod_types (
    core_id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    is_uploadable INTEGER DEFAULT 0,
    is_dubable INTEGER DEFAULT 0,
    is_live INTEGER DEFAULT 0,
    class INTEGER DEFAULT 0,
    category INTEGER DEFAULT 0,
    disabled INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rw_revenue_types (
    core_id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    disabled INTEGER DEFAULT 0,
    integration_id TEXT,
    is_billable INTEGER DEFAULT 0,
    commission_exception_new INTEGER DEFAULT 0,
    commission_exception_agency INTEGER DEFAULT 0,
    default_commission REAL DEFAULT 15,
    nat_rep_commission REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rw_dayparts (
    core_id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    start_time TEXT,
    end_time TEXT,
    disabled INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rw_avail_types (
    core_id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    disabled INTEGER DEFAULT 0,
    integration_id TEXT
);

CREATE TABLE IF NOT EXISTS rw_spot_types (
    core_id INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    disabled INTEGER DEFAULT 0,
    integration_id TEXT
);

CREATE TABLE IF NOT EXISTS rw_spot_lengths (
    length_seconds INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS rw_automation_systems (
    system_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rw_production (
    core_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES rw_accounts(core_id),
    station_id INTEGER REFERENCES rw_stations(core_id),
    prod_type_id INTEGER REFERENCES rw_prod_types(core_id),
    title TEXT NOT NULL,
    isci_code TEXT,
    cart_id TEXT,
    cut_number INTEGER DEFAULT 1,
    length_seconds INTEGER DEFAULT 30,
    start_date TEXT,
    end_date TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER DEFAULT 0,
    talent_id INTEGER REFERENCES rw_talent(core_id),
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rw_instructions (
    core_id SERIAL PRIMARY KEY,
    production_id INTEGER NOT NULL REFERENCES rw_production(core_id),
    line_number INTEGER NOT NULL DEFAULT 1,
    instruction_text TEXT NOT NULL,
    instruction_type TEXT DEFAULT 'copy',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rw_logs (
    core_id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES rw_stations(core_id),
    log_date TEXT NOT NULL,
    status INTEGER DEFAULT 0,
    total_spots INTEGER DEFAULT 0,
    total_revenue REAL DEFAULT 0,
    exported INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rw_days_logs (
    core_id SERIAL PRIMARY KEY,
    log_id INTEGER REFERENCES rw_logs(core_id),
    station_id INTEGER REFERENCES rw_stations(core_id),
    air_date TEXT NOT NULL,
    scheduled_time TEXT,
    actual_time TEXT,
    cart_id TEXT,
    isci_code TEXT,
    account_id INTEGER REFERENCES rw_accounts(core_id),
    length_seconds INTEGER DEFAULT 30,
    daypart_id INTEGER REFERENCES rw_dayparts(core_id),
    avail_type_id INTEGER REFERENCES rw_avail_types(core_id),
    spot_type_id INTEGER REFERENCES rw_spot_types(core_id),
    revenue REAL DEFAULT 0,
    aired INTEGER DEFAULT 1,
    reconciled INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rw_clocks (
    core_id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES rw_stations(core_id),
    name TEXT NOT NULL,
    description TEXT,
    day_of_week INTEGER,
    hour_start INTEGER,
    hour_end INTEGER,
    active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS rw_clock_breaks (
    core_id SERIAL PRIMARY KEY,
    clock_id INTEGER NOT NULL REFERENCES rw_clocks(core_id),
    position INTEGER NOT NULL DEFAULT 1,
    avail_type_id INTEGER REFERENCES rw_avail_types(core_id),
    max_units INTEGER DEFAULT 4,
    max_seconds INTEGER DEFAULT 120
);

CREATE TABLE IF NOT EXISTS rw_ad_bank (
    core_id SERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES rw_stations(core_id),
    account_id INTEGER REFERENCES rw_accounts(core_id),
    cart_id TEXT,
    isci_code TEXT,
    title TEXT NOT NULL,
    length_seconds INTEGER DEFAULT 30,
    start_date TEXT,
    end_date TEXT,
    created_date TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    is_network INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rw_network_dubs (
    core_id SERIAL PRIMARY KEY,
    cart_id TEXT,
    title TEXT NOT NULL,
    isci_code TEXT,
    length_seconds INTEGER DEFAULT 30,
    first_seen_date TEXT NOT NULL,
    agency TEXT,
    advertiser TEXT,
    status TEXT DEFAULT 'active'
);

-- ═══════════════════════════════════════════════════════════════════════════
-- StackAdapt (GraphQL DSP) — sa_* tables
-- Mirrors the official StackAdapt GraphQL schema: Advertiser → CampaignGroup →
-- Campaign (channel-typed), with per-day delivery stats backing the delivery
-- report queries. String UUID-style primary keys, ISO 8601 timestamps.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sa_advertisers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    is_archived BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS sa_campaign_groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    advertiser_id TEXT REFERENCES sa_advertisers(id),
    status TEXT DEFAULT 'ACTIVE',
    budget_type TEXT DEFAULT 'COST',
    budget_rollover BOOLEAN DEFAULT FALSE,
    is_archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sa_campaigns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'NATIVE',
    status TEXT DEFAULT 'DRAFT',
    is_draft BOOLEAN DEFAULT FALSE,
    is_archived BOOLEAN DEFAULT FALSE,
    goal_type TEXT,
    advertiser_id TEXT REFERENCES sa_advertisers(id),
    campaign_group_id TEXT REFERENCES sa_campaign_groups(id),
    timezone TEXT DEFAULT 'America/New_York',
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS sa_delivery_stats (
    id SERIAL PRIMARY KEY,
    campaign_id TEXT REFERENCES sa_campaigns(id),
    campaign_group_id TEXT REFERENCES sa_campaign_groups(id),
    advertiser_id TEXT REFERENCES sa_advertisers(id),
    stat_date TEXT NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    conversions DOUBLE PRECISION DEFAULT 0,
    cost DOUBLE PRECISION DEFAULT 0,
    revenue DOUBLE PRECISION DEFAULT 0,
    atos DOUBLE PRECISION DEFAULT 0
);

-- ===========================================================================
-- Freewheel (api.freewheel.tv) -- fw_* tables
-- Spec-faithful rebuild from the authoritative OpenAPI specs in
-- /tmp/fw-specs/*.json. Real sub-paths /services/v3, /services/v4, /reporting/v1
-- are preserved verbatim under the AdBridge /freewheel prefix. Integer PKs match
-- the real id types. Graph: agency -> advertiser -> brand -> campaign -> IO ->
-- placement, plus per-day delivery stats backing the ASYNC reporting job model,
-- marketplace V4 inventory, and RFP/proposed/programmatic V4 entities.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS fw_agencies (
    agency_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    external_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    parent_agency_id INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_advertisers (
    advertiser_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    external_id TEXT,
    agency_id INTEGER REFERENCES fw_agencies(agency_id),
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_brands (
    brand_id SERIAL PRIMARY KEY,
    advertiser_id INTEGER REFERENCES fw_advertisers(advertiser_id),
    name TEXT NOT NULL,
    external_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_relationships (
    id SERIAL PRIMARY KEY,
    parent_type TEXT NOT NULL,
    parent_id INTEGER NOT NULL,
    child_type TEXT NOT NULL,
    child_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS fw_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    advertiser_id INTEGER REFERENCES fw_advertisers(advertiser_id),
    agency_id INTEGER REFERENCES fw_agencies(agency_id),
    external_id TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS fw_insertion_orders (
    insertion_order_id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES fw_campaigns(campaign_id),
    name TEXT NOT NULL,
    description TEXT,
    client_po TEXT,
    brand_id INTEGER REFERENCES fw_brands(brand_id),
    external_id TEXT,
    primary_sales_person TEXT,
    primary_trafficker TEXT,
    currency TEXT NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL DEFAULT 'draft',
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS fw_placements (
    placement_id SERIAL PRIMARY KEY,
    insertion_order_id INTEGER REFERENCES fw_insertion_orders(insertion_order_id),
    name TEXT NOT NULL,
    description TEXT,
    external_id TEXT,
    start_date TEXT,
    end_date TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS fw_delivery_stats (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER,
    insertion_order_id INTEGER,
    placement_id INTEGER,
    advertiser_id INTEGER,
    sales_channel TEXT,
    stat_date TEXT NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    spend DOUBLE PRECISION DEFAULT 0,
    revenue DOUBLE PRECISION DEFAULT 0,
    completion_rate DOUBLE PRECISION DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fw_audience_items (
    audience_item_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    sales_channel TEXT
);

CREATE TABLE IF NOT EXISTS fw_audience_segments (
    audience_segment_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    segment_kv TEXT,
    sales_channel TEXT
);

CREATE TABLE IF NOT EXISTS fw_report_jobs (
    job_id TEXT PRIMARY KEY,
    report_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    sales_channel TEXT,
    start_date TEXT,
    end_date TEXT,
    filters TEXT,
    rows_json TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS fw_creative_metrics (
    creative_metric_id SERIAL PRIMARY KEY,
    creative_instance_id INTEGER NOT NULL,
    metric_type TEXT,
    event_name TEXT,
    url TEXT,
    created_at TEXT NOT NULL
);

-- Marketplace (V4)
CREATE TABLE IF NOT EXISTS fw_available_listings (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    seller TEXT,
    inventory_type TEXT,
    floor_cpm DOUBLE PRECISION,
    status TEXT NOT NULL DEFAULT 'available'
);

CREATE TABLE IF NOT EXISTS fw_listings (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    seller TEXT,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS fw_mkpl_creatives (
    mkpl_creative_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    creative_type TEXT NOT NULL DEFAULT 'mkpl_exchange_programmatic_creative',
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS fw_inventory_splits (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    split_percent DOUBLE PRECISION,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS fw_inventory_split_orders (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    inventory_split_id INTEGER,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS fw_purchased_inventory_orders (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    buyer TEXT,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS fw_sold_inventory_orders (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    seller TEXT,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS fw_supply_source_packages (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source TEXT,
    status TEXT NOT NULL DEFAULT 'active'
);

-- RFP + Proposed + Programmatic (V4)
CREATE TABLE IF NOT EXISTS fw_rfp_forecasts (
    id SERIAL PRIMARY KEY,
    granularity TEXT,
    data_json TEXT,
    status TEXT NOT NULL DEFAULT 'completed',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_proposed_insertion_orders (
    proposed_io_id SERIAL PRIMARY KEY,
    campaign_id INTEGER,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_proposed_placements (
    proposed_placement_id SERIAL PRIMARY KEY,
    proposed_io_id INTEGER REFERENCES fw_proposed_insertion_orders(proposed_io_id),
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_proposed_ads (
    proposed_ad_id SERIAL PRIMARY KEY,
    proposed_placement_id INTEGER REFERENCES fw_proposed_placements(proposed_placement_id),
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_programmatic_deals (
    deal_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    deal_type TEXT,
    description TEXT,
    salesperson TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_programmatic_buyers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    seat_id TEXT
);

CREATE TABLE IF NOT EXISTS fw_global_advertisers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fw_global_brands (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    global_advertiser_id INTEGER
);
"""


def _exec(cur, sql, params=()):
    cur.execute(sql, params)


def _seed_core(cur):
    """Seed shared tables: users, agencies, clients, brands, campaigns, etc."""
    now = _now()

    # --- Users ---
    user_id = _uuid()
    cur.execute(
        "INSERT INTO users VALUES (%s,%s,%s,%s,%s)",
        (user_id, "Alex", "Morgan", "alex.morgan@example.com", now),
    )

    # --- Agency ---
    agency_id = _uuid()
    cur.execute(
        "INSERT INTO agencies VALUES (%s,%s,%s,%s)",
        (agency_id, "Horizon Media Group", "DSP-" + _uuid()[:8], now),
    )

    # --- Verticals ---
    vertical_names = [
        "Automotive", "Technology", "Healthcare", "Finance",
        "Retail", "Travel", "Entertainment", "Food & Beverage",
    ]
    vertical_ids = []
    for v in vertical_names:
        vid = _uuid()
        vertical_ids.append(vid)
        cur.execute("INSERT INTO verticals VALUES (%s,%s,%s)", (vid, v, now))

    # --- KPIs ---
    kpi_data = [
        ("CTR", "percentage"), ("CPA", "currency"), ("ROAS", "ratio"),
        ("Viewability Rate", "percentage"), ("Video Completion Rate", "percentage"),
        ("CPM", "currency"), ("Conversions", "count"),
    ]
    kpi_ids = []
    for name, goal_type in kpi_data:
        kid = _uuid()
        kpi_ids.append(kid)
        cur.execute("INSERT INTO kpis VALUES (%s,%s,%s,%s)", (kid, name, goal_type, now))

    # --- Clients ---
    client_data = [
        ("Acme Corporation", "Acme Corp Billing", "Jane", "Smith", "jane@acme.com", "555-0101", "101"),
        ("TechNova Inc", "TechNova Billing", "Bob", "Chen", "bob@technova.com", "555-0102", "202"),
        ("GreenLeaf Brands", "GreenLeaf LLC", "Sara", "Johnson", "sara@greenleaf.com", "555-0103", None),
    ]
    client_ids = []
    for name, billing, cfirst, clast, email, phone, ext in client_data:
        cid = _uuid()
        client_ids.append(cid)
        cur.execute(
            "INSERT INTO clients VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (cid, name, billing, cfirst, clast, email, phone, ext,
             "Synthetic test client", now),
        )

    # --- Brands ---
    brand_data = [
        ("Acme Widgets", 0), ("Acme Premium", 0),
        ("TechNova Cloud", 1), ("TechNova Mobile", 1),
        ("GreenLeaf Organic", 2), ("GreenLeaf Essentials", 2),
    ]
    brand_ids = []
    for bname, client_idx in brand_data:
        bid = _uuid()
        brand_ids.append(bid)
        cur.execute(
            "INSERT INTO brands VALUES (%s,%s,%s,%s)",
            (bid, bname, client_ids[client_idx], now),
        )
        for vid in random.sample(vertical_ids, k=min(2, len(vertical_ids))):
            cur.execute(
                "INSERT INTO brand_verticals VALUES (%s,%s,%s)",
                (bid, vid, f"Sub-{random.choice(['Premium','Standard','Digital'])}"),
            )

    # --- Campaigns ---
    statuses = ["live", "approved", "completed"]
    campaign_ids = []
    for i in range(8):
        cid = _uuid()
        campaign_ids.append(cid)
        client_idx = i % len(client_ids)
        brand_idx = i % len(brand_ids)
        cur.execute(
            "INSERT INTO campaigns VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                cid, f"Campaign {i+1} - {'Spring' if i < 4 else 'Summer'} 2025",
                f"UGCID-{1000+i}", f"Initiative {'Alpha' if i < 4 else 'Beta'}",
                statuses[i % 3],
                round(random.uniform(10000, 500000), 2),
                _past_date(90 - i * 10), _future_date(30 + i * 10),
                client_ids[client_idx], brand_ids[brand_idx], now,
            ),
        )
        for kid in random.sample(kpi_ids, k=2):
            cur.execute(
                "INSERT INTO campaign_kpis VALUES (%s,%s,%s)",
                (cid, kid, round(random.uniform(0.5, 10.0), 2)),
            )

    # --- Line Items ---
    formats_list = ["display", "video", "native", "audio"]
    platforms_list = ["desktop", "mobile", "ctv", "all"]
    line_item_ids = []
    # Collect vendor and property IDs for FK references
    cur.execute("SELECT id FROM vendors")
    vendor_ids = [r["id"] for r in cur.fetchall()]
    cur.execute("SELECT id FROM properties")
    property_ids = [r["id"] for r in cur.fetchall()]
    for camp_id in campaign_ids:
        for j in range(random.randint(2, 4)):
            lid = _uuid()
            lineage = _uuid()
            media_plan_id = _uuid()
            line_item_ids.append((lid, camp_id))
            total_contracted = round(random.uniform(5000, 100000), 2)
            media_contracted = round(total_contracted * random.uniform(0.7, 0.9), 2)
            ad_serving_contracted = round(total_contracted - media_contracted, 2)
            margin = round(random.uniform(0.05, 0.25), 4)
            fmt = random.choice(formats_list)
            plat = random.choice(platforms_list)
            v_id = random.choice(vendor_ids) if vendor_ids else None
            p_id = random.choice(property_ids) if property_ids else None
            k_id = random.choice(kpi_ids) if kpi_ids else None
            cur.execute(
                "INSERT INTO line_items VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    lid, lineage, camp_id, p_id, v_id,
                    f"Line Item {j+1}",
                    random.choice(["direct", "dsp"]),
                    media_plan_id,
                    _past_date(60), _future_date(30),
                    "300x250,728x90", fmt, plat,
                    random.choice(["cpm", "cpc", "cpv"]),
                    total_contracted,
                    round(total_contracted * margin, 2),
                    margin,
                    round(random.uniform(1, 25), 2),
                    random.randint(10000, 500000),
                    media_contracted,
                    random.choice(["cpm", "flat"]),
                    random.choice([None, "Google CM360", "Sizmek"]),
                    round(random.uniform(100, 2000), 2),
                    round(random.uniform(0.01, 0.10), 4),
                    random.randint(1000, 50000),
                    random.randint(10000, 500000),
                    ad_serving_contracted,
                    k_id,
                    round(random.uniform(0.5, 10.0), 2),
                    random.choice(["programmatic", "direct", "social"]),
                    now,
                ),
            )

    # --- Addons ---
    for camp_id in campaign_ids[:4]:
        aid = _uuid()
        contracted = round(random.uniform(1000, 10000), 2)
        cur.execute(
            "INSERT INTO addons VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                aid, _uuid(), camp_id,
                random.choice(["Ad Serving Fee", "Data Fee", "Verification Fee"]),
                _past_date(60), _future_date(30),
                contracted, round(contracted * random.uniform(0.3, 0.9), 2), now,
            ),
        )

    # --- Vendors ---
    vendor_names = [
        ("Google DV360", "dsp"), ("The Trade Desk", "dsp"),
        ("Amazon DSP", "dsp"), ("Xandr", "exchange"),
        ("PubMatic", "exchange"), ("Magnite", "ssp"),
    ]
    for vname, vtype in vendor_names:
        cur.execute("INSERT INTO vendors VALUES (%s,%s,%s,%s)", (_uuid(), vname, vtype, now))

    # --- Properties ---
    property_data = [
        ("CNN.com", "website", "https://cnn.com"),
        ("ESPN.com", "website", "https://espn.com"),
        ("Hulu", "streaming", "https://hulu.com"),
        ("Spotify", "audio", "https://spotify.com"),
        ("NYTimes.com", "website", "https://nytimes.com"),
    ]
    for pname, ptype, purl in property_data:
        pid = _uuid()
        cur.execute("INSERT INTO properties VALUES (%s,%s,%s,%s,%s)", (pid, pname, ptype, purl, now))
        for vid in random.sample(vertical_ids, k=2):
            cur.execute("INSERT INTO property_verticals VALUES (%s,%s)", (pid, vid))

    # --- Creatives ---
    sources = ["gcm", "dsp", "upload", "facebook"]
    media_types = ["image", "video", "html5", "native"]
    for i in range(12):
        w = random.choice([300, 728, 160, 320, 1920])
        h = random.choice([250, 90, 600, 50, 1080])
        cur.execute(
            "INSERT INTO creatives VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                _uuid(), f"Creative {i+1} - {'Banner' if i % 2 == 0 else 'Video'}",
                random.choice(sources), f"EXT-REF-{10000+i}",
                random.choice(media_types),
                random.choice(["standard", "premium", "interactive"]),
                f"{w}x{h}",
                now,
            ),
        )

    # --- Conversions ---
    for i in range(6):
        cur.execute(
            "INSERT INTO conversions VALUES (%s,%s,%s,%s,%s)",
            (
                _uuid(), f"Conversion Event {i+1}",
                random.choice(["floodlight", "pixel", "postback"]),
                f"CONV-{20000+i}", now,
            ),
        )

    # --- Delivery Sources ---
    for i in range(5):
        cur.execute(
            "INSERT INTO delivery_sources VALUES (%s,%s,%s,%s,%s,%s)",
            (
                _uuid(), f"Delivery Source {i+1}",
                random.choice(["dcm", "dsp", "ad_server"]),
                f"DS-{30000+i}", random.choice(["300x250", "728x90", "video"]), now,
            ),
        )

    # --- Groups ---
    group_ids = []
    for i in range(4):
        cur.execute(
            "INSERT INTO groups_ (name,budget_amount,budget_type,flight_start,flight_end,"
            "even_delivery,status,pacing_control_level,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (
                f"Group {i+1} - {'Awareness' if i < 2 else 'Performance'}",
                round(random.uniform(10000, 100000), 2),
                random.choice(["daily", "lifetime"]),
                _past_date(60), _future_date(30),
                bool(random.randint(0, 1)),
                random.choice(["active", "paused"]),
                random.choice(["group", "tactic"]),
                now,
            ),
        )
        group_ids.append(cur.fetchone()["id"])

    # --- Tactics ---
    for i in range(8):
        cur.execute(
            "INSERT INTO tactics (name,group_id,budget_amount,budget_type,flight_start,flight_end,"
            "pacing_priority,status,default_bid,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                f"Tactic {i+1}",
                group_ids[i % len(group_ids)],
                round(random.uniform(2000, 30000), 2),
                random.choice(["daily", "lifetime"]),
                _past_date(45), _future_date(20),
                random.choice(["high", "medium", "low"]),
                random.choice(["active", "paused"]),
                round(random.uniform(0.5, 15.0), 2),
                now,
            ),
        )

    # --- Collect delivery_source and creative IDs for stats FK references ---
    cur.execute("SELECT id FROM delivery_sources")
    ds_ids = [r["id"] for r in cur.fetchall()]
    cur.execute("SELECT id FROM creatives")
    cr_ids = [r["id"] for r in cur.fetchall()]
    cur.execute("SELECT conversion_id FROM conversions")
    conv_ids = [r["conversion_id"] for r in cur.fetchall()]

    # --- Stats ---
    for lid, camp_id in line_item_ids:
        lineage_id = _uuid()
        for day_offset in range(30):
            date = _past_date(30 - day_offset)
            impressions = random.randint(1000, 50000)
            impressions_raw = round(impressions + random.uniform(0, 0.99), 2)
            clicks = random.randint(10, int(impressions * 0.05))
            eligible_imps = random.randint(int(impressions * 0.7), impressions)
            measurable_imps = random.randint(int(eligible_imps * 0.6), eligible_imps)
            viewable_imps = random.randint(int(measurable_imps * 0.3), measurable_imps)
            interactions = random.randint(0, int(impressions * 0.01))
            video_starts = random.randint(0, int(impressions * 0.4))
            video_completions = random.randint(0, video_starts) if video_starts else 0
            view_conv = random.randint(0, 10)
            click_conv = random.randint(0, 20)
            total_conv = click_conv + view_conv
            cpa_view = round(random.uniform(5, 50), 2) if view_conv else 0
            cpa_click = round(random.uniform(5, 50), 2) if click_conv else 0
            cpa_total = round(random.uniform(5, 50), 2) if total_conv else 0
            inventory_spend = round(random.uniform(30, 1500), 2)
            data_spend = round(random.uniform(5, 200), 2)
            media_sp = round(inventory_spend + data_spend, 2)
            ad_serving_sp = round(random.uniform(5, 100), 2)
            total_sp = round(media_sp + ad_serving_sp, 2)
            delivered_units = impressions
            auctions_won = random.randint(int(impressions * 0.8), impressions)
            click_conv_rev = round(random.uniform(0, 300), 2) if click_conv else 0
            view_conv_rev = round(random.uniform(0, 200), 2) if view_conv else 0
            total_conv_rev = round(click_conv_rev + view_conv_rev, 2)
            ds_id = random.choice(ds_ids) if ds_ids else None
            cr_id = random.choice(cr_ids) if cr_ids else None
            cn_id = random.choice(conv_ids) if conv_ids else None
            ext_ad_ref = f"EXT-AD-{random.randint(10000,99999)}"
            cur.execute(
                "INSERT INTO stats (line_item_id,line_item_lineage_id,campaign_id,"
                "delivery_source_id,creative_id,conversion_id,external_ad_ref,date,"
                "delivered_impressions,delivered_impressions_raw,delivered_clicks,"
                "delivered_eligible_impressions,delivered_measurable_impressions,"
                "delivered_viewable_impressions,delivered_interactions,"
                "delivered_video_starts,delivered_video_completions,"
                "delivered_total_view_conversions,delivered_total_click_conversions,"
                "delivered_total_conversions,delivered_cpa_view_conversions,"
                "delivered_cpa_click_conversions,delivered_cpa_total_conversions,"
                "delivered_inventory_spend,delivered_data_spend,delivered_units,"
                "total_spend,media_spend,ad_serving_spend,auctions_won,"
                "click_conversions,click_conversion_revenue,"
                "view_conversions,view_conversion_revenue,"
                "total_conversions,total_conversion_revenue) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    lid, lineage_id, camp_id, ds_id, cr_id, cn_id, ext_ad_ref, date,
                    impressions, impressions_raw, clicks,
                    eligible_imps, measurable_imps, viewable_imps, interactions,
                    video_starts, video_completions,
                    view_conv, click_conv, total_conv,
                    cpa_view, cpa_click, cpa_total,
                    inventory_spend, data_spend, delivered_units,
                    total_sp, media_sp, ad_serving_sp, auctions_won,
                    click_conv, click_conv_rev,
                    view_conv, view_conv_rev,
                    total_conv, total_conv_rev,
                ),
            )


def _seed_dv360(cur, now):
    cur.execute("SELECT COUNT(*) FROM dv360_partners")
    if cur.fetchone()["count"] > 0:
        return
    partner_ids = [100001, 100002]
    for i, pid in enumerate(partner_ids):
        cur.execute(
            "INSERT INTO dv360_partners VALUES (%s,%s,%s,%s)",
            (pid, f"Partner {'Alpha' if i == 0 else 'Beta'} Media", "ENTITY_STATUS_ACTIVE", now),
        )

    advertiser_data = [
        (200001, 100001, "Acme Digital Ads", "acme.com"),
        (200002, 100001, "TechNova Performance", "technova.com"),
        (200003, 100002, "GreenLeaf Display", "greenleaf.com"),
        (200004, 100002, "Sunset Travel Promos", "sunsettravel.com"),
    ]
    advertiser_ids = []
    for adv_id, p_id, name, domain in advertiser_data:
        advertiser_ids.append(adv_id)
        cur.execute(
            "INSERT INTO dv360_advertisers VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (adv_id, p_id, name, "ENTITY_STATUS_ACTIVE", "USD", domain, now),
        )

    goal_types = [
        "CAMPAIGN_GOAL_TYPE_BRAND_AWARENESS",
        "CAMPAIGN_GOAL_TYPE_DRIVE_ONLINE_ACTION",
        "CAMPAIGN_GOAL_TYPE_DRIVE_OFFLINE_ACTION",
    ]
    perf_goal_types = [
        "PERFORMANCE_GOAL_TYPE_CPM", "PERFORMANCE_GOAL_TYPE_CPC",
        "PERFORMANCE_GOAL_TYPE_CPA", "PERFORMANCE_GOAL_TYPE_VIEWABLE_CPM",
    ]
    campaign_ids_dv = []
    cam_counter = 300001
    for adv_id in advertiser_ids:
        for j in range(3):
            cid = cam_counter
            cam_counter += 1
            campaign_ids_dv.append((cid, adv_id))
            cur.execute(
                "INSERT INTO dv360_campaigns VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    cid, adv_id,
                    f"DV360 Campaign {j+1} - {'Brand' if j == 0 else 'Performance'}",
                    "ENTITY_STATUS_ACTIVE",
                    random.choice(goal_types), random.choice(perf_goal_types),
                    random.randint(500000, 10000000),
                    _past_date(60 - j * 15), _future_date(30 + j * 15),
                    random.choice([3, 5, 10]),
                    random.choice(["TIME_UNIT_DAYS", "TIME_UNIT_WEEKS"]),
                    now,
                ),
            )

    io_counter = 400001
    io_ids = []
    for cid, adv_id in campaign_ids_dv:
        for k in range(2):
            io_id = io_counter
            io_counter += 1
            io_ids.append((io_id, adv_id, cid))
            cur.execute(
                "INSERT INTO dv360_insertion_orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    io_id, adv_id, cid,
                    f"IO {k+1} - {'Always On' if k == 0 else 'Flight'}",
                    "ENTITY_STATUS_ACTIVE",
                    random.choice(["PACING_TYPE_EVEN", "PACING_TYPE_AHEAD"]),
                    "BUDGET_TYPE_FIXED",
                    random.randint(1000000, 50000000),
                    "BUDGET_UNIT_CURRENCY",
                    _past_date(45 - k * 10), _future_date(30 + k * 10),
                    now,
                ),
            )

    li_types = [
        "LINE_ITEM_TYPE_DISPLAY_DEFAULT", "LINE_ITEM_TYPE_VIDEO_DEFAULT",
        "LINE_ITEM_TYPE_DISPLAY_MOBILE_APP_INSTALL",
        "LINE_ITEM_TYPE_YOUTUBE_AND_PARTNERS_ACTION",
    ]
    bid_strategies = [
        "BIDDING_STRATEGY_FIXED_CPM",
        "BIDDING_STRATEGY_MAXIMIZE_SPEND_AUTO_BID",
        "BIDDING_STRATEGY_PERFORMANCE_GOAL_AUTO_BID",
    ]
    li_counter = 500001
    li_ids = []
    for io_id, adv_id, cid in io_ids:
        for m in range(random.randint(2, 4)):
            li_id = li_counter
            li_counter += 1
            li_ids.append((li_id, adv_id, cid, io_id))
            cur.execute(
                "INSERT INTO dv360_line_items VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    li_id, adv_id, cid, io_id,
                    f"Line Item {m+1} - {random.choice(['Prospecting','Retargeting','Contextual'])}",
                    random.choice(li_types),
                    random.choice(["ENTITY_STATUS_ACTIVE", "ENTITY_STATUS_PAUSED"]),
                    _past_date(30), _future_date(20),
                    "LINE_ITEM_BUDGET_ALLOCATION_TYPE_FIXED",
                    random.randint(500000, 10000000),
                    random.choice(["PACING_TYPE_EVEN", "PACING_TYPE_AHEAD"]),
                    random.choice([3, 5, 10, None]),
                    random.choice(["TIME_UNIT_DAYS", "TIME_UNIT_WEEKS", None]),
                    random.choice(bid_strategies),
                    random.randint(100000, 5000000),
                    now,
                ),
            )

    ag_counter = 600001
    ag_ids = []
    for li_id, adv_id, cid, io_id in li_ids[:12]:
        ag_id = ag_counter
        ag_counter += 1
        ag_ids.append((ag_id, adv_id, li_id))
        cur.execute(
            "INSERT INTO dv360_ad_groups VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                ag_id, adv_id, li_id,
                f"Ad Group - {random.choice(['Broad','Narrow','Custom'])} Targeting",
                "ENTITY_STATUS_ACTIVE",
                random.choice(["AD_GROUP_FORMAT_IN_STREAM", "AD_GROUP_FORMAT_VIDEO_DISCOVERY", "AD_GROUP_FORMAT_BUMPER"]),
                random.choice(["BIDDING_STRATEGY_FIXED_CPM", "BIDDING_STRATEGY_MAXIMIZE_SPEND_AUTO_BID"]),
                random.randint(100000, 3000000),
                now,
            ),
        )

    aga_counter = 700001
    for ag_id, adv_id, li_id in ag_ids:
        for n in range(random.randint(1, 3)):
            cur.execute(
                "INSERT INTO dv360_ad_group_ads VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    aga_counter, adv_id, ag_id,
                    f"Ad {n+1} - {random.choice(['Awareness','Action','Promo'])}",
                    "ENTITY_STATUS_ACTIVE",
                    f"https://example.com/landing/{aga_counter}",
                    now,
                ),
            )
            aga_counter += 1

    creative_types = [
        "CREATIVE_TYPE_STANDARD", "CREATIVE_TYPE_VIDEO",
        "CREATIVE_TYPE_NATIVE", "CREATIVE_TYPE_EXPANDABLE",
    ]
    hosting_sources = ["HOSTING_SOURCE_CM", "HOSTING_SOURCE_THIRD_PARTY", "HOSTING_SOURCE_HOSTED"]
    approval_statuses = [
        "APPROVAL_STATUS_PENDING_REVIEW", "APPROVAL_STATUS_APPROVED", "APPROVAL_STATUS_REJECTED",
    ]
    cr_counter = 800001
    for adv_id in advertiser_ids:
        for p in range(5):
            cur.execute(
                "INSERT INTO dv360_creatives VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    cr_counter, adv_id,
                    f"Creative {p+1} - {random.choice(['Hero Banner','Product Showcase','Video Pre-roll','Native Card'])}",
                    "ENTITY_STATUS_ACTIVE",
                    random.choice(creative_types), random.choice(hosting_sources),
                    random.choice([300, 728, 160, 320, 1920, 640]),
                    random.choice([250, 90, 600, 50, 1080, 480]),
                    random.choice(approval_statuses),
                    "REVIEW_STATUS_UNSPECIFIED",
                    f"https://example.com/click/{cr_counter}",
                    now,
                ),
            )
            cr_counter += 1

    targeting_data = [
        ("TARGETING_TYPE_GEO_REGION", [
            ("geo_us", "United States"), ("geo_uk", "United Kingdom"),
            ("geo_ca", "Canada"), ("geo_de", "Germany"), ("geo_fr", "France"),
        ]),
        ("TARGETING_TYPE_AGE_RANGE", [
            ("age_18_24", "18-24"), ("age_25_34", "25-34"),
            ("age_35_44", "35-44"), ("age_45_54", "45-54"), ("age_55_64", "55-64"),
        ]),
        ("TARGETING_TYPE_GENDER", [
            ("gender_male", "Male"), ("gender_female", "Female"), ("gender_unknown", "Unknown"),
        ]),
        ("TARGETING_TYPE_DEVICE_TYPE", [
            ("device_computer", "Computer"), ("device_mobile", "Mobile"),
            ("device_tablet", "Tablet"), ("device_ctv", "Connected TV"),
        ]),
        ("TARGETING_TYPE_BROWSER", [
            ("browser_chrome", "Chrome"), ("browser_safari", "Safari"),
            ("browser_firefox", "Firefox"), ("browser_edge", "Edge"),
        ]),
        ("TARGETING_TYPE_CONTENT_GENRE", [
            ("genre_news", "News"), ("genre_sports", "Sports"),
            ("genre_entertainment", "Entertainment"), ("genre_tech", "Technology"),
        ]),
    ]
    for ttype, options in targeting_data:
        for opt_id, name in options:
            cur.execute(
                "INSERT INTO dv360_targeting_options VALUES (%s,%s,%s)",
                (opt_id, ttype, name),
            )

    at_counter = 1
    for li_id, adv_id, cid, io_id in li_ids[:8]:
        for ttype, opt_id, name in [
            ("TARGETING_TYPE_GEO_REGION", "geo_us", "United States"),
            ("TARGETING_TYPE_AGE_RANGE", "age_25_34", "25-34"),
            ("TARGETING_TYPE_DEVICE_TYPE", "device_mobile", "Mobile"),
        ]:
            cur.execute(
                "INSERT INTO dv360_assigned_targeting VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (at_counter, f"at-{at_counter}", "line_item", li_id, adv_id, ttype, opt_id, name),
            )
            at_counter += 1

    ch_counter = 900001
    for adv_id in advertiser_ids[:2]:
        for ch_name in ["Premium Publishers", "Brand Safe List", "Performance Sites"]:
            cur.execute(
                "INSERT INTO dv360_channels VALUES (%s,%s,%s,%s,%s,%s)",
                (ch_counter, adv_id, None, ch_name, random.randint(2, 10), random.randint(0, 3)),
            )
            ch_counter += 1

    inv_counter = 1000001
    exchanges = ["EXCHANGE_GOOGLE_AD_MANAGER", "EXCHANGE_APPNEXUS", "EXCHANGE_OPENX", "EXCHANGE_PUBMATIC"]
    for i in range(6):
        cur.execute(
            "INSERT INTO dv360_inventory_sources VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                inv_counter + i,
                f"Deal - {random.choice(['Premium Video','Display Select','CTV Bundle','Audio Package'])} {i+1}",
                random.choice(["INVENTORY_SOURCE_TYPE_PRIVATE", "INVENTORY_SOURCE_TYPE_AUCTION_PACKAGE"]),
                random.choice(exchanges),
                "INVENTORY_SOURCE_STATUS_ACTIVE",
                f"DEAL-{50000+i}",
                random.choice(["RATE_TYPE_CPM_FIXED", "RATE_TYPE_CPM_FLOOR"]),
                random.randint(500000, 8000000),
                now,
            ),
        )

    for i, adv_id in enumerate(advertiser_ids[:2]):
        fg_id = 1100001 + i
        cur.execute(
            "INSERT INTO dv360_floodlight_groups VALUES (%s,%s,%s,%s,%s)",
            (fg_id, f"Floodlight Group - {'Conversions' if i == 0 else 'Remarketing'}", 30, 14, "WEB_TAG_TYPE_FLOODLIGHT"),
        )
        for j in range(3):
            cur.execute(
                "INSERT INTO dv360_floodlight_activities VALUES (%s,%s,%s,%s,%s)",
                (
                    1200001 + i * 3 + j, fg_id,
                    f"Activity - {['Purchase','Add to Cart','Page View'][j]}",
                    "FLOODLIGHT_ACTIVITY_SERVING_STATUS_ENABLED",
                    str(adv_id),
                ),
            )

    # ── Delivery stats: ~30 days of daily metrics per entity ─────────────────
    # Covers every seeded campaign, insertion order, and line item so the
    # Bid Manager-style reporting endpoints have real data to aggregate.
    stat_entities = (
        [("campaign", cid, adv_id) for cid, adv_id in campaign_ids_dv]
        + [("insertion_order", io_id, adv_id) for io_id, adv_id, _cid in io_ids]
        + [("line_item", li_id, adv_id) for li_id, adv_id, _cid, _io in li_ids]
    )
    for entity_level, entity_id, adv_id in stat_entities:
        # Deterministic-ish daily volume scaled by entity level so parents
        # read larger than their children.
        base = {"campaign": 40000, "insertion_order": 18000, "line_item": 7000}[entity_level]
        for day_offset in range(30):
            date = _past_date(30 - day_offset)
            impressions = random.randint(int(base * 0.5), int(base * 1.5))
            clicks = random.randint(int(impressions * 0.001), max(1, int(impressions * 0.02)))
            # revenue/spend in micros: ~ $2–$12 CPM
            revenue_micros = int(impressions / 1000 * random.uniform(2.0, 12.0) * 1_000_000)
            conversions = random.randint(0, max(1, int(clicks * 0.1)))
            video_completions = random.randint(0, int(impressions * 0.3))
            cur.execute(
                "INSERT INTO dv360_delivery_stats "
                "(advertiser_id,entity_level,entity_id,date,impressions,clicks,"
                "revenue_micros,conversions,video_completions) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (adv_id, entity_level, entity_id, date, impressions, clicks,
                 revenue_micros, conversions, video_completions),
            )

    # ── Example saved queries (reporting definitions) ────────────────────────
    example_queries = [
        (
            90000001,
            "Last 30 Days - Line Item Performance",
            ["impressions", "clicks", "revenueMicros", "conversions", "videoCompletions"],
            ["line_item"],
            _past_date(30), _past_date(0),
        ),
        (
            90000002,
            "Last 7 Days - Campaign Delivery",
            ["impressions", "clicks", "revenueMicros"],
            ["campaign"],
            _past_date(7), _past_date(0),
        ),
    ]
    for q_id, title, metrics, group_bys, dr_start, dr_end in example_queries:
        cur.execute(
            "INSERT INTO dv360_queries "
            "(query_id,title,metrics,group_bys,date_range_start,date_range_end,created_time) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (q_id, title, json.dumps(metrics), json.dumps(group_bys),
             dr_start, dr_end, now),
        )


def _seed_triton_booking(cur, now):
    cur.execute("SELECT COUNT(*) FROM tap_advertisers")
    if cur.fetchone()["count"] > 0:
        return
    tap_advertisers = [
        ("iHeartMedia", "iheart.com", "Music & Audio", "USD", "IHM-001", "active",
         "Horizon Agency", "HA-100", None, None, "direct"),
        ("Procter & Gamble", "pg.com", "Consumer Goods", "USD", "PG-002", "active",
         "Omnicom Media", "OM-200", 120, None, "direct"),
        ("State Farm Insurance", "statefarm.com", "Insurance", "USD", "SF-003", "active",
         None, None, 90, None, "direct"),
        ("Toyota USA", "toyota.com", "Automotive", "USD", "TY-004", "active",
         "Saatchi & Saatchi", "SS-300", None, None, "direct"),
        ("Amazon Audible", "audible.com", "Entertainment", "USD", "AA-005", "active",
         None, None, 60, None, "direct"),
    ]
    for name, domain, iab, currency, ext_id, status, ag_name, ag_ext, sep, reseller, typ in tap_advertisers:
        cur.execute(
            "INSERT INTO tap_advertisers (name,domain,iab_category_code,currency_iso_code,"
            "external_id,status,agency_name,agency_external_id,ad_separation_override,"
            "reseller_id,type,created_by,creation_time,last_updated_time) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (name, domain, iab, currency, ext_id, status, ag_name, ag_ext, sep,
             reseller, typ, "admin@triton.com", now, now),
        )

    for adv_id in range(1, 4):
        cur.execute(
            "INSERT INTO tap_billing (advertiser_id,billing_contact,billing_email,"
            "billing_address,payment_terms,creation_time,last_updated_time) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (adv_id, f"Billing Contact {adv_id}", f"billing{adv_id}@example.com",
             f"{adv_id}00 Commerce St, New York, NY", "net30", now, now),
        )

    campaign_data = [
        ("Spring Audio Blitz 2025", 1, "approved", "EXT-C001", "Spring campaign",
         1, _past_date(60), _future_date(30), "Sarah Johnson", "Mike Torres"),
        ("Q2 Podcast Push", 2, "approved", "EXT-C002", "Podcast-focused campaign",
         1, _past_date(45), _future_date(45), "Sarah Johnson", "Mike Torres"),
        ("Summer Drive Time", 3, "draft", "EXT-C003", "Drive time audio spots",
         2, _past_date(10), _future_date(80), "Alex Chen", "Lisa Park"),
        ("Fall Brand Awareness", 4, "approved", "EXT-C004", "Brand awareness push",
         2, _past_date(30), _future_date(60), "Alex Chen", "Lisa Park"),
        ("Holiday Special 2025", 5, "draft", "EXT-C005", "Holiday promotional campaign",
         1, _future_date(10), _future_date(90), "Sarah Johnson", "Mike Torres"),
        ("Year-Round Sponsorship", 1, "approved", "EXT-C006", "Always-on sponsorship deal",
         1, _past_date(180), _future_date(180), "Sarah Johnson", "Mike Torres"),
    ]
    for name, adv_id, status, ext_id, notes, team_id, start, end, acct, traff in campaign_data:
        cur.execute(
            "INSERT INTO tap_campaigns (name,advertiser_id,status,external_id,notes,"
            "team_id,start_date,end_date,account_exec,trafficked_by,created_by,"
            "creation_time,last_updated_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (name, adv_id, status, ext_id, notes, team_id, start, end, acct, traff,
             "admin@triton.com", now, now),
        )

    positions = ["preroll", "midroll"]
    delivery_methods = ["podcast", "live"]
    pacings = ["even", "asFastAsPossible"]
    statuses = ["draft", "active", "active", "active", "done"]
    delivery_statuses = ["pending", "ready", "ready", "ready", "done"]

    flight_counter = 0
    for camp_id in range(1, 7):
        cur.execute("SELECT advertiser_id FROM tap_campaigns WHERE id = %s", (camp_id,))
        adv_id = cur.fetchone()["advertiser_id"]
        for j in range(random.randint(2, 4)):
            flight_counter += 1
            s = statuses[flight_counter % len(statuses)]
            ds = delivery_statuses[flight_counter % len(delivery_statuses)]
            cur.execute(
                "INSERT INTO tap_flights (name,campaign_id,advertiser_id,status,type,"
                "start_date,end_date,pricing_model,priority,position,delivery_method,"
                "pacing,run_on_network,open_measurement_enabled,goal_impressions,"
                "goal_spots,external_id,account_exec,trafficked_by,delivery_status,"
                "created_by,creation_time,last_updated_time) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    f"Flight {flight_counter} - {random.choice(['Morning','Afternoon','Evening','All Day'])}",
                    camp_id, adv_id, s, "direct",
                    _past_date(30 + j * 5), _future_date(20 + j * 10),
                    "cpm", random.randint(3, 16),
                    random.choice(positions), random.choice(delivery_methods),
                    random.choice(pacings),
                    bool(random.randint(0, 1)), bool(random.randint(0, 1)),
                    random.randint(50000, 2000000),
                    random.randint(100, 5000) if random.random() > 0.5 else None,
                    f"EXT-F{flight_counter:03d}",
                    "Sarah Johnson" if camp_id <= 3 else "Alex Chen",
                    "Mike Torres" if camp_id <= 3 else "Lisa Park",
                    ds, "admin@triton.com", now, now,
                ),
            )

    cur.execute("SELECT id FROM tap_flights WHERE status IN ('active','done')")
    active_flights = cur.fetchall()
    for row in active_flights:
        fid = row["id"]
        cur.execute(
            "INSERT INTO tap_creatives (flight_id,creative_url,ad_duration_in_seconds,"
            "status,overtone_id,deal_id,reseller_contract_id,creation_time,last_updated_time) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (fid, f"https://cdn.example.com/audio/spot_{fid}.mp3",
             random.choice([15, 30, 60]), "active", None, None, None, now, now),
        )

    for row in active_flights[:6]:
        fid = row["id"]
        cur.execute(
            "INSERT INTO tap_cappings (flight_id,type,limit_value,period,creation_time) VALUES (%s,%s,%s,%s,%s)",
            (fid, "impression", random.choice([3, 5, 10]), random.choice(["hour", "day", "week"]), now),
        )

    users = ["admin@triton.com", "sarah.johnson@example.com", "mike.torres@example.com"]
    audit_types = ["advertiser", "campaign", "flight", "creative"]
    for i in range(20):
        cur.execute(
            "INSERT INTO tap_audits (object_id,object_type,user_email,impersonated_by,creation_time) "
            "VALUES (%s,%s,%s,%s,%s)",
            (random.randint(1, 10), random.choice(audit_types), random.choice(users), None, now),
        )


def _seed_triton(cur, now):
    cur.execute("SELECT COUNT(*) FROM triton_reports")
    if cur.fetchone()["count"] > 0:
        return
    stations = [
        ("WABC-FM", "New York", "US"),
        ("KLOS-FM", "Los Angeles", "US"),
        ("WXRT-FM", "Chicago", "US"),
        ("CFRB-AM", "Toronto", "CA"),
        ("BBC Radio 1", "London", "GB"),
        ("KROQ-FM", "Los Angeles", "US"),
    ]

    report_data = [
        (_uuid(), "Streaming Sessions - Q1 2025", "Quarterly streaming session metrics"),
        (_uuid(), "Podcast Downloads - March 2025", "Monthly podcast download report"),
        (_uuid(), "Audience Overview - Weekly", "Weekly audience reach and listening hours"),
        (_uuid(), "Station Performance - YTD", "Year-to-date station performance metrics"),
        (_uuid(), "Market Comparison Report", "Cross-market audience comparison"),
    ]

    for report_id, name, desc in report_data:
        cur.execute(
            "INSERT INTO triton_reports VALUES (%s,%s,%s,%s,%s)",
            (report_id, name, desc, now, now),
        )
        for day_offset in range(30):
            date = _past_date(30 - day_offset)
            for station, market, country in random.sample(stations, k=random.randint(3, 6)):
                sessions = random.randint(5000, 200000)
                unique = int(sessions * random.uniform(0.4, 0.8))
                cur.execute(
                    "INSERT INTO triton_report_data "
                    "(report_id,date,station_name,market,country,sessions,"
                    "total_listening_hours,avg_listening_duration,cume,impressions,"
                    "downloads,unique_listeners,peak_listeners) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        report_id, date, station, market, country, sessions,
                        round(sessions * random.uniform(0.3, 1.5), 2),
                        round(random.uniform(5, 45), 2),
                        int(unique * random.uniform(1.2, 2.0)),
                        random.randint(10000, 500000),
                        random.randint(100, 50000),
                        unique,
                        random.randint(500, int(sessions * 0.3)),
                    ),
                )


def _hs_settings():
    from app.config import get_hivestack_settings
    return get_hivestack_settings()


def _hs_sample_dir():
    """The sample capture directory when seed mode is "sample" and it exists, else None."""
    s = _hs_settings()
    if s["seed"] != "sample":
        return None
    d = Path(s["sample_dir"])
    if d.is_dir() and any((d / f"{stem}.json").exists() for stem, _, _ in CAPTURE_ENTITIES):
        return d
    return None


def _hs_organization_id():
    """Organization for new records: the captured accounts' organization in sample
    mode, else 5021 (the synthetic buyer organization)."""
    d = _hs_sample_dir()
    if d and (d / "accounts.json").exists():
        try:
            for r in _hs_read_capture(d / "accounts.json"):
                if r.get("organization_id") is not None:
                    return r["organization_id"]
        except (ValueError, OSError):
            pass
    return 5021


# HS_ORGANIZATION_ID is set at the end of this module, once _hs_read_capture exists.

# Tables whose shape changed with the Hivestack v2 spec (app/specs/hivestack.openapi.json),
# plus tables the v2 shape replaced. Children first so SQLite FK checks never trip.
_HS_V2_TABLES = [
    "hs_creative_approvals", "hs_creative_units", "hs_lineitem_creatives",
    "hs_lineitem_deal_creatives", "hs_lineitem_targeting", "hs_lineitem_units",
    "hs_lineitems", "hs_campaigns", "hs_creatives", "hs_advertisers", "hs_accounts",
    "hs_unit_languages", "hs_unit_properties", "hs_unitpack_targeting", "hs_unitpack_units",
    "hs_concentrations", "hs_units", "hs_sites", "hs_networks", "hs_unitpacks",
    "hs_demographics", "hs_languages", "hs_locations", "hs_mediatypes",
    "hs_report_executions", "hs_report_definitions",
]


def _migrate_hivestack_v2(conn):
    """Drop Hivestack tables still in the pre-v2 shape (TEXT uuid `id` keys).

    CREATE TABLE IF NOT EXISTS never alters an existing table, and the v2
    shape changes every primary key, so the old tables are dropped and the
    schema step recreates them. Only seeded mock rows live there; they are
    re-seeded by _seed_hivestack. Runs before init_db_schema. No-op on a
    fresh database or one already on v2.
    """
    from app.db_backend import BACKEND

    cur = conn.cursor()
    if BACKEND == "sqlite":
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hs_accounts'")
        if not cur.fetchall():
            return
        cur.execute("PRAGMA table_info(hs_accounts)")
        columns = {r["name"] for r in cur.fetchall()}
    else:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            ("hs_accounts",),
        )
        columns = {r["column_name"] for r in cur.fetchall()}
        if not columns:
            return
    if "account_id" in columns:
        return

    suffix = "" if BACKEND == "sqlite" else " CASCADE"
    for table in _HS_V2_TABLES:
        cur.execute(f"DROP TABLE IF EXISTS {table}{suffix}")
    conn.commit()
    print("Hivestack: dropped pre-v2 tables; they will be recreated and re-seeded.")


def _hs_ts(days_ago=0.0):
    """Hivestack-style timestamp: ISO 8601, milliseconds, no UTC offset."""
    return (datetime.utcnow() - timedelta(days=days_ago)).isoformat(timespec="milliseconds")


def _hs_insert(cur, table, record, json_fields=None):
    """Insert a dict whose keys are column names. JSON columns (app.hivestack_model) are encoded."""
    if json_fields is None:
        json_fields = JSON_FIELDS.get(table, ())
    cols = list(record)
    vals = [
        (None if record[c] is None else json.dumps(record[c])) if c in json_fields else record[c]
        for c in cols
    ]
    cur.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
        vals,
    )


def _hs_empty_inventory_targeting(targeting_id):
    """Creative inventory_targeting object with every key the live API returns."""
    t = {k: [] for k in (
        "creative_format_exclusions", "creative_formats", "creative_orientation_exclusions",
        "creative_orientations", "geofencegroup_exclusion_ids", "geofencegroup_ids",
        "iab_categories", "iab_category_exclusions", "language_exclusion_ids", "language_ids",
        "location_exclusion_ids", "location_ids", "market_exclusion_ids", "market_ids",
        "mediatype_exclusion_ids", "mediatype_ids", "network_exclusion_ids", "network_ids",
        "owner_ids", "site_exclusion_ids", "site_ids", "unit_exclusion_ids", "unit_ids",
        "unit_tag_exclusions", "unit_tags", "unitpack_exclusion_ids", "unitpack_ids",
    )}
    t.update({"co2e_emissions_score_threshold": None, "complex_targeting": None,
              "inventorytargeting_id": targeting_id})
    return {k: t[k] for k in sorted(t)}


def _seed_hivestack(cur, now):
    """Seed Hivestack DOOH mock data.

    Two independent parts so a v2 migration (which drops only the v2 tables)
    re-seeds the v2 resources without duplicating the RTB data.
    """
    _seed_hivestack_rtb(cur, now)
    _seed_hivestack_v2(cur)


def _seed_hivestack_rtb(cur, now):
    """Publishers, OpenRTB screens, deals and custom events."""
    cur.execute("SELECT COUNT(*) FROM hs_publishers")
    if cur.fetchone()["count"] > 0:
        return

    publishers = [
        ("hs-pub-001", "Outfront Media", "outfront.com", "PUBLISHER"),
        ("hs-pub-002", "Lamar Advertising", "lamar.com", "PUBLISHER"),
        ("hs-pub-003", "Clear Channel Outdoor", "clearchannel.com", "PUBLISHER"),
        ("hs-pub-004", "Pattison Outdoor", "pattison.com", "PUBLISHER"),
        ("hs-pub-005", "Astral Media", "astral.com", "INTERMEDIARY"),
    ]
    for pub_id, name, domain, seller_type in publishers:
        cur.execute(
            "INSERT INTO hs_publishers (id, name, domain, seller_type, created_at) VALUES (%s,%s,%s,%s,%s)",
            (pub_id, name, domain, seller_type, now),
        )

    # Screen definitions: (screen_id, publisher_idx, display_manager, w, h, audio, venue_type_id,
    #                       lat, lon, country, region, city, zip, utcoffset, network_id, site_id)
    screens = [
        ("SCR-001", 0, "MANAGER-OF-001", 1920, 1080, 0, 105,
         40.7128, -74.0060, "USA", "NY", "NEW YORK", "10001", -300, "NET-OF-001", "SITE-OF-001"),
        ("SCR-002", 0, "MANAGER-OF-001", 1080, 1920, 0, 205,
         40.7580, -73.9855, "USA", "NY", "NEW YORK", "10036", -300, "NET-OF-001", "SITE-OF-002"),
        ("SCR-003", 1, "MANAGER-LA-001", 1920, 1080, 1, 105,
         34.0522, -118.2437, "USA", "CA", "LOS ANGELES", "90001", -480, "NET-LA-001", "SITE-LA-001"),
        ("SCR-004", 1, "MANAGER-LA-001", 1280, 720, 0, 301,
         34.0195, -118.4912, "USA", "CA", "LOS ANGELES", "90210", -480, "NET-LA-001", "SITE-LA-002"),
        ("SCR-005", 2, "MANAGER-CC-001", 1920, 1080, 0, 105,
         41.8781, -87.6298, "USA", "IL", "CHICAGO", "60601", -360, "NET-CC-001", "SITE-CC-001"),
        ("SCR-006", 2, "MANAGER-CC-001", 1080, 1920, 0, 205,
         41.8827, -87.6233, "USA", "IL", "CHICAGO", "60602", -360, "NET-CC-001", "SITE-CC-002"),
        ("SCR-007", 3, "MANAGER-PA-001", 1920, 1080, 0, 105,
         43.6532, -79.3832, "CAN", "ON", "TORONTO", "M5H2N2", -300, "NET-PA-001", "SITE-PA-001"),
        ("SCR-008", 3, "MANAGER-PA-001", 1280, 720, 1, 401,
         45.5017, -73.5673, "CAN", "QC", "MONTREAL", "H2Y2P1", -300, "NET-PA-001", "SITE-PA-002"),
        ("SCR-009", 4, "MANAGER-AS-001", 1920, 1080, 0, 105,
         45.4215, -75.6972, "CAN", "ON", "OTTAWA", "K1A0A9", -300, "NET-AS-001", "SITE-AS-001"),
        ("SCR-010", 0, "MANAGER-OF-001", 1920, 1080, 0, 501,
         40.6892, -74.0445, "USA", "NJ", "JERSEY CITY", "07302", -300, "NET-OF-001", "SITE-OF-003"),
    ]

    pub_ids = [p[0] for p in publishers]
    for (scr_id, pub_idx, mgr, w, h, audio, venue_type,
         lat, lon, country, region, city, zip_code, utcoffset, net_id, site_id) in screens:
        ifa = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO hs_screens (id, screen_id, publisher_id, publisher_name, publisher_domain, "
            "display_manager, display_manager_ver, width, height, supports_banner, supports_video, "
            "audio, venue_type_id, geo_lat, geo_lon, geo_country, geo_region, geo_city, geo_zip, "
            "geo_utcoffset, ifa, network_id, site_id, aspect_ratio_tolerance, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                ifa, scr_id, pub_ids[pub_idx], publishers[pub_idx][1], publishers[pub_idx][2],
                mgr, "1.0", w, h, True, True,
                audio, venue_type, lat, lon, country, region, city, zip_code,
                utcoffset, ifa, net_id, site_id, 0.1, now,
            ),
        )

    # Deals: (id, publisher_idx, name, bidfloor, bidfloorcur, deal_type, must_bid, wseat)
    deals = [
        ("DEAL-OF-001", 0, "Outfront Premium NYC", 2.50, "USD", 1, 1, "seat-001"),
        ("DEAL-OF-002", 0, "Outfront Non-Guaranteed NYC", 1.00, "USD", 2, 0, "seat-001"),
        ("DEAL-LA-001", 1, "Lamar LA Preferred", 1.75, "USD", 3, 0, "seat-002"),
        ("DEAL-LA-002", 1, "Lamar LA Guaranteed", 3.00, "USD", 1, 1, "seat-002"),
        ("DEAL-CC-001", 2, "Clear Channel Chicago Open", 0.50, "USD", 2, 0, None),
        ("DEAL-PA-001", 3, "Pattison Canada Guaranteed", 2.00, "CAD", 1, 1, "seat-003"),
        ("DEAL-PA-002", 3, "Pattison Canada Non-Guaranteed", 0.87, "CAD", 2, 0, "seat-003"),
        ("DEAL-AS-001", 4, "Astral Ottawa Open", 0.75, "CAD", 2, 0, None),
    ]
    for deal_id, pub_idx, name, bidfloor, cur_code, deal_type, must_bid, wseat in deals:
        cur.execute(
            "INSERT INTO hs_deals (id, publisher_id, name, bidfloor, bidfloorcur, deal_type, must_bid, wseat, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (deal_id, pub_ids[pub_idx], name, bidfloor, cur_code, deal_type, must_bid, wseat, now),
        )

    for ename in ["Super Bowl Weekend", "Black Friday", "Holiday Season"]:
        cur.execute(
            "INSERT INTO hs_custom_events (id, name, description, status, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid.uuid4()), ename, f"Mock event: {ename}", "active", now, now))


def _seed_hivestack_v2(cur):
    """Hivestack API v2 resources in the live-API shape (see app/specs/hivestack.openapi.json).

    Mode comes from config.yml `hivestack.seed`:
      sample    — load the captured live records (_load_hivestack_capture), then
                  synthesize the resources the capture lacks around the real IDs
      synthetic — generate everything
    Enum-like strings (state, run_state, campaign_type, budget_mode, ...) in the
    synthesized rows are plausible placeholders: the spec gives their type, not values.
    """
    cur.execute("SELECT COUNT(*) FROM hs_accounts")
    if cur.fetchone()["count"] > 0:
        return

    rng = random.Random(20261110)  # deterministic seed data
    sample = _hs_sample_dir()
    if sample:
        counts = _load_hivestack_capture(cur, sample)
        print(f"Hivestack: loaded sample capture from {sample}: {counts}")
    else:
        if _hs_settings()["seed"] == "sample":
            print(f"Hivestack: sample dir {_hs_settings()['sample_dir']} not found; seeding synthetic data.")
        _seed_hivestack_v2_synthetic(cur, rng)
    _seed_hivestack_v2_dependents(cur, rng)


# ── Sample capture import ─────────────────────────────────────────────────────

def _hs_table_columns(table):
    """Column names of a table, read from SCHEMA (backend-neutral)."""
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\);", SCHEMA, re.S)
    if not m:
        raise KeyError(table)
    return {line.strip().split()[0] for line in m.group(1).splitlines()
            if line.strip() and not line.strip().startswith(("PRIMARY", "UNIQUE", "--"))}


def _hs_read_capture(path):
    """Records from one captured file: {"_meta": ..., "records": [...]} or a bare list."""
    data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError(f"{path}: expected a JSON list or an object with a 'records' list")


def _hs_upsert(cur, table, pk, record, update=True):
    """INSERT ... ON CONFLICT (pk) DO UPDATE (or DO NOTHING). Works on Postgres and SQLite."""
    json_fields = JSON_FIELDS.get(table, ())
    cols = list(record)
    vals = [(None if record[c] is None else json.dumps(record[c])) if c in json_fields else record[c]
            for c in cols]
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)
    action = f"DO UPDATE SET {sets}" if update and sets else "DO NOTHING"
    cur.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
        f"ON CONFLICT ({pk}) {action}",
        vals,
    )


def _load_hivestack_capture(cur, directory):
    """Upsert captured live records from `directory` into the hs_ v2 tables.

    Client data is sanitized first (app/hivestack_sanitize.py): names, addresses,
    domains, logos, external IDs and UUIDs are replaced, and numeric IDs and
    structure are kept. Raw values are never written. Column names equal the
    live field names. Fields with no column are dropped with a warning, and
    unrecognised files are skipped. Media types and location extras are
    derived afterwards. Returns {entity: record count}.
    """
    from app.hivestack_sanitize import sanitize

    directory = Path(directory)
    known = {stem for stem, _, _ in CAPTURE_ENTITIES} | {"sample_summary"}
    for f in sorted(directory.glob("*.json")):
        if f.stem not in known:
            print(f"Hivestack import: skipping {f.name} (no table mapping)")

    captured = {stem: _hs_read_capture(directory / f"{stem}.json")
                for stem, _, _ in CAPTURE_ENTITIES if (directory / f"{stem}.json").exists()}
    captured = sanitize(captured)

    loaded = {}
    for stem, table, pk in CAPTURE_ENTITIES:
        if stem not in captured:
            continue
        records = captured[stem]
        columns = _hs_table_columns(table)
        dropped = sorted({k for r in records for k in r} - columns)
        if dropped:
            print(f"Hivestack import: {stem}: no column for {dropped}; values dropped")
        for r in records:
            _hs_upsert(cur, table, pk, {k: v for k, v in r.items() if k in columns})
        loaded[stem] = records

    _hs_derive_mediatypes(cur, loaded.get("units", []))
    _hs_derive_location_extras(cur, loaded.get("locations", []), loaded.get("units", []))
    return {stem: len(r) for stem, r in loaded.items()}


def _hs_derive_mediatypes(cur, units):
    """Media types from the object embedded in each unit, plus stubs for missing parents.

    A stub (code = the child code's prefix) only fills a gap so /mediatypes forms a
    tree; it never overwrites an existing row. A real /mediatypes capture replaces it.
    """
    found = {}
    for u in units:
        mt = u.get("media_type")
        if mt and mt.get("mediatype_id") is not None:
            found[mt["mediatype_id"]] = mt
    for mt in found.values():
        _hs_upsert(cur, "hs_mediatypes", "mediatype_id", {
            k: mt.get(k) for k in ("mediatype_id", "archived", "code", "enumeration_id", "name",
                                   "parent_mediatype_id")})
    for mt in found.values():
        parent = mt.get("parent_mediatype_id")
        if parent is None or parent in found:
            continue
        code = (mt.get("code") or "").split(".")[0] or f"mediatype_{parent}"
        _hs_upsert(cur, "hs_mediatypes", "mediatype_id", {
            "mediatype_id": parent, "archived": False, "code": code, "enumeration_id": parent,
            "name": code.replace("_", " ").title(), "parent_mediatype_id": None,
        }, update=False)


def _hs_derive_location_extras(cur, locations, units):
    """Fill hierarchy / archived / version, which GET /locations doesn't return.

    Taken from the location object embedded in units where one exists; otherwise
    hierarchy is built from the country_id / region_id chain and version borrowed
    from another location in the same country.
    """
    embedded = {u["location"]["location_id"]: u["location"] for u in units
                if isinstance(u.get("location"), dict) and u["location"].get("location_id") is not None}
    by_id = {loc["location_id"]: loc for loc in locations}
    version_by_country = {}
    for e in embedded.values():
        if e.get("version"):
            version_by_country.setdefault(e.get("country_id"), e["version"])

    for loc in locations:
        e = embedded.get(loc["location_id"])
        if e:
            hierarchy, archived, version = e.get("hierarchy"), bool(e.get("archived")), e.get("version")
        else:
            parts = []
            for ref in (loc.get("country_id"), loc.get("region_id")):
                if ref is not None and ref != loc["location_id"] and ref in by_id:
                    parts.append(by_id[ref]["name"])
            parts.append(loc["name"])
            country = loc.get("country_id") if loc.get("country_id") is not None else loc["location_id"]
            hierarchy, archived, version = " > ".join(parts), False, version_by_country.get(country)
        cur.execute("UPDATE hs_locations SET hierarchy = %s, archived = %s, version = %s WHERE location_id = %s",
                    (hierarchy, archived, version, loc["location_id"]))


# ── Synthetic data ────────────────────────────────────────────────────────────

def _seed_hivestack_v2_synthetic(cur, rng):
    """Generated locations, media types, inventory, accounts and advertisers
    (the resources a sample capture would otherwise provide)."""
    org = HS_ORGANIZATION_ID
    always = "1" * 168

    # (location_id, code, name, level, country_id, region_id, lat, lon, hierarchy)
    locations = [
        (1, "US", "United States", 1, None, None, None, None, "United States"),
        (2, "CA", "Canada", 1, None, None, None, None, "Canada"),
        (101, "US-NY", "New York", 2, 1, None, None, None, "United States > New York"),
        (102, "US-CA", "California", 2, 1, None, None, None, "United States > California"),
        (103, "US-IL", "Illinois", 2, 1, None, None, None, "United States > Illinois"),
        (104, "US-DC", "District of Columbia", 2, 1, None, None, None, "United States > District of Columbia"),
        (201, "CA-ON", "Ontario", 2, 2, None, None, None, "Canada > Ontario"),
        (202, "CA-QC", "Quebec", 2, 2, None, None, None, "Canada > Quebec"),
        (10001, "US-NY-NYC", "New York City", 3, 1, 101, "40.7128", "-74.0060", "United States > New York > New York City"),
        (10002, "US-CA-LAX", "Los Angeles", 3, 1, 102, "34.0522", "-118.2437", "United States > California > Los Angeles"),
        (10003, "US-IL-CHI", "Chicago", 3, 1, 103, "41.8781", "-87.6298", "United States > Illinois > Chicago"),
        (10004, "US-DC-WAS", "Washington", 3, 1, 104, "38.9072", "-77.0369", "United States > District of Columbia > Washington"),
        (20001, "CA-ON-TOR", "Toronto", 3, 2, 201, "43.6532", "-79.3832", "Canada > Ontario > Toronto"),
        (20002, "CA-ON-MIS", "Mississauga", 3, 2, 201, "43.5890", "-79.6441", "Canada > Ontario > Mississauga"),
        (20003, "CA-QC-MTL", "Montreal", 3, 2, 202, "45.5017", "-73.5673", "Canada > Quebec > Montreal"),
    ]
    loc_by_id = {}
    for loc_id, code, name, level, country_id, region_id, lat, lon, hierarchy in locations:
        row = {"location_id": loc_id, "code": code, "country_id": country_id, "latitude": lat,
               "level": level, "longitude": lon, "name": name, "region_id": region_id,
               "hierarchy": hierarchy, "archived": False, "version": "1"}
        loc_by_id[loc_id] = row
        _hs_insert(cur, "hs_locations", row)

    # (mediatype_id, code, name, enumeration_id, parent_mediatype_id)
    mediatypes = [
        (1, "outdoor", "Outdoor", 1, None), (2, "transit", "Transit", 2, None),
        (3, "retail", "Retail", 3, None), (4, "airports", "Airports", 4, None),
        (101, "outdoor.billboards", "Billboards", 101, 1),
        (102, "outdoor.street_furniture", "Street Furniture", 102, 1),
        (201, "transit.subway", "Subway", 201, 2),
        (202, "transit.train_stations", "Train Stations", 202, 2),
        (301, "retail.malls", "Malls", 301, 3),
        (401, "airports.terminals", "Terminals", 401, 4),
    ]
    mt_by_id = {}
    for mt_id, code, name, enum_id, parent in mediatypes:
        row = {"mediatype_id": mt_id, "archived": False, "code": code, "enumeration_id": enum_id,
               "name": name, "parent_mediatype_id": parent}
        mt_by_id[mt_id] = row
        _hs_insert(cur, "hs_mediatypes", row)

    # Media-owner IDs 701-705 (one per hs_publishers row, same order)
    networks = [
        (2201, "DDN", "Downtown Digital Network", 701), (2202, "TMN", "Transit Media Network", 702),
        (2203, "MALL", "Mall Network", 703), (2204, "AIR", "Airport Network", 704),
    ]
    for net_id, code, name, owner in networks:
        ts = _hs_ts(rng.uniform(300, 600))
        _hs_insert(cur, "hs_networks", {
            "network_id": net_id, "code": code, "created_on_utc": ts, "loop_length": None,
            "modified_on_utc": ts, "name": name, "owner_id": owner, "spot_length": None,
        })

    # (site_id, name, owner, location_id, lat, lon, network_id, mediatype_id, timezone)
    sites = [
        (9101, "Times Square Hub", 701, 10001, 40.7580, -73.9855, 2201, 101, "America/New_York"),
        (9102, "Union Station", 702, 10004, 38.8973, -77.0063, 2202, 202, "America/New_York"),
        (9103, "Eaton Centre", 703, 20001, 43.6544, -79.3807, 2203, 301, "America/Toronto"),
        (9104, "Pearson Airport T1", 704, 20002, 43.6777, -79.6248, 2204, 401, "America/Toronto"),
        (9105, "LAX Terminal 4", 704, 10002, 33.9425, -118.4081, 2204, 401, "America/Los_Angeles"),
    ]
    for s_idx, (site_id, sname, owner, loc_id, lat, lon, net_id, mt_id, tz) in enumerate(sites):
        ts = _hs_ts(rng.uniform(200, 500))
        _hs_insert(cur, "hs_sites", {
            "site_id": site_id, "active": True, "created_on_utc": ts, "description": f"{sname} DOOH venue",
            "external_id": f"EXT-{sname[:4].upper()}", "latitude": lat, "location_id": loc_id,
            "longitude": lon, "modified_on_utc": ts, "name": sname, "owner_id": owner,
            "uuid": str(uuid.UUID(int=rng.getrandbits(128))),
        })
        loc = loc_by_id[loc_id]
        location_obj = {k: loc[k] for k in ("archived", "code", "country_id", "hierarchy", "latitude",
                                            "level", "location_id", "longitude", "name", "region_id",
                                            "version")}
        mt = mt_by_id[mt_id]
        for j in range(2):
            unit_id = 30101 + s_idx * 2 + j
            w, h = rng.choice([(1920, 1080), (1080, 1920), (1280, 720)])
            heard = round(rng.uniform(5, 900), 1)
            played = round(rng.uniform(5, 3600), 1)
            ts = _hs_ts(rng.uniform(100, 400))
            _hs_insert(cur, "hs_units", {
                "unit_id": unit_id, "active": True,
                "allow_audio": False, "allow_flash": False, "allow_html": rng.random() < 0.5,
                "allow_image": True, "allow_video": True, "allow_zip": False,
                "available_for_adserver": True, "available_for_deals": True,
                "available_for_open_exchange": rng.random() < 0.7, "available_for_storefront": False,
                "blacklist_unique_advertiser_ids": [], "created_on_utc": ts,
                "description": f"{sname} screen {j + 1}",
                "enable_strict_iab_blacklisting": False, "enable_strict_iab_frequency_capping": False,
                "external_id": f"SCR-{s_idx + 1:02d}{j + 1}", "facing_direction": None,
                "floor_cpm": round(rng.uniform(2, 12), 2), "image_uri": None,
                "impression_verifier_vendor": "geopath", "impression_verifier_vendor_external_id": None,
                "latitude": round(lat + rng.uniform(-0.002, 0.002), 6), "location": location_obj,
                "location_id": loc_id, "longitude": round(lon + rng.uniform(-0.002, 0.002), 6),
                "loop_length": 120, "max_spot_length": 30, "media_type": mt, "mediatype_id": mt_id,
                "mediatype_name": mt["name"], "min_seconds_between_ad_domain_plays": 0,
                "min_seconds_between_creative_category_plays": 0, "min_seconds_between_iab_category_plays": 0,
                "min_spot_length": 5, "modified_on_utc": ts, "name": f"{sname} - Screen {j + 1}",
                "network_id": net_id, "operating_hours": always, "owner_id": owner,
                "physical_screen_height_cm": rng.choice([None, 120, 300]),
                "physical_screen_width_cm": rng.choice([None, 210, 540]), "physical_unit_count": None,
                "screen_height": h, "screen_width": w, "site_id": site_id, "spot_length": 15,
                "timezone": tz, "uuid": str(uuid.UUID(int=rng.getrandbits(128))),
                "weekly_impressions": rng.randint(20000, 400000), "weekly_traffic": rng.randint(50000, 900000),
                "last_heard_from_seconds_ago": heard, "last_heard_from_utc": _hs_ts(heard / 86400),
                "last_played_on_seconds_ago": played, "last_played_on_utc": _hs_ts(played / 86400),
                "has_audience_data": unit_id in (30101, 30102),
            })

    for acct_id, name, ext in [(3101, "Horizon Media Group", "HZN-001"), (3102, "Omnicom Media", None),
                               (3103, "Publicis Groupe", None)]:
        _hs_insert(cur, "hs_accounts", {"account_id": acct_id, "external_id": ext, "logo_url": None,
                                        "name": name, "organization_id": org})

    advertisers = [("Acme Corp DOOH", "acme.com"), ("TechNova Outdoor", "technova.com"),
                   ("GreenLeaf OOH", "greenleaf.com"), ("Sunset Travel", "sunsettravel.com"),
                   ("Amazon Ads", "amazon.com")]
    for i, (name, domain) in enumerate(advertisers):
        ts = _hs_ts(rng.uniform(100, 700))
        _hs_insert(cur, "hs_advertisers", {
            "advertiser_id": 48201 + i, "created_on_utc": ts, "external_id": None, "modified_on_utc": ts,
            "name": name, "organization_id": org,
            "unique_advertiser": {"advertiser_domains": [{"domain": domain}], "logo_url": None,
                                  "name": name, "uniqueadvertiser_id": 77001 + i},
        })


def _seed_hivestack_v2_dependents(cur, rng):
    """Resources a sample capture doesn't cover, built on whatever units,
    advertisers and accounts are in the tables (captured or synthetic):
    languages, demographics, unit packs, unit languages/properties,
    concentrations, creatives, campaigns, line items and their creative
    links, creative approvals, and reports."""
    org = HS_ORGANIZATION_ID
    always = "1" * 168

    cur.execute("SELECT unit_id, owner_id, has_audience_data FROM hs_units ORDER BY unit_id")
    unit_rows = cur.fetchall()
    unit_ids = [r["unit_id"] for r in unit_rows]
    unit_owner = {r["unit_id"]: r["owner_id"] for r in unit_rows}
    cur.execute("SELECT advertiser_id FROM hs_advertisers ORDER BY advertiser_id")
    adv_ids = [r["advertiser_id"] for r in cur.fetchall()]
    cur.execute("SELECT account_id FROM hs_accounts ORDER BY account_id")
    acct_ids = [r["account_id"] for r in cur.fetchall()]

    # ── Reference data ────────────────────────────────────────────────────────
    for lang_id, code, name in [(1, "en", "English"), (2, "fr", "French"), (3, "es", "Spanish"),
                                (4, "zh", "Chinese"), (5, "de", "German"), (6, "ja", "Japanese")]:
        _hs_insert(cur, "hs_languages", {"language_id": lang_id, "code": code, "name": name})

    for demo_id, code, name, dtype in [
        (801, "GNDR-M|AGE-18-34", "Male 18-34", "age_gender"),
        (802, "GNDR-F|AGE-18-34", "Female 18-34", "age_gender"),
        (803, "AGE-25-54", "Adults 25-54", "age"), (804, "HHI-100K-*", "High Income", "income"),
        (805, "AGE-18-24", "Young Adults", "age"), (806, "AGE-65-*", "Seniors 65+", "age"),
    ]:
        ts = _hs_ts(rng.uniform(200, 400))
        _hs_insert(cur, "hs_demographics", {
            "demographic_id": demo_id, "code": code, "created_on_utc": ts, "modified_on_utc": ts,
            "name": name, "organization_id": None, "show_buyers": True, "type": dtype})

    # ── Inventory extras ──────────────────────────────────────────────────────
    if unit_ids:
        packs = [(4401, "Premium Pack", unit_ids[0:2]), (4402, "Commuter Pack", unit_ids[2:4]),
                 (4403, "Weekend Pack", unit_ids[-4:])]
        for pack_id, pname, members in packs:
            if not members:
                continue
            ts = _hs_ts(rng.uniform(30, 200))
            _hs_insert(cur, "hs_unitpacks", {
                "unitpack_id": pack_id, "active": True, "created_on_utc": ts, "description": f"Mock {pname}",
                "external_id": f"PACK-{pack_id}", "modified_on_utc": ts, "name": pname,
                "owner_id": unit_owner[members[0]], "prepopulation_option": "none"})
            for unit_id in members:
                _hs_insert(cur, "hs_unitpack_units", {"unitpack_id": pack_id, "unit_id": unit_id})

    for i, unit_id in enumerate(unit_ids[:4]):
        ts = _hs_ts(rng.uniform(10, 100))
        _hs_insert(cur, "hs_unit_languages", {
            "unitlanguage_id": 89001 + i, "created_on_utc": ts, "language_id": 1,
            "modified_on_utc": ts, "unit_id": unit_id})
    prop_id = 89501
    for unit_id in unit_ids[:4]:
        for name, value in [("brightness", "high"), ("orientation", "landscape"), ("audio", "0")]:
            ts = _hs_ts(rng.uniform(10, 100))
            _hs_insert(cur, "hs_unit_properties", {
                "unitproperty_id": prop_id, "created_on_utc": ts, "modified_on_utc": ts,
                "name": name, "unit_id": unit_id, "value": value})
            prop_id += 1

    stamp = _hs_ts()
    for r in unit_rows:
        if not r["has_audience_data"]:
            continue
        for dow in range(7):
            for hour in range(24):
                cur.execute(
                    "INSERT INTO hs_concentrations (unit_id, day_of_week, hour, audience_count, updated_at) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (r["unit_id"], dow, hour, round(rng.uniform(5, 200), 1), stamp))

    if not (unit_ids and adv_ids):
        return

    # ── Buy side ──────────────────────────────────────────────────────────────
    creatives_by_adv = {a: [] for a in adv_ids}
    for i in range(10):
        creative_id = 85001 + i
        adv_id = adv_ids[i % len(adv_ids)]
        creatives_by_adv[adv_id].append(creative_id)
        is_video = i % 2 == 0
        w, h = rng.choice([(1920, 1080), (1080, 1920), (1280, 720)])
        state = rng.choice(["approved", "approved", "pending"])
        created = _hs_ts(rng.uniform(20, 120))
        targeting = _hs_empty_inventory_targeting(87001 + i)
        targeting["unit_ids"] = sorted(rng.sample(unit_ids, min(3, len(unit_ids))))
        ext = "mp4" if is_video else "jpg"
        _hs_insert(cur, "hs_creatives", {
            "creative_id": creative_id, "advertiser_id": adv_id, "created_at": created,
            "creative_files": [{
                "archived": False, "creative_id": creative_id, "creativefile_id": 86001 + i,
                "duration": 15.0 if is_video else None, "external_id": f"CF-{86001 + i}",
                "file_size": rng.randint(200_000, 25_000_000), "file_validation_errors": None,
                "format": "video/mp4" if is_video else "image/jpeg", "generated": False,
                "height": h, "is_vast": False, "name": f"creative_{i + 1}.{ext}", "placement_type": None,
                "prefetch_approval": None, "prefetch_is_external": False, "tag_content": None,
                "tag_type": None, "url": f"https://cdn.mock.local/creative_{i + 1}.{ext}",
                "uuid": str(uuid.UUID(int=rng.getrandbits(128))), "width": w,
            }],
            "daypart": always, "iab_categories": [rng.choice(["IAB3", "IAB8", "IAB19", "IAB20"])],
            "inventory_targeting": targeting, "language_ids": [1], "name": f"DOOH Creative {i + 1}",
            "organization_id": org, "retired_at": None,
            "run_state": "active" if state == "approved" else "inactive",
            "state": state, "submitted_at": created,
            "uuid": str(uuid.UUID(int=rng.getrandbits(128))), "last_played_time_utc": None,
        })

    # About 10 campaigns: 2 per advertiser for small advertiser lists, else 1 each.
    per_adv = 2 if len(adv_ids) <= 5 else 1
    campaigns = []  # (campaign_id, advertiser_id)
    for i, adv_id in enumerate(adv_ids[:10]):
        account_id = acct_ids[i * len(acct_ids) // len(adv_ids)] if acct_ids else None
        for j in range(per_adv):
            campaigns.append((61001 + len(campaigns), adv_id, account_id, i, j))
    for n, (campaign_id, adv_id, account_id, i, j) in enumerate(campaigns):
        ts = _hs_ts(rng.uniform(10, 90))
        budget = round(rng.uniform(5000, 100000), 2)
        _hs_insert(cur, "hs_campaigns", {
            "campaign_id": campaign_id,
            "account_id": None if n == len(campaigns) - 1 else account_id,  # account is optional
            "advertiser_id": adv_id, "budget": budget, "budget_cap": budget if j == 0 else None,
            "campaign_properties": [{"key": "po_number", "value": f"PO-{campaign_id}"}],
            "campaign_type": "standard", "cap": None, "created_on_utc": ts,
            "end_date": _future_date(30), "min_seconds_between_plays": 0, "modified_on_utc": ts,
            "name": f"DOOH Campaign {i + 1}-{j + 1}", "notes": None, "organization_id": org,
            "pacing_budget": None, "pacing_budget_mode": None, "pacing_strategy": None,
            "priority": None, "start_date": _past_date(60),
        })

    deal_ids = ["DEAL-OF-001", "DEAL-LA-001", "DEAL-PA-001"]
    lineitem_id = 72001
    lic_id = 88001
    for campaign_id, adv_id, _acct, _i, _j in campaigns[:6]:
        creative_pool = creatives_by_adv[adv_id] or [85001]
        for k in range(rng.randint(1, 3)):
            targeted = sorted(rng.sample(unit_ids, rng.randint(min(2, len(unit_ids)), min(4, len(unit_ids)))))
            owner_ids = sorted({unit_owner[u] for u in targeted if unit_owner[u] is not None})
            creative_ids = creative_pool[: rng.randint(1, 2)]
            deal_id = deal_ids.pop(0) if deal_ids else None
            state = rng.choice(["approved", "draft"])
            ts = _hs_ts(rng.uniform(5, 60))
            record = {
                "lineitem_id": lineitem_id, "ad_duration": 15,
                "budget": round(rng.uniform(1000, 20000), 2), "budget_mode": "total",
                "cadence": 1.0, "cadence_smooth_catchup": False, "campaign_id": campaign_id,
                "cancelled_at": None, "cap": None, "cpp": None, "created_at": ts,
                "created_by": "buyer@mock.local", "created_on_utc": ts, "creative_ids": creative_ids,
                "daypart": always, "delivery_curve": "even", "demographic_ids": [], "external_id": None,
                "flight_from": _past_date(30), "flight_to": _future_date(30),
                "is_cpm_from_rate_card": False, "lineitem_properties": [],
                "market_targeted_impressions": rng.choice([None, rng.randint(10000, 500000)]),
                "max_bid_cpm": None, "mediaowner_ids": owner_ids,
                "min_concentration_factor": 0.0, "min_seconds_between_plays": 0, "modified_on_utc": ts,
                "name": f"Line Item {k + 1}", "notes": None, "package_id": None, "priority": 5,
                "purchase_type": "deal" if deal_id else "direct",
                "run_state": "running" if state == "approved" else "not_started", "state": state,
                "submitted_at": ts if state == "approved" else None, "unit_ids": targeted,
                "cpm_net": [],
            }
            for owner in owner_ids:
                cpm = round(rng.uniform(4, 15), 2)
                record["cpm_net"].append({"owner_id": owner, "cpm": cpm, "cpm_min": round(cpm * 0.8, 2),
                                          "cpm_max": round(cpm * 1.2, 2), "cpm_avg": cpm})
            if deal_id:
                record["deal_id"] = deal_id
            _hs_insert(cur, "hs_lineitems", record)
            for creative_id in creative_ids:
                _hs_insert(cur, "hs_lineitem_creatives", {
                    "lineitemcreative_id": lic_id, "lineitem_id": lineitem_id, "creative_id": creative_id,
                    "local_from_date": None, "local_to_date": None, "weight": 1})
                lic_id += 1
            if deal_id:
                _hs_insert(cur, "hs_lineitem_deal_creatives", {
                    "lineitem_id": lineitem_id, "creative_id": creative_ids[0], "deal_id": deal_id,
                    "deal_flight_from": record["flight_from"], "deal_flight_to": record["flight_to"],
                    "local_from_date": None, "local_to_date": None})
            lineitem_id += 1

    for creative_id in range(85001, 85007):
        cur.execute(
            "INSERT INTO hs_creative_approvals (id, creative_id, media_owner_id, approval_status, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid.UUID(int=rng.getrandbits(128))), creative_id, "hs-pub-001",
             rng.choice(["approved", "pending", "approved"]), _hs_ts(5), _hs_ts(1)))

    # ── Reporting ─────────────────────────────────────────────────────────────
    report_defs = [(5501, "Impressions by Unit", ["unit_id"]),
                   (5502, "Campaign Performance", ["campaign_id", "lineitem_id"]),
                   (5503, "Audience Demographics Report", ["unit_id", "demographic_id"])]
    filter_campaigns = [c[0] for c in campaigns[:2]]
    for i, (def_id, name, dims) in enumerate(report_defs):
        executed = _hs_ts(1 + i)
        _hs_insert(cur, "hs_report_definitions", {
            "report_definition_id": def_id,
            "definition_json": {
                "breakdown": "day", "format": "csv", "metrics": ["impressions", "plays", "spend"],
                "dimensions": dims,
                "dates": {"mode": "relative", "start_date": None, "end_date": None,
                          "date_range": "last_7_days", "inclusive": True},
                "defaultReportOutputFormat": "csv", "timezone": "UTC",
                "filters": [{"property": "campaign_id", "values": filter_campaigns}],
                "options": {"include_header_bidder_data": False},
            },
            "exchange_id": None, "last_executed_on_utc": executed, "name": name, "organization_id": org,
        })
        exec_id = 5601 + i
        _hs_insert(cur, "hs_report_executions", {
            "report_execution_id": exec_id, "execution_finished_time_utc": executed,
            "execution_started_time_utc": executed, "progress": 100.0, "report_definition_id": def_id,
            "report_file_url": f"https://mock.adbridge.local/reports/{exec_id}.csv",
            "status_code": "completed", "status_message": None,
        })


def _migrate_adswizz_v9(cur):
    """Add v9 columns to AdsWizz tables created by an older schema.

    CREATE TABLE IF NOT EXISTS never alters an existing table, so a database
    seeded before v9 support lacks these columns. Postgres supports
    ADD COLUMN IF NOT EXISTS; SQLite does not, so check PRAGMA table_info.
    """
    from app.db_backend import BACKEND

    new_columns = [
        ("aw_advertisers", "agency_id", "INTEGER"),
        ("aw_ads", "missing_creative", "BOOLEAN DEFAULT FALSE"),
        # order push
        ("aw_orders", "managed_objective", "BOOLEAN DEFAULT FALSE"),
        ("aw_orders", "updated_at", "TEXT"),
        ("aw_campaigns", "category_ids", "TEXT"),
        ("aw_campaigns", "ad_clashing", "BOOLEAN DEFAULT FALSE"),
        ("aw_campaigns", "inventory_deal_id", "TEXT"),
        ("aw_campaigns", "updated_at", "TEXT"),
    ]
    for table, column, ddl in new_columns:
        if BACKEND == "sqlite":
            cur.execute(f"PRAGMA table_info({table})")
            if any(r["name"] == column for r in cur.fetchall()):
                continue
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        else:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ddl}")

    # Rows written before updated_at existed get a timestamp so
    # GET /orders?modifiedAfter= can compare them.
    stamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    for table in ("aw_orders", "aw_campaigns"):
        cur.execute(f"UPDATE {table} SET updated_at = %s WHERE updated_at IS NULL", (stamp,))


def _seed_adswizz(cur, now):
    """Seed AdsWizz Domain API v9 mock data."""
    _migrate_adswizz_v9(cur)
    cur.execute("SELECT COUNT(*) FROM aw_agencies")
    if cur.fetchone()["count"] > 0:
        return

    # --- Agencies ---
    agency_ids = []
    for aname, contact, email in [
        ("SoundWave Media", "Alice Park", "alice@soundwave.com"),
        ("AudioReach Group", "Tom Rivera", "tom@audioreach.com"),
    ]:
        cur.execute(
            "INSERT INTO aw_agencies (name, contact, email, currency, timezone, margin, created_at) "
            "VALUES (%s,%s,%s,'USD','America/New_York',15,%s) RETURNING id",
            (aname, contact, email, now))
        agency_ids.append(cur.fetchone()["id"])

    # --- Advertisers ---
    adv_data = [
        ("PodcastPro Inc", "www.podcastpro.com", "Jane Lee", "jane@podcastpro.com"),
        ("StreamAudio Co", "www.streamaudio.com", "Mark Chen", "mark@streamaudio.com"),
        ("VoiceAds Ltd", "www.voiceads.com", "Sara Kim", "sara@voiceads.com"),
        ("SonicBrand Media", "www.sonicbrand.com", "Leo Diaz", "leo@sonicbrand.com"),
    ]
    adv_ids = []
    for i, (name, domain, contact, email) in enumerate(adv_data):
        cur.execute(
            "INSERT INTO aw_advertisers (name, domain, contact, email, status, ad_clashing, "
            "agency_id, created_at) "
            "VALUES (%s,%s,%s,%s,'ACTIVE',false,%s,%s) RETURNING id",
            (name, domain, contact, email, agency_ids[i % len(agency_ids)], now))
        adv_ids.append(cur.fetchone()["id"])

    # --- Orders ---
    order_ids = []
    for i, oname in enumerate(["Q1 Audio Push", "Podcast Sponsorship", "Summer Streaming"]):
        cur.execute(
            "INSERT INTO aw_orders (name, advertiser_id, start_date, end_date, "
            "objective_type, objective_value, objective_currency, objective_unlimited, "
            "comments, archived, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,'IMPRESSIONS',%s,'USD',false,%s,false,%s,%s) RETURNING id",
            (oname, adv_ids[i % len(adv_ids)],
             "2025-01-01T00:00:00Z", "2025-06-30T23:59:59Z",
             500000 * (i + 1), f"Mock order {i+1}", now, now))
        order_ids.append(cur.fetchone()["id"])

    # --- Campaigns ---
    campaign_types = ["STANDARD", "FILLER", "STANDARD", "SPONSORSHIP", "STANDARD", "INTERACTIVE"]
    statuses = ["RUNNING", "PAUSED", "READY", "COMPLETED", "DRAFT", "RUNNING"]
    pacing_types = ["EVENLY", "AS_FAST_AS_POSSIBLE", "EVENLY", "EVENLY",
                    "AS_FAST_AS_POSSIBLE", "EVENLY"]
    camp_ids = []
    for i in range(6):
        cur.execute(
            "INSERT INTO aw_campaigns "
            "(name, campaign_type, advertiser_id, order_id, status, billing, "
            "start_date, end_date, revenue_type, revenue_value, revenue_currency, "
            "objective_type, objective_value, objective_unlimited, "
            "pacing_type, pacing_priority, comments, external_reference, archived, created_at, "
            "updated_at) "
            "VALUES (%s,%s,%s,%s,%s,'SOLD',%s,%s,'CPM',%s,'USD','IMPRESSIONS',%s,false,%s,%s,%s,%s,false,%s,%s) "
            "RETURNING id",
            (f"Campaign {i+1} - Audio {'Spring' if i < 3 else 'Summer'}",
             campaign_types[i], adv_ids[i % len(adv_ids)],
             order_ids[i % len(order_ids)] if i < 3 else None,
             statuses[i],
             "2025-02-01T00:00:00Z", "2025-08-31T23:59:59Z",
             round(random.uniform(2.0, 12.0), 2),
             random.randint(50000, 500000),
             pacing_types[i], random.randint(1, 8),
             f"Mock campaign {i+1}", f"EXT-AW-{1000+i}", now, now))
        camp_ids.append(cur.fetchone()["id"])

    # --- Ads ---
    ad_types = ["AUDIO", "AUDIO", "DISPLAY", "AUDIO", "DISPLAY", "AUDIO"]
    for i, cid in enumerate(camp_ids):
        for j in range(random.randint(2, 4)):
            atype = ad_types[(i + j) % len(ad_types)]
            cur.execute(
                "INSERT INTO aw_ads "
                "(campaign_id, name, type, subtype, status, included_in_objective, "
                "weight, comments, creative_file_name, duration_ms, archived, created_at) "
                "VALUES (%s,%s,%s,%s,%s,true,%s,%s,%s,%s,false,%s)",
                (cid, f"Ad {j+1} - {atype.title()}", atype, atype,
                 random.choice(["ACTIVE", "INACTIVE"]),
                 random.randint(1, 10),
                 f"Mock ad for campaign {cid}",
                 f"creative_{cid}_{j+1}.{'mp3' if atype == 'AUDIO' else 'png'}",
                 random.choice([15000, 30000, 60000]) if atype == "AUDIO" else None,
                 now))

    # --- Publishers ---
    pub_ids = []
    for pname, website, email in [
        ("AudioStream FM", "https://audiostream.fm", "ops@audiostream.fm"),
        ("PodNet Radio", "https://podnetradio.com", "tech@podnetradio.com"),
        ("WaveLength Media", "https://wavelength.media", "admin@wavelength.media"),
    ]:
        cur.execute(
            "INSERT INTO aw_publishers (name, contact, website, email, description, timezone, created_at) "
            "VALUES (%s,'Publisher Ops',%s,%s,'Mock publisher',%s,%s) RETURNING id",
            (pname, website, email, "America/New_York", now))
        pub_ids.append(cur.fetchone()["id"])

    # --- Zones ---
    zone_ids = []
    zone_types = ["AUDIO", "AUDIO", "DISPLAY", "VIDEO", "AUDIO"]
    formats = ["Pre-roll", "Mid-roll", "Companion", "Pre-roll", "Post-roll"]
    for i, pid in enumerate(pub_ids):
        for j in range(3):
            idx = (i * 3 + j) % len(zone_types)
            ztype = zone_types[idx]
            cur.execute(
                "INSERT INTO aw_zones "
                "(publisher_id, name, description, type, format_id, "
                "width, height, duration_min, duration_max, comments, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (pid, f"Zone {j+1} - {ztype.title()}", f"Mock {ztype.lower()} zone",
                 ztype, formats[idx],
                 300 if ztype == "DISPLAY" else None,
                 250 if ztype == "DISPLAY" else None,
                 15.0 if ztype == "AUDIO" else None,
                 60.0 if ztype == "AUDIO" else None,
                 f"Zone for publisher {pid}", now))
            zone_ids.append(cur.fetchone()["id"])

    # --- Zone Groups ---
    zg_ids = []
    for zgname in ["Rock Stations", "News & Talk", "Podcast Network", "Premium Audio"]:
        cur.execute(
            "INSERT INTO aw_zone_groups "
            "(name, description, total_capping, session_capping, comments, archived, created_at) "
            "VALUES (%s,%s,%s,%s,%s,false,%s) RETURNING id",
            (zgname, f"Mock zone group: {zgname}",
             random.randint(500, 2000), random.randint(5, 20),
             f"Auto-generated group", now))
        zg_ids.append(cur.fetchone()["id"])

    # Link zones to zone groups
    for i, zgid in enumerate(zg_ids):
        for zid in zone_ids[i::len(zg_ids)]:
            cur.execute(
                "INSERT INTO aw_zone_group_zones (zone_group_id, zone_id) "
                "VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (zgid, zid))

    # Targeting for the first ad of the first campaign, so GET targeting-rules
    # has something to show out of the box.
    cur.execute("SELECT id FROM aw_ads WHERE campaign_id = %s ORDER BY id LIMIT 1",
                (camp_ids[0],))
    first_ad = cur.fetchone()
    if first_ad:
        cur.execute(
            "INSERT INTO aw_ad_targeting "
            "(ad_id, zone_ids, zone_group_ids, delivery_instructions, rules, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (first_ad["id"], "[]", f"[{zg_ids[0]}]", '{"adBreakPositions": []}', "[]", now))

    # --- Categories (IAB-style) ---
    parent_cats = [
        ("Arts & Entertainment", "IAB1"),
        ("Automotive", "IAB2"),
        ("Business", "IAB3"),
        ("Technology & Computing", "IAB19"),
        ("Sports", "IAB17"),
    ]
    for pname, desc in parent_cats:
        cur.execute(
            "INSERT INTO aw_categories (name, description, parent_id) VALUES (%s,%s,NULL) RETURNING id",
            (pname, desc))
        pid = cur.fetchone()["id"]
        # Add 2 subcategories per parent
        for sub in [f"{pname} - Sub A", f"{pname} - Sub B"]:
            cur.execute(
                "INSERT INTO aw_categories (name, description, parent_id) VALUES (%s,%s,%s)",
                (sub, f"Subcategory of {pname}", pid))

    # --- v9: Audiogram creatives ---
    for i, (cname, status) in enumerate([("Spring Audiogram", "PUBLISHED"),
                                          ("Summer Audiogram", "PROCESSING")]):
        cur.execute(
            "INSERT INTO aw_audiogram_creatives "
            "(advertiser_id, creative_name, audio_creative_identifier, audio_file_extension, "
            "display_creative_identifier, display_file_extension, status, created_at) "
            "VALUES (%s,%s,%s,'mp3',%s,'png',%s,%s)",
            (adv_ids[i % len(adv_ids)], cname,
             f"_ad_{_uuid()}", f"_ad_{_uuid()}", status, now))


def _seed_thetradedesk(cur, now):
    """Seed The Trade Desk v3 mock data."""
    cur.execute("SELECT COUNT(*) FROM ttd_advertisers")
    if cur.fetchone()["count"] > 0:
        return

    partner_ids = ["ttd-p-001", "ttd-p-002"]

    # --- Advertisers ---
    adv_data = [
        ("ttd-adv-001", partner_ids[0], "Acme Programmatic", "Acme programmatic buying", "acme.com"),
        ("ttd-adv-002", partner_ids[0], "TechNova DSP", "TechNova display campaigns", "technova.com"),
        ("ttd-adv-003", partner_ids[1], "GreenLeaf Digital", "GreenLeaf CTV and audio", "greenleaf.com"),
        ("ttd-adv-004", partner_ids[1], "Sunset Travel Ads", "Sunset travel promotions", "sunsettravel.com"),
    ]
    adv_ids = []
    for adv_id, pid, name, desc, domain in adv_data:
        adv_ids.append(adv_id)
        cur.execute(
            "INSERT INTO ttd_advertisers VALUES (%s,%s,%s,%s,'USD',%s,NULL,NULL,'Available',%s)",
            (adv_id, pid, name, desc, domain, now))

    # --- Campaigns ---
    goal_types = ["CPC", "CPM", "CPA", "ROAS", "MaximizeReach"]
    pacing_modes = ["PaceEvenly", "PaceAhead", "AsSoonAsPossible"]
    camp_ids = []
    camp_counter = 1
    for adv_id in adv_ids:
        for j in range(3):
            cid = f"ttd-c-{camp_counter:04d}"
            camp_counter += 1
            camp_ids.append((cid, adv_id))
            cur.execute(
                "INSERT INTO ttd_campaigns VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (cid, adv_id,
                 f"TTD Campaign {j+1} - {'Brand' if j == 0 else 'Performance' if j == 1 else 'Retargeting'}",
                 round(random.uniform(10000, 500000), 2),
                 round(random.uniform(500, 10000), 2),
                 _past_date(60 - j * 15), _future_date(30 + j * 15),
                 random.choice(goal_types),
                 round(random.uniform(0.5, 25.0), 2),
                 random.choice(pacing_modes),
                 random.choice([3, 5, 10, None]),
                 random.choice(["Day", "Week", "Lifetime", None]),
                 now))

    # --- Campaign Flights ---
    flight_counter = 1
    for cid, adv_id in camp_ids:
        for k in range(2):
            fid = f"ttd-fl-{flight_counter:04d}"
            flight_counter += 1
            budget_imp = random.randint(100000, 5000000)
            budget_currency = round(random.uniform(5000, 100000), 2)
            cur.execute(
                "INSERT INTO ttd_campaign_flights VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (fid, cid,
                 f"Flight {k+1} - {'Always On' if k == 0 else 'Burst'}",
                 _past_date(45 - k * 15), _future_date(30 + k * 15),
                 budget_imp, budget_imp // 30,
                 budget_currency, round(budget_currency / 30, 2),
                 now))

    # --- Ad Groups ---
    ad_formats = ["Display", "Video", "Audio", "NativeDisplay", "ConnectedTV"]
    bid_types = ["CPM", "VCPM", "CPV", "CPC"]
    ag_counter = 1
    for cid, adv_id in camp_ids:
        for j in range(random.randint(2, 4)):
            agid = f"ttd-ag-{ag_counter:04d}"
            ag_counter += 1
            cur.execute(
                "INSERT INTO ttd_ad_groups VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (agid, cid,
                 f"Ad Group {j+1} - {random.choice(['Prospecting', 'Retargeting', 'Lookalike', 'Contextual'])}",
                 round(random.uniform(1.0, 20.0), 2),
                 random.choice(bid_types),
                 True,
                 random.choice(ad_formats),
                 _past_date(30), _future_date(30),
                 random.choice([3, 5, 10, None]),
                 now))

    # --- Creatives ---
    creative_types = ["Banner", "Video", "NativeDisplay", "Audio", "HTML5"]
    sizes = [(300, 250), (728, 90), (160, 600), (320, 50), (1920, 1080), (640, 480)]
    cr_counter = 1
    for adv_id in adv_ids:
        for j in range(5):
            crid = f"ttd-cr-{cr_counter:04d}"
            cr_counter += 1
            ctype = random.choice(creative_types)
            w, h = random.choice(sizes)
            cur.execute(
                "INSERT INTO ttd_creatives VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (crid, adv_id,
                 f"Creative {j+1} - {ctype}",
                 ctype, w, h,
                 f"https://click.example.com/{crid}",
                 f"https://landing.example.com/{adv_id}",
                 None,
                 random.choice(["Active", "Pending", "Rejected"]),
                 now))

    # --- Tracking Tags ---
    tag_types = ["Script", "Image", "Iframe"]
    tt_counter = 1
    for adv_id in adv_ids:
        for j in range(3):
            ttid = f"ttd-tt-{tt_counter:04d}"
            tt_counter += 1
            cur.execute(
                "INSERT INTO ttd_tracking_tags VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (ttid, adv_id,
                 f"Tracking Tag {j+1} - {'Conversion' if j == 0 else 'Retargeting' if j == 1 else 'Attribution'}",
                 random.choice(tag_types),
                 random.randint(0, 50000),
                 random.choice([True, False]),
                 now))


def _seed_gam(cur, now):
    """Seed Google Ad Manager (GAM) mock data."""
    cur.execute("SELECT COUNT(*) FROM gam_networks")
    if cur.fetchone()["count"] > 0:
        return

    # --- Networks ---
    networks = [
        ("12345678", "Acme Publisher Network", 12345678, "ACME-PUB", "America/New_York", "USD"),
        ("87654321", "TechNova Media Network", 87654321, "TN-MEDIA", "America/Los_Angeles", "USD"),
    ]
    for net_code, name, net_id, prop_code, tz, currency in networks:
        cur.execute(
            "INSERT INTO gam_networks VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (net_code, name, net_id, prop_code, tz, currency, 1, now))

    # --- Companies ---
    company_counter = 5000001
    company_data = [
        ("Acme Widgets Advertising", "ADVERTISER", "acme-adv-001", "ads@acme.com"),
        ("TechNova Performance", "ADVERTISER", "tn-adv-001", "perf@technova.com"),
        ("GreenLeaf Direct", "ADVERTISER", "gl-adv-001", "media@greenleaf.com"),
        ("Sunset Travel Marketing", "ADVERTISER", "st-adv-001", "ads@sunsettravel.com"),
        ("Horizon Agency Group", "AGENCY", "horizon-001", "bookings@horizon.com"),
        ("MediaCorp Partners", "AGENCY", "mcorp-001", "ops@mediacorp.com"),
    ]
    company_ids = []
    for display_name, comp_type, ext_id, email in company_data:
        cid = company_counter
        company_counter += 1
        company_ids.append(cid)
        net_code = "12345678" if cid % 2 == 1 else "87654321"
        cur.execute(
            "INSERT INTO gam_companies VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (cid, net_code, display_name, comp_type, ext_id,
             "123 Ad Street, New York, NY 10001", email,
             f"Synthetic {comp_type.lower()}", "ACTIVE", now))

    # --- Orders ---
    order_counter = 6000001
    order_statuses = ["APPROVED", "DRAFT", "APPROVED", "PENDING_APPROVAL", "APPROVED"]
    advertiser_ids = [c for c in company_ids[:4]]  # first 4 are advertisers
    order_ids = []
    for i in range(10):
        oid = order_counter
        order_counter += 1
        order_ids.append(oid)
        adv_id = advertiser_ids[i % len(advertiser_ids)]
        # Determine network_code from advertiser
        net_code = "12345678" if adv_id % 2 == 1 else "87654321"
        cur.execute(
            "INSERT INTO gam_orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (oid, net_code,
             f"Order {i+1} - {'Direct' if i % 2 == 0 else 'Programmatic'} Deal",
             adv_id,
             "trafficker@publisher.com",
             "sales@publisher.com",
             order_statuses[i % len(order_statuses)],
             _past_date(60 - i * 5) + "T00:00:00Z",
             _future_date(30 + i * 10) + "T23:59:59Z",
             False, "USD",
             90000 + i, f"PO-{2000+i}",
             f"Mock order for testing GAM integration",
             False, now))

    # --- Line Items ---
    li_counter = 7000001
    li_types = ["STANDARD", "SPONSORSHIP", "NETWORK", "HOUSE", "PRICE_PRIORITY"]
    cost_types = ["CPM", "CPC", "CPD", "CPA"]
    goal_types = ["LIFETIME", "DAILY", "LIFETIME", "NONE"]
    for oid in order_ids:
        # Get network_code for this order
        row = cur.execute("SELECT network_code FROM gam_orders WHERE order_id = %s", (oid,))
        net_code_row = cur.fetchone()
        net_code = net_code_row["network_code"]
        for j in range(random.randint(2, 5)):
            lid = li_counter
            li_counter += 1
            li_type = li_types[j % len(li_types)]
            cost_type = cost_types[j % len(cost_types)]
            goal_type = goal_types[j % len(goal_types)]
            impressions_goal = random.randint(100000, 5000000)
            delivered = random.randint(0, impressions_goal)
            clicks = random.randint(0, int(delivered * 0.03))
            cur.execute(
                "INSERT INTO gam_line_items VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (lid, net_code, oid,
                 f"Line Item {j+1} - {li_type.replace('_', ' ').title()}",
                 li_type,
                 random.choice([4, 6, 8, 10, 12, 16]),
                 random.choice(["DELIVERING", "READY", "PAUSED", "COMPLETED"]),
                 _past_date(45 - j * 5) + "T00:00:00Z",
                 _future_date(20 + j * 10) + "T23:59:59Z",
                 False,
                 cost_type,
                 random.randint(500000, 15000000),
                 "USD", goal_type,
                 impressions_goal,
                 delivered, clicks,
                 random.choice(["EVEN", "WEIGHTED", "SEQUENTIAL"]),
                 round(random.uniform(0, 15), 2),
                 now))

    # --- Ad Units ---
    au_counter = 8000001
    ad_unit_names = [
        ("Homepage Leaderboard", "728x90", "HOMEPAGE_LB"),
        ("Homepage Medium Rectangle", "300x250", "HOMEPAGE_MR"),
        ("Article Top Banner", "970x250", "ART_TOP"),
        ("Article Sidebar", "300x600", "ART_SIDE"),
        ("Video Pre-roll", "640x480", "VID_PRE"),
        ("Mobile Interstitial", "320x480", "MOB_INT"),
        ("Footer Banner", "728x90", "FOOTER_BN"),
        ("Native In-Feed", "1x1", "NATIVE_IF"),
    ]
    au_ids = []
    for name, size, code in ad_unit_names:
        auid = au_counter
        au_counter += 1
        au_ids.append(auid)
        net_code = "12345678" if auid % 2 == 1 else "87654321"
        cur.execute(
            "INSERT INTO gam_ad_units VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (auid, net_code, name, code, None,
             "ACTIVE", size, "BLANK", random.choice([True, False]), now))

    # --- Placements ---
    pl_counter = 9000001
    placement_data = [
        ("Run of Site", "All ad units across the site"),
        ("Homepage Package", "Homepage leaderboard and medium rectangle"),
        ("Article Pages", "All article page ad units"),
        ("Video Package", "Pre-roll and mid-roll video units"),
        ("Mobile Only", "Mobile interstitial and mobile banners"),
    ]
    for name, desc in placement_data:
        pid = pl_counter
        pl_counter += 1
        net_code = "12345678" if pid % 2 == 1 else "87654321"
        targeted = ",".join(str(au_ids[i]) for i in random.sample(range(len(au_ids)), k=min(3, len(au_ids))))
        cur.execute(
            "INSERT INTO gam_placements VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (pid, net_code, name, desc, "ACTIVE", targeted, now))

    # --- Creatives ---
    cr_counter = 9500001
    creative_types = ["THIRD_PARTY", "IMAGE", "VIDEO_VAST", "CUSTOM", "NATIVE"]
    sizes = [(728, 90), (300, 250), (300, 600), (320, 50), (640, 480), (970, 250)]
    for adv_id in advertiser_ids:
        net_code = "12345678" if adv_id % 2 == 1 else "87654321"
        for j in range(4):
            crid = cr_counter
            cr_counter += 1
            ctype = creative_types[j % len(creative_types)]
            w, h = sizes[(j + adv_id) % len(sizes)]
            cur.execute(
                "INSERT INTO gam_creatives VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (crid, net_code, adv_id,
                 f"Creative {j+1} - {ctype.replace('_', ' ').title()}",
                 ctype, w, h,
                 f"https://preview.mock.local/creative/{crid}",
                 random.choice(["ACTIVE", "INACTIVE"]),
                 now))

    # --- Reports ---
    rpt_counter = 9900001
    report_types = ["HISTORICAL", "REACH", "AD_SPEED", "REAL_TIME"]
    for i in range(6):
        rid = rpt_counter
        rpt_counter += 1
        net_code = "12345678" if rid % 2 == 1 else "87654321"
        cur.execute(
            "INSERT INTO gam_reports VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (rid, net_code,
             f"Report {i+1} - {report_types[i % len(report_types)]}",
             report_types[i % len(report_types)],
             "COMPLETED",
             _past_date(30), _past_date(1), now))


def _seed_basisuil(cur, now):
    """Seed Basis DSP (BasisUIL) mock data."""
    cur.execute("SELECT COUNT(*) FROM bn_advertisers")
    if cur.fetchone()["count"] > 0:
        return

    # --- Advertisers ---
    advertisers = [
        (156329, "Acme Advertising Co.", "USD", "admin@acme-ads.com", "", "active", True, 15045.52, 1000000.0, 2.0, 3000),
        (156330, "TechNova Media Group", "USD", "media@technova.com", "Performance advertiser", "active", True, 8200.00, 500000.0, 5.0, 2000),
        (156331, "GreenLeaf Digital", "USD", "digital@greenleaf.com", "", "active", True, 22500.75, 1000000.0, 2.0, 3000),
    ]
    for adv_id, name, currency, email, notes, status, active, balance, max_budget, min_budget, camp_limit in advertisers:
        cur.execute(
            "INSERT INTO bn_advertisers (advertiser_id, company_name, currency_code, email, notes, status, active, balance, max_budget_amount, min_campaign_budget_amount, active_campaign_limit, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (adv_id, name, currency, email, notes, status, active, balance, max_budget, min_budget, camp_limit, now))

    # --- Brands ---
    brands = [
        (2, 156329, "SuperCool Scooters", "The fastest, coolest scooters!", "supercoolscooters.com", False),
        (3, 156329, "ACME Corp", "Foolproof solutions", "acme.com", False),
        (4, 156329, "TechNova Cloud", "Cloud platform advertising", "cloud.technova.com", False),
        (5, 156329, "TechNova Mobile", "Mobile app campaigns", "mobile.technova.com", False),
        (6, 156329, "GreenLeaf Organics", "Organic products brand", "organics.greenleaf.com", False),
        (7, 156329, "EcoTravel", "Sustainable travel brand", "ecotravel.greenleaf.com", False),
    ]
    for brand_id, adv_id, name, notes, domain, archived in brands:
        cur.execute(
            "INSERT INTO bn_brands (brand_id, advertiser_id, name, notes, landing_page_domain, archived, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (brand_id, adv_id, name, notes, domain, archived, now))

    # --- Campaign Groups ---
    groups = [
        (30535, 156329, 2, "Summer 2025", "online", "CTR", "2.25", 45000.0, "all_time", True, _past_date(90), _future_date(30), "CAMPAIGN_GROUP", "margin", 0),
        (30536, 156329, 2, "Holiday Push Q4", "online", "CPA", "4.50", 225.0, "daily", True, _past_date(30), _future_date(60), "CAMPAIGN", None, 0),
        (30537, 156329, 3, "ACME Brand Awareness", "offline", None, None, 10000.0, "all_time", False, _past_date(60), _future_date(15), "CAMPAIGN_GROUP", "markup", 0.30),
        (30538, 156329, 4, "Cloud Launch Campaign", "online", "CTR", "1.5", 500.0, "daily", True, _past_date(14), _future_date(45), "CAMPAIGN", None, 0),
        (30539, 156329, 5, "Mobile App Install", "online", "CPA", "8.00", 30000.0, "all_time", True, _past_date(7), _future_date(90), "CAMPAIGN_GROUP", "margin", 0.15),
        (30540, 156329, 6, "Organics Spring Sale", "online", None, None, 150.0, "daily", True, _past_date(5), _future_date(25), "CAMPAIGN", None, 0),
        (30541, 156329, 7, "EcoTravel Summer", "offline", "CPClick", "1.20", 20000.0, "all_time", False, _past_date(45), _future_date(10), "CAMPAIGN_GROUP", "flat_cpm", 1.25),
    ]
    for gid, adv_id, brand_id, name, status, kpi_type, kpi_value, budget, btype, even, fstart, fend, pacing, spend_type, spend_rate in groups:
        cur.execute(
            "INSERT INTO bn_campaign_groups (campaign_group_id, advertiser_id, brand_id, name, status, kpi_type, kpi_value, budget_amount, budget_type, even_delivery_enabled, flight_start, flight_end, pacing_setting, advertiser_spend_type, advertiser_spend_rate, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (gid, adv_id, brand_id, name, status, kpi_type, kpi_value, budget, btype, even, fstart, fend, pacing, spend_type, spend_rate, now))

    # --- Campaigns ---
    campaigns = [
        (3017848, 156329, 30535, "Summer 2025", "Scooter Display - West Coast", "online", 1.7, 5.0, "", 25.0, "daily", True, 10000, "all_time", _past_date(85), _future_date(25), "advanced", True, False, "eligible"),
        (3017849, 156329, 30535, "Summer 2025", "Scooter Video - Preroll", "online", 4.0, 10.0, "", 100.0, "daily", True, None, "none", _past_date(85), _future_date(25), "advanced", True, False, "eligible"),
        (3017850, 156329, 30536, "Holiday Push Q4", "Holiday Retargeting", "offline", 2.5, 8.0, "Retargeting campaign", 50.0, "daily", True, 5000, "daily", _past_date(25), _future_date(55), "advanced", False, True, "eligible"),
        (3017851, 156329, 30537, "ACME Brand Awareness", "ACME CTV Awareness", "offline", 8.0, 15.0, "", 500.0, "all_time", False, None, "none", _past_date(55), _future_date(10), "advanced", True, False, "eligible"),
        (3017852, 156329, 30538, "Cloud Launch Campaign", "Cloud Platform - Search", "online", 3.0, 7.0, "", 200.0, "daily", True, None, "none", _past_date(10), _future_date(40), "advanced", True, False, "eligible"),
        (3017853, 156329, 30538, "Cloud Launch Campaign", "Cloud Platform - Display", "online", 1.5, 4.0, "", 150.0, "daily", True, 20000, "all_time", _past_date(10), _future_date(40), "advanced", True, False, "eligible"),
        (3017854, 156329, 30539, "Mobile App Install", "Mobile Install - Android", "online", 5.0, 12.0, "Android only", 300.0, "all_time", True, None, "none", _past_date(5), _future_date(85), "advanced", False, True, "eligible"),
        (3017855, 156329, 30539, "Mobile App Install", "Mobile Install - iOS", "online", 6.0, 14.0, "iOS only", 350.0, "all_time", True, None, "none", _past_date(5), _future_date(85), "advanced", False, True, "eligible"),
        (3017856, 156329, 30540, "Organics Spring Sale", "Organics - Native Ads", "online", 2.0, 6.0, "", 100.0, "daily", True, 8000, "daily", _past_date(3), _future_date(22), "advanced", True, False, "eligible"),
        (3017857, 156329, 30541, "EcoTravel Summer", "EcoTravel DOOH", "offline", 10.0, 25.0, "DOOH pilot", 1000.0, "all_time", False, None, "none", _past_date(40), _future_date(5), "dooh", True, False, "eligible"),
    ]
    for cid, adv_id, gid, gname, name, status, dbid, maxbid, notes, budget, btype, even, impcap, impcaptype, fstart, fend, ctype, rop, xdev, review in campaigns:
        created = _past_date(random.randint(1, 90)) + " " + f"{random.randint(8,17):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        cur.execute(
            "INSERT INTO bn_campaigns (campaign_id, advertiser_id, campaign_group_id, campaign_group_name, name, status, default_bid, max_bid, notes, budget_amount, budget_type, even_delivery_enabled, impression_cap, impression_cap_type, flight_start, flight_end, campaign_type, enabled_rop, enable_cross_device, review_status, created, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (cid, adv_id, gid, gname, name, status, dbid, maxbid, notes, budget, btype, even, impcap, impcaptype, fstart, fend, ctype, rop, xdev, review, created, now))

    # --- Creatives ---
    creative_counter = 4001
    creative_data = [
        (156329, 2, "Scooter Banner 300x250", "display", 300, 250, "active", "https://supercoolscooters.com/summer"),
        (156329, 2, "Scooter Banner 728x90", "display", 728, 90, "active", "https://supercoolscooters.com/summer"),
        (156329, 2, "Scooter Video 15s", "video", 1920, 1080, "active", "https://supercoolscooters.com/video"),
        (156329, 3, "ACME CTV Spot 30s", "video", 1920, 1080, "active", "https://acme.com/brand"),
        (156329, 4, "Cloud Banner 320x50", "display", 320, 50, "active", "https://cloud.technova.com/signup"),
        (156329, 4, "Cloud Native Ad", "native", None, None, "active", "https://cloud.technova.com/features"),
        (156329, 5, "Mobile Install Ad", "display", 320, 480, "active", "https://mobile.technova.com/install"),
        (156329, 6, "Organics Native Card", "native", None, None, "active", "https://organics.greenleaf.com/sale"),
        (156329, 7, "EcoTravel DOOH 1920x1080", "video", 1920, 1080, "active", "https://ecotravel.greenleaf.com"),
    ]
    for adv_id, brand_id, name, ctype, w, h, status, url in creative_data:
        cur.execute(
            "INSERT INTO bn_creatives (creative_id, advertiser_id, brand_id, name, creative_type, width, height, status, landing_page_url, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (creative_counter, adv_id, brand_id, name, ctype, w, h, status, url, now))
        creative_counter += 1

    # --- Campaign Stats (sample daily stats for the past 7 days) ---
    campaign_ids = [c[0] for c in campaigns]
    for cid in campaign_ids:
        adv_id = next(c[1] for c in campaigns if c[0] == cid)
        for days_ago in range(7):
            imps = random.randint(5000, 50000)
            clicks = random.randint(50, 500)
            spend = round(random.uniform(10.0, 200.0), 2)
            convs = random.randint(0, 20)
            ctr = round(clicks / imps, 6) if imps else 0
            cpm = round((spend / imps) * 1000, 4) if imps else 0
            cpc = round(spend / clicks, 4) if clicks else 0
            cur.execute(
                "INSERT INTO bn_campaign_stats (advertiser_id, campaign_id, date, impressions, clicks, spend, conversions, ctr, cpm, cpc) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (adv_id, cid, _past_date(days_ago), imps, clicks, spend, convs, ctr, cpm, cpc))

    # --- Pre-existing Reports (completed) ---
    report_seeds = [
        (156329, "Spring campaigns daily", "BASIC", "DAILY",
         _past_date(7), _past_date(0), "[3017848,3017849]", "CAMPAIGN", "EST", "DONE"),
        (156329, "Holiday retargeting weekly", "BASIC", "WEEKLY",
         _past_date(30), _past_date(0), "[3017850]", "CAMPAIGN", "EST", "DONE"),
        (156329, "Cloud launch monthly", "BASIC", "MONTHLY",
         _past_date(30), _past_date(0), "[3017852,3017853]", "CAMPAIGN", "EST", "DONE"),
        (156329, "Mobile install daily", "AD_WITH_HIERARCHY", "DAILY",
         _past_date(14), _past_date(0), "[3017854,3017855]", "CAMPAIGN", "EST", "DONE"),
        (156329, "Organics brand report", "BASIC", "DAILY",
         _past_date(7), _past_date(0), "[3017856]", "CAMPAIGN", "EST", "QUEUED"),
    ]
    for adv_id, desc, rtype, agg, fdate, tdate, eids, etype, tz, status in report_seeds:
        start_t = now.replace("T", " ").replace("Z", "")[:19] if status == "DONE" else None
        comp_t = start_t
        exp_t = (_future_date(15) + " 12:00:00") if status == "DONE" else None
        url = f"https://cdn01.basis.net/reports/{adv_id}/{fdate}/{desc.replace(' ', '_')}.csv" if status == "DONE" else None
        cur.execute(
            "INSERT INTO bn_reports (advertiser_id, description, report_type, aggregation, "
            "from_date, to_date, entity_ids, entity_type, timezone, status, "
            "start_time, completion_time, expiry_time, url, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (adv_id, desc, rtype, agg, fdate, tdate, eids, etype, tz, status,
             start_t, comp_t, exp_t, url, now))

    # --- Report Schedules ---
    schedule_seeds = [
        (156329, "Acme Weekly Performance", "BASIC", "WEEKLY", "[3017848,3017849,3017850]",
         "CAMPAIGN", _past_date(60), _future_date(30), "01:00", "WEEKLY", 7, "DAY",
         '["admin@acme-ads.com"]', "ACTIVE"),
        (156329, "TechNova Daily Check", "BASIC", "DAILY", "[3017852,3017853,3017854,3017855]",
         "CAMPAIGN", _past_date(14), _future_date(60), "08:00", "DAILY", 1, "DAY",
         '["media@technova.com"]', "ACTIVE"),
        (156329, "GreenLeaf Monthly", "BASIC", "MONTHLY", "[3017856,3017857]",
         "CAMPAIGN", _past_date(30), _future_date(90), "02:00", "MONTHLY", 30, "DAY",
         '["digital@greenleaf.com"]', "PAUSED"),
    ]
    for adv_id, name, rtype, agg, eids, etype, sstart, send, trun, freq, lb, lbt, emails, status in schedule_seeds:
        cur.execute(
            "INSERT INTO bn_report_schedules (advertiser_id, name, report_type, aggregation, "
            "entity_ids, entity_type, schedule_start_date, schedule_end_date, time_to_run_report, "
            "frequency, lookback_window, lookback_type, emails, status, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (adv_id, name, rtype, agg, eids, etype, sstart, send, trun, freq, lb, lbt, emails, status, now))


def _seed_radioworkflow(cur, now):
    """Seed Radio Workflow Partner API mock data."""
    cur.execute("SELECT COUNT(*) FROM rw_stations")
    if cur.fetchone()["count"] > 0:
        return

    # --- Stations ---
    stations = [
        (1001, "97.1 KVVL Real Alternative Radio", "KVVL", "FM", None, None, None, None, None, 51, 0),
        (1002, "101.5 WKRP Classic Hits", "WKRP", "FM", None, None, None, None, None, 51, 0),
        (1003, "AM 680 KNBR Sports", "KNBR", "AM", None, None, None, None, None, 36, 0),
        (1004, "92.3 WFAN Sports Radio", "WFAN", "FM", None, None, None, None, None, 36, 0),
        (1005, "Digital Stream One", None, "", None, None, None, None, None, 0, 0),
        (1006, "104.7 KISS Country", "KISS", "FM", None, None, None, None, None, 51, 0),
    ]
    for sid, desc, call, band, purl, iurl, lpath, mpath, aid, asys, asysimp in stations:
        cur.execute(
            "INSERT INTO rw_stations VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (sid, desc, call, band, purl, iurl, lpath, mpath, aid, asys, asysimp))

    # --- Accounts ---
    accounts = [
        (2001, "Acme Auto Group", 0, "AA", "John Smith", "john@acmeauto.com", "555-0201", "100 Main St", "Springfield", "IL", "62701", "US"),
        (2002, "Metro Health Systems", 0, "MH", "Linda Garcia", "linda@metrohealth.com", "555-0202", "200 Hospital Dr", "Chicago", "IL", "60601", "US"),
        (2003, "Sunrise Furniture", 0, "SF", "Tom Brown", "tom@sunrisefurn.com", "555-0203", "50 Oak Ave", "Dallas", "TX", "75201", "US"),
        (2004, "First National Bank", 0, "FN", "Sarah Davis", "sarah@firstnatl.com", "555-0204", "1 Bank Plaza", "New York", "NY", "10001", "US"),
        (2005, "City Pizza Co", 0, "CP", "Mike Wilson", "mike@citypizza.com", "555-0205", "75 Elm St", "Boston", "MA", "02101", "US"),
        (2006, "TechStart Solutions", 0, "TS", "Amy Lee", "amy@techstart.com", "555-0206", "300 Innovation Way", "San Jose", "CA", "95101", "US"),
        (2007, "Regional Airlines", 0, "RA", "Dan Johnson", "dan@regionalair.com", "555-0207", "Airport Blvd", "Atlanta", "GA", "30301", "US"),
        (2008, "Green Valley Farms", 0, "GV", "Pat Miller", "pat@greenvalley.com", "555-0208", "Rural Route 5", "Des Moines", "IA", "50301", "US"),
    ]
    for row in accounts:
        cur.execute(
            "INSERT INTO rw_accounts VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            row)

    # --- Talent ---
    talent = [
        (3001, "Chris Matthews", "chris@studio.com", "555-3001", "producer", 0),
        (3002, "Jessica Torres", "jessica@studio.com", "555-3002", "voice_talent", 0),
        (3003, "Ryan Blake", "ryan@studio.com", "555-3003", "producer", 0),
        (3004, "Samantha Reed", "samantha@studio.com", "555-3004", "voice_talent", 0),
        (3005, "Marcus Johnson", "marcus@studio.com", "555-3005", "engineer", 0),
    ]
    for row in talent:
        cur.execute("INSERT INTO rw_talent VALUES (%s,%s,%s,%s,%s,%s)", row)

    # --- Production Types ---
    prod_types = [
        (4001, "New Spot", 0, 1, 0, 0, 0, 0),
        (4002, "Extend Spot", 0, 1, 0, 0, 0, 0),
        (4003, "Show Credit", 0, 1, 0, 0, 0, 0),
        (4004, "Production Only", 0, 0, 0, 0, 0, 0),
        (4005, "Live Liner", 0, 0, 1, 0, 1, 0),
        (4006, "Live Cross", 0, 0, 1, 0, 1, 0),
        (4007, "Sponsorship Tag", 0, 1, 0, 0, 0, 0),
        (4008, "Promo", 0, 1, 0, 1, 0, 0),
    ]
    for row in prod_types:
        cur.execute("INSERT INTO rw_prod_types VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", row)

    # --- Revenue Types ---
    revenue_types = [
        (5001, "Cash", 0, "C", 0, 0, 0, 15, 0),
        (5002, "Cash Package", 0, "CP", 0, 0, 0, 15, 0),
        (5003, "Finance Charges", 0, "F", 0, 0, 0, 0, 0),
        (5004, "Trade", 0, "T", 0, 0, 0, 15, 0),
        (5005, "Political", 0, "PL", 1, 0, 0, 0, 0),
        (5006, "National", 0, "N", 1, 0, 1, 15, 5),
        (5007, "Digital", 0, "D", 1, 0, 0, 12, 0),
        (5008, "NTR (Non-Traditional Revenue)", 0, "NTR", 1, 0, 0, 10, 0),
    ]
    for row in revenue_types:
        cur.execute("INSERT INTO rw_revenue_types VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)

    # --- Dayparts ---
    dayparts = [
        (6001, "AM DRIVE 6-10", "06:00", "10:00", 0),
        (6002, "MIDDAY 10-3", "10:00", "15:00", 0),
        (6003, "PM DRIVE 3-7", "15:00", "19:00", 0),
        (6004, "EVENING 7-12", "19:00", "00:00", 0),
        (6005, "OVERNIGHT 12-6", "00:00", "06:00", 0),
        (6006, "ROS (Run of Schedule)", "06:00", "00:00", 0),
    ]
    for row in dayparts:
        cur.execute("INSERT INTO rw_dayparts VALUES (%s,%s,%s,%s,%s)", row)

    # --- Avail Types ---
    avail_types = [
        (7001, "REGULAR COMMERCIAL", 0, "RG"),
        (7002, "NEWS", 0, "NW"),
        (7003, "SPORTS", 0, "SP"),
        (7004, "PROMO", 0, "PR"),
        (7005, "NATIONAL NEWS", 0, "NN"),
        (7006, "SPECIAL PROGRAM", 0, "XX"),
        (7007, "INTO STOP SET LINER", 0, "LN"),
    ]
    for row in avail_types:
        cur.execute("INSERT INTO rw_avail_types VALUES (%s,%s,%s,%s)", row)

    # --- Spot Types ---
    spot_types = [
        (8001, "30-Second Commercial", 0, "30"),
        (8002, "60-Second Commercial", 0, "60"),
        (8003, "15-Second Commercial", 0, "15"),
        (8004, "Live Read", 0, "LR"),
        (8005, "Sponsorship", 0, "SP"),
        (8006, "PSA", 0, "PSA"),
    ]
    for row in spot_types:
        cur.execute("INSERT INTO rw_spot_types VALUES (%s,%s,%s,%s)", row)

    # --- Spot Lengths ---
    for length in [10, 12, 15, 30, 60, 90]:
        cur.execute("INSERT INTO rw_spot_lengths VALUES (%s)", (length,))

    # --- Automation Systems (subset of the 50+ supported) ---
    automation_systems = [
        (1, "AudioVault"), (2, "BE Audiovault FleX"),
        (5, "BSI OpX"), (7, "DAVID Systems (DigaSystem)"),
        (10, "Dalet Plus"), (14, "Enco DAD"),
        (20, "Google (DoubleClick)"), (22, "Harris Automation (Imagine)"),
        (33, "Prophet NexGen"), (36, "RCS NexGen"),
        (39, "Rivendell"), (43, "SMARTS Skylla"),
        (45, "Techno Creations StationPlaylist"),
        (51, "WideOrbit Radio Automation"),
        (52, "WinAmp EXTM3U"), (53, "Zetta (RCS)"),
        (99, "JSON Export"),
    ]
    for row in automation_systems:
        cur.execute("INSERT INTO rw_automation_systems VALUES (%s,%s)", row)

    # --- Production Orders (primary focus) ---
    import random as _rng

    production_statuses = ["pending", "in_progress", "completed", "cancelled"]
    production_orders = []
    for i in range(20):
        prod_id = i + 1
        acct = accounts[i % len(accounts)]
        station = stations[i % len(stations)]
        ptype = prod_types[i % len(prod_types)]
        tal = talent[i % len(talent)]
        status = production_statuses[i % len(production_statuses)]
        length = _rng.choice([15, 30, 30, 30, 60])
        isci = f"ISCI{_rng.randint(10000,99999)}"
        cart = f"CART-{station[0]}-{_rng.randint(100,999)}"

        cur.execute(
            "INSERT INTO rw_production (account_id, station_id, prod_type_id, title, "
            "isci_code, cart_id, cut_number, length_seconds, start_date, end_date, "
            "status, priority, talent_id, notes, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                acct[0], station[0], ptype[0],
                f"{acct[1]} - {ptype[1]} ({length}s)",
                isci, cart, 1, length,
                _past_date(60 - i * 3), _future_date(30 + i * 5),
                status, i % 3, tal[0],
                f"Production order for {acct[1]}",
                now, now,
            ))
        production_orders.append(prod_id)

    # --- Instructions (for production orders) ---
    instruction_templates = [
        ("Copy: {account} summer sale starts now!", "copy"),
        ("SFX: Upbeat music bed underneath", "sfx"),
        ("VO: Mention website and phone number", "direction"),
        ("Tag: Visit {account} dot com today", "tag"),
        ("Live mention of current promotion", "live"),
        ("Include legal disclaimer for financing", "legal"),
    ]
    instr_counter = 0
    for prod_id in production_orders:
        acct_name = accounts[(prod_id - 1) % len(accounts)][1]
        num_lines = _rng.randint(2, 5)
        for line_num in range(1, num_lines + 1):
            tmpl = instruction_templates[instr_counter % len(instruction_templates)]
            text = tmpl[0].format(account=acct_name)
            cur.execute(
                "INSERT INTO rw_instructions (production_id, line_number, instruction_text, "
                "instruction_type, created_at) VALUES (%s,%s,%s,%s,%s)",
                (prod_id, line_num, text, tmpl[1], now))
            instr_counter += 1

    # --- Logs ---
    log_ids = []
    for station in stations[:4]:
        for days_ago in range(14):
            log_date = _past_date(days_ago)
            # Recent logs are reconciled, older are locked, very recent are drafts
            if days_ago < 2:
                status = 0  # draft
            elif days_ago < 7:
                status = 1  # locked, not reconciled
            else:
                status = 2  # locked and reconciled

            total_spots = _rng.randint(40, 120)
            total_revenue = round(_rng.uniform(2000, 15000), 2)
            exported = 1 if status >= 1 else 0

            cur.execute(
                "INSERT INTO rw_logs (station_id, log_date, status, total_spots, "
                "total_revenue, exported, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING core_id",
                (station[0], log_date, status, total_spots, total_revenue, exported, now, now))
            result = cur.fetchone()
            if result:
                log_ids.append((result["core_id"], station[0], log_date))

    # --- Days Logs (individual spot entries) ---
    hours = ["06:15", "06:45", "07:15", "07:45", "08:15", "08:45",
             "09:15", "10:00", "10:30", "11:00", "12:00", "12:30",
             "13:00", "14:00", "15:15", "15:45", "16:15", "17:00",
             "18:00", "19:30", "20:00", "21:00"]

    for log_id, station_id, log_date in log_ids[:20]:  # limit to keep seed manageable
        num_entries = _rng.randint(8, 16)
        for j in range(num_entries):
            sched_time = hours[j % len(hours)]
            actual_offset = _rng.randint(-2, 2)
            h, m = sched_time.split(":")
            actual_min = int(m) + actual_offset
            actual_time = f"{h}:{max(0, min(59, actual_min)):02d}"

            acct = accounts[j % len(accounts)]
            length = _rng.choice([15, 30, 30, 60])
            daypart_idx = 0 if int(h) < 10 else (1 if int(h) < 15 else (2 if int(h) < 19 else 3))
            daypart_id = dayparts[daypart_idx][0]
            avail_id = avail_types[j % len(avail_types)][0]
            spot_type_id = spot_types[j % len(spot_types)][0]
            revenue = round(_rng.uniform(25, 500), 2)
            aired = 1 if _rng.random() > 0.05 else 0  # 95% air rate
            reconciled = 1 if aired and _rng.random() > 0.3 else 0

            cur.execute(
                "INSERT INTO rw_days_logs (log_id, station_id, air_date, scheduled_time, "
                "actual_time, cart_id, isci_code, account_id, length_seconds, daypart_id, "
                "avail_type_id, spot_type_id, revenue, aired, reconciled) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    log_id, station_id, log_date, sched_time, actual_time,
                    f"CART-{station_id}-{_rng.randint(100,999)}",
                    f"ISCI{_rng.randint(10000,99999)}",
                    acct[0], length, daypart_id, avail_id, spot_type_id,
                    revenue, aired, reconciled,
                ))

    # --- Clocks ---
    clock_data = [
        (1001, "AM Drive Standard", "Standard AM drive clock", 1, 6, 10),
        (1001, "Midday Relaxed", "Lighter load midday", 1, 10, 15),
        (1001, "PM Drive Standard", "Standard PM drive clock", 1, 15, 19),
        (1002, "All Day Default", "Default clock for WKRP", None, 6, 22),
        (1003, "Sports Heavy", "Heavy spot load for sports", 1, 6, 19),
    ]
    clock_ids = []
    for station_id, name, desc, dow, hstart, hend in clock_data:
        cur.execute(
            "INSERT INTO rw_clocks (station_id, name, description, day_of_week, "
            "hour_start, hour_end, active) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING core_id",
            (station_id, name, desc, dow, hstart, hend, 1))
        result = cur.fetchone()
        if result:
            clock_ids.append(result["core_id"])

    # --- Clock Breaks ---
    for clock_id in clock_ids:
        num_breaks = _rng.randint(3, 6)
        for pos in range(1, num_breaks + 1):
            avail_id = avail_types[pos % len(avail_types)][0]
            max_units = _rng.choice([3, 4, 5, 6])
            max_secs = max_units * 30
            cur.execute(
                "INSERT INTO rw_clock_breaks (clock_id, position, avail_type_id, "
                "max_units, max_seconds) VALUES (%s,%s,%s,%s,%s)",
                (clock_id, pos, avail_id, max_units, max_secs))

    # --- Ad Bank ---
    for i in range(30):
        station = stations[i % len(stations)]
        acct = accounts[i % len(accounts)]
        length = _rng.choice([15, 30, 30, 60])
        is_network = 1 if i % 5 == 0 else 0
        created = _past_date(_rng.randint(1, 90))
        start = created
        end = _future_date(_rng.randint(30, 180))

        cur.execute(
            "INSERT INTO rw_ad_bank (station_id, account_id, cart_id, isci_code, title, "
            "length_seconds, start_date, end_date, created_date, status, is_network) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                station[0], acct[0],
                f"CART-{station[0]}-{_rng.randint(100,999)}",
                f"ISCI{_rng.randint(10000,99999)}",
                f"{acct[1]} - {'Network ' if is_network else ''}Spot {i+1}",
                length, start, end, created,
                _rng.choice(["active", "active", "active", "expired"]),
                is_network,
            ))

    # --- Network Dubs ---
    network_advertisers = [
        "McDonald's", "Geico", "Progressive Insurance", "Amazon",
        "Toyota National", "Ford Motor Co", "Coca-Cola", "AT&T",
    ]
    network_agencies = [
        "Omnicom", "WPP", "Publicis", "Interpublic", "Dentsu",
    ]
    for i in range(12):
        cur.execute(
            "INSERT INTO rw_network_dubs (cart_id, title, isci_code, length_seconds, "
            "first_seen_date, agency, advertiser, status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                f"NET-{_rng.randint(1000,9999)}",
                f"{network_advertisers[i % len(network_advertisers)]} - Q3 Spot {i+1}",
                f"NTWK{_rng.randint(10000,99999)}",
                _rng.choice([15, 30, 60]),
                _past_date(_rng.randint(1, 45)),
                network_agencies[i % len(network_agencies)],
                network_advertisers[i % len(network_advertisers)],
                "active",
            ))


def _seed_stackadapt(cur, now):
    """Seed StackAdapt GraphQL DSP mock data.

    Idempotent: guarded by a COUNT(*) on sa_advertisers so re-runs on every
    startup (per init_db's design) do not duplicate rows. Builds a small
    Advertiser → CampaignGroup → Campaign tree across StackAdapt's channel
    types, plus 30 days of per-campaign delivery stats to back the delivery
    report queries (campaignDelivery / campaignGroupDelivery / advertiserDelivery).
    """
    cur.execute("SELECT COUNT(*) AS count FROM sa_advertisers")
    if cur.fetchone()["count"] > 0:
        return

    import random as _random

    # --- Advertisers (sub-advertiser brands) ---
    advertisers = [
        ("sa-adv-001", "Northwind Retail", "Northwind omni-channel retail"),
        ("sa-adv-002", "Helios Travel", "Helios travel & hospitality"),
    ]
    for aid, name, desc in advertisers:
        cur.execute(
            "INSERT INTO sa_advertisers (id, name, description, is_archived) "
            "VALUES (%s,%s,%s,FALSE)", (aid, name, desc))

    # --- Campaign groups (line items) ---
    groups = [
        ("sa-g-001", "sa-adv-001", "Northwind Q4 Prospecting", "COST"),
        ("sa-g-002", "sa-adv-001", "Northwind Retargeting", "COST"),
        ("sa-g-003", "sa-adv-002", "Helios Summer Launch", "IMP"),
    ]
    for gid, aid, name, budget_type in groups:
        cur.execute(
            "INSERT INTO sa_campaign_groups "
            "(id, name, advertiser_id, status, budget_type, budget_rollover, "
            " is_archived, created_at) VALUES (%s,%s,%s,'ACTIVE',%s,FALSE,FALSE,%s)",
            (gid, name, aid, budget_type, now))

    # --- Campaigns (channel-typed) ---
    channels = ["NATIVE", "DISPLAY", "VIDEO", "CTV", "AUDIO", "DOOH"]
    goal_types = ["CONVERSION", "ENGAGEMENT"]
    statuses = ["ACTIVE", "PAUSED", "PENDING"]
    campaigns = []
    counter = 1
    for gid, aid, gname, _bt in groups:
        for j in range(3):
            cid = f"sa-c-{counter:04d}"
            counter += 1
            channel = channels[(counter + j) % len(channels)]
            campaigns.append((cid, gid, aid, channel))
            cur.execute(
                "INSERT INTO sa_campaigns "
                "(id, name, channel, status, is_draft, is_archived, goal_type, "
                " advertiser_id, campaign_group_id, timezone, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,FALSE,FALSE,%s,%s,%s,'America/New_York',%s,%s)",
                (cid, f"{gname} — {channel.title()} {j+1}", channel,
                 statuses[j % len(statuses)], goal_types[j % len(goal_types)],
                 aid, gid, now, now))

    # --- Delivery stats: 30 daily rows per campaign ---
    for cid, gid, aid, _channel in campaigns:
        for d in range(30):
            stat_date = _past_date(29 - d)
            impressions = _random.randint(5000, 80000)
            clicks = int(impressions * _random.uniform(0.001, 0.02))
            conversions = round(clicks * _random.uniform(0.01, 0.08), 2)
            cost = round(impressions / 1000 * _random.uniform(2.0, 9.0), 2)
            revenue = round(conversions * _random.uniform(20.0, 120.0), 2)
            atos = round(_random.uniform(15.0, 180.0), 1)
            cur.execute(
                "INSERT INTO sa_delivery_stats "
                "(campaign_id, campaign_group_id, advertiser_id, stat_date, "
                " impressions, clicks, conversions, cost, revenue, atos) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (cid, gid, aid, stat_date, impressions, clicks,
                 conversions, cost, revenue, atos))


def _seed_freewheel(cur, now):
    """Seed Freewheel (api.freewheel.tv) mock data.

    Idempotent: guarded by a COUNT(*) on fw_agencies so re-runs on every startup
    (per init_db's design) do not duplicate rows. Builds a coherent graph --
    agency -> advertiser -> brand -> campaign -> insertion order -> placement --
    plus ~30 days of per-placement delivery stats that back the ASYNC reporting
    job model, a handful of marketplace V4 entities, a couple of RFP forecasts,
    proposed IO/placement/ad under a campaign, and programmatic deals + buyers +
    global advertisers/brands.
    """
    cur.execute("SELECT COUNT(*) AS count FROM fw_agencies")
    if cur.fetchone()["count"] > 0:
        return

    import random as _random
    _random.seed(97)  # deterministic seed graph

    # --- Agencies ---
    agencies = [
        ("Meridian Media Agency", "AGY-EXT-001"),
        ("Vantage Advertising Group", "AGY-EXT-002"),
    ]
    agency_ids = []
    for name, ext in agencies:
        cur.execute(
            "INSERT INTO fw_agencies (name, external_id, status, parent_agency_id, created_at) "
            "VALUES (%s,%s,'active',%s,%s) RETURNING agency_id",
            (name, ext, None, now))
        agency_ids.append(cur.fetchone()["agency_id"])

    # --- Advertisers (under agency 0) ---
    advertiser_specs = [
        ("Aurora Foods", "ADV-EXT-001", agency_ids[0]),
        ("Beacon Financial", "ADV-EXT-002", agency_ids[0]),
        ("Caldera Travel", "ADV-EXT-003", agency_ids[1]),
    ]
    advertiser_ids = []
    for name, ext, agy in advertiser_specs:
        cur.execute(
            "INSERT INTO fw_advertisers (name, external_id, agency_id, status, created_at) "
            "VALUES (%s,%s,%s,'active',%s) RETURNING advertiser_id",
            (name, ext, agy, now))
        advertiser_ids.append(cur.fetchone()["advertiser_id"])

    # --- Agency/advertiser relationships ---
    for adv_id, (_n, _e, agy) in zip(advertiser_ids, advertiser_specs):
        cur.execute(
            "INSERT INTO fw_relationships (parent_type, parent_id, child_type, child_id, status) "
            "VALUES ('agency',%s,'advertiser',%s,'active')", (agy, adv_id))

    # --- Brands (under each advertiser) ---
    brand_ids = []
    for adv_id in advertiser_ids:
        for b in range(2):
            cur.execute(
                "INSERT INTO fw_brands (advertiser_id, name, external_id, status, created_at) "
                "VALUES (%s,%s,%s,'active',%s) RETURNING brand_id",
                (adv_id, f"Brand {adv_id}-{b+1}", f"BRD-{adv_id}-{b+1}", now))
            brand_ids.append((cur.fetchone()["brand_id"], adv_id))

    # --- Campaigns (one per advertiser) ---
    campaign_ids = []
    for i, adv_id in enumerate(advertiser_ids):
        agy = advertiser_specs[i][2]
        cur.execute(
            "INSERT INTO fw_campaigns (name, description, advertiser_id, agency_id, "
            "external_id, status, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,'active',%s,%s) RETURNING campaign_id",
            (f"Campaign {adv_id} FY26", f"Seeded campaign for advertiser {adv_id}",
             adv_id, agy, f"CMP-EXT-{adv_id}", now, now))
        campaign_ids.append((cur.fetchone()["campaign_id"], adv_id))

    # --- Insertion orders (one per campaign) + placements + delivery stats ---
    sales_channels = ["direct", "programmatic"]
    for idx, (cid, adv_id) in enumerate(campaign_ids):
        brand_id = next(b for b, a in brand_ids if a == adv_id)
        cur.execute(
            "INSERT INTO fw_insertion_orders (campaign_id, name, description, client_po, "
            "brand_id, external_id, primary_sales_person, primary_trafficker, currency, "
            "status, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'USD','booked',%s,%s) RETURNING insertion_order_id",
            (cid, f"IO {cid}-1", "Seeded insertion order", f"PO-{cid}", brand_id,
             f"IO-EXT-{cid}", "Jordan Sales", "Casey Traffic", now, now))
        io_id = cur.fetchone()["insertion_order_id"]

        for p in range(2):
            cur.execute(
                "INSERT INTO fw_placements (insertion_order_id, name, description, external_id, "
                "start_date, end_date, status, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,'active',%s,%s) RETURNING placement_id",
                (io_id, f"Placement {io_id}-{p+1}", "Seeded placement",
                 f"PL-EXT-{io_id}-{p+1}", _past_date(30), _future_date(30), now, now))
            placement_id = cur.fetchone()["placement_id"]

            # ~30 days of daily delivery stats per placement
            channel = sales_channels[(idx + p) % len(sales_channels)]
            for d in range(30):
                stat_date = _past_date(29 - d)
                impressions = _random.randint(4000, 90000)
                clicks = int(impressions * _random.uniform(0.001, 0.02))
                spend = round(impressions / 1000 * _random.uniform(3.0, 12.0), 2)
                revenue = round(spend * _random.uniform(1.1, 1.8), 2)
                completion_rate = round(_random.uniform(0.55, 0.98), 4)
                cur.execute(
                    "INSERT INTO fw_delivery_stats (campaign_id, insertion_order_id, "
                    "placement_id, advertiser_id, sales_channel, stat_date, impressions, "
                    "clicks, spend, revenue, completion_rate) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (cid, io_id, placement_id, adv_id, channel, stat_date,
                     impressions, clicks, spend, revenue, completion_rate))

    # --- Audience items / segments (reporting dimensions) ---
    for i in range(4):
        cur.execute(
            "INSERT INTO fw_audience_items (name, status, sales_channel) VALUES (%s,'active',%s)",
            (f"Audience Item {i+1}", sales_channels[i % len(sales_channels)]))
    for i in range(3):
        cur.execute(
            "INSERT INTO fw_audience_segments (name, segment_kv, sales_channel) VALUES (%s,%s,%s)",
            (f"Audience Segment {i+1}", f"age={25+i*10}-{34+i*10}",
             sales_channels[i % len(sales_channels)]))

    # --- Creative instance metrics ---
    for ci in range(2):
        cur.execute(
            "INSERT INTO fw_creative_metrics (creative_instance_id, metric_type, event_name, "
            "url, created_at) VALUES (%s,%s,%s,%s,%s)",
            (9000 + ci, "impression", "fire", f"https://pixel.example/{ci}", now))

    # --- Marketplace (V4) ---
    for i in range(3):
        cur.execute(
            "INSERT INTO fw_available_listings (name, seller, inventory_type, floor_cpm, status) "
            "VALUES (%s,%s,%s,%s,'available')",
            (f"Available Listing {i+1}", f"Seller {i+1}", "ctv", round(5.0 + i, 2)))
    for i in range(2):
        cur.execute(
            "INSERT INTO fw_listings (name, seller, status) VALUES (%s,%s,'active')",
            (f"Listing {i+1}", f"Seller {i+1}"))
    mkpl_types = [
        "mkpl_exchange_programmatic_creative",
        "mkpl_private_direct_sold_creative",
        "mkpl_private_programmatic_creative",
    ]
    for i, t in enumerate(mkpl_types):
        cur.execute(
            "INSERT INTO fw_mkpl_creatives (name, creative_type, status) VALUES (%s,%s,'active')",
            (f"Marketplace Creative {i+1}", t))
    for i in range(2):
        cur.execute(
            "INSERT INTO fw_inventory_splits (name, split_percent, status) VALUES (%s,%s,'active')",
            (f"Inventory Split {i+1}", round(50.0, 2)))
        cur.execute(
            "INSERT INTO fw_inventory_split_orders (name, inventory_split_id, status) "
            "VALUES (%s,%s,'active')", (f"Split Order {i+1}", i + 1))
        cur.execute(
            "INSERT INTO fw_purchased_inventory_orders (name, buyer, status) VALUES (%s,%s,'active')",
            (f"Purchased Order {i+1}", f"Buyer {i+1}"))
        cur.execute(
            "INSERT INTO fw_sold_inventory_orders (name, seller, status) VALUES (%s,%s,'active')",
            (f"Sold Order {i+1}", f"Seller {i+1}"))
        cur.execute(
            "INSERT INTO fw_supply_source_packages (name, source, status) VALUES (%s,%s,'active')",
            (f"Supply Source Package {i+1}", f"Source {i+1}"))

    # --- RFP forecasts ---
    for i in range(2):
        cur.execute(
            "INSERT INTO fw_rfp_forecasts (granularity, data_json, status, created_at) "
            "VALUES (%s,%s,'completed',%s)",
            ("daily" if i == 0 else "weekly",
             '{"available_impressions": %d}' % (1000000 * (i + 1)), now))

    # --- Proposed IO -> placement -> ad (under first campaign) ---
    first_campaign = campaign_ids[0][0]
    cur.execute(
        "INSERT INTO fw_proposed_insertion_orders (campaign_id, name, status, created_at) "
        "VALUES (%s,%s,'proposed',%s) RETURNING proposed_io_id",
        (first_campaign, "Proposed IO 1", now))
    pio_id = cur.fetchone()["proposed_io_id"]
    cur.execute(
        "INSERT INTO fw_proposed_placements (proposed_io_id, name, status, created_at) "
        "VALUES (%s,%s,'proposed',%s) RETURNING proposed_placement_id",
        (pio_id, "Proposed Placement 1", now))
    ppl_id = cur.fetchone()["proposed_placement_id"]
    cur.execute(
        "INSERT INTO fw_proposed_ads (proposed_placement_id, name, status, created_at) "
        "VALUES (%s,%s,'proposed',%s)", (ppl_id, "Proposed Ad 1", now))

    # --- Programmatic deals + buyers + globals ---
    deal_specs = [
        ("Premium CTV Deal", "preferred_deal", "High-value CTV inventory", "Sam Seller"),
        ("Open Exchange Deal", "private_auction", "Programmatic auction deal", "Pat Seller"),
    ]
    for name, dtype, desc, sp in deal_specs:
        cur.execute(
            "INSERT INTO fw_programmatic_deals (name, deal_type, description, salesperson, "
            "status, created_at) VALUES (%s,%s,%s,%s,'active',%s)",
            (name, dtype, desc, sp, now))
    for i in range(2):
        cur.execute(
            "INSERT INTO fw_programmatic_buyers (name, seat_id) VALUES (%s,%s)",
            (f"Programmatic Buyer {i+1}", f"SEAT-{100+i}"))
    for i in range(2):
        cur.execute(
            "INSERT INTO fw_global_advertisers (name) VALUES (%s) RETURNING id",
            (f"Global Advertiser {i+1}",))
        gadv_id = cur.fetchone()["id"]
        cur.execute(
            "INSERT INTO fw_global_brands (name, global_advertiser_id) VALUES (%s,%s)",
            (f"Global Brand {i+1}", gadv_id))


# Organization for seeded and POSTed Hivestack records (see _hs_organization_id)
HS_ORGANIZATION_ID = _hs_organization_id()
