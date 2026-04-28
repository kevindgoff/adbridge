import uuid
import os
import random
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

# ─── Delegate get_db / init_db to the backend abstraction ───────────────────
from app.db_backend import get_db, init_db  # noqa: F401 — re-exported for all importers


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
    contact_name TEXT,
    contact_email TEXT,
    billing_name TEXT,
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
    budget_amount REAL,
    budget_currency TEXT DEFAULT 'USD',
    start_date TEXT,
    end_date TEXT,
    client_id TEXT NOT NULL REFERENCES clients(id),
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
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    name TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'dsp',
    format TEXT,
    platform TEXT,
    ad_sizes TEXT,
    rate_type TEXT,
    rate_amount REAL,
    contracted_spend REAL,
    contracted_units INTEGER,
    advertising_channel TEXT,
    start_date TEXT,
    end_date TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS addons (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(id),
    name TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    contracted_spend REAL,
    actual_spend REAL,
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
    external_reference TEXT,
    media_type TEXT,
    classification TEXT,
    pixel_width INTEGER,
    pixel_height INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conversions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    external_source TEXT,
    external_reference TEXT,
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
    campaign_id TEXT REFERENCES campaigns(id),
    date TEXT NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    spend REAL DEFAULT 0.0,
    viewability REAL DEFAULT 0.0,
    video_completions INTEGER DEFAULT 0,
    click_conversions INTEGER DEFAULT 0,
    view_conversions INTEGER DEFAULT 0,
    conversion_revenue REAL DEFAULT 0.0
);

-- ==================== Hivestack (OpenRTB 2.5 DOOH) Tables ====================

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

CREATE TABLE IF NOT EXISTS hs_accounts (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'buyer',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_advertisers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    account_id TEXT REFERENCES hs_accounts(id),
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_campaigns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    advertiser_id TEXT REFERENCES hs_advertisers(id),
    status TEXT NOT NULL DEFAULT 'draft',
    start_date TEXT,
    end_date TEXT,
    budget REAL DEFAULT 0,
    currency TEXT DEFAULT 'USD',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_lineitems (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    campaign_id TEXT REFERENCES hs_campaigns(id),
    status TEXT NOT NULL DEFAULT 'draft',
    start_date TEXT,
    end_date TEXT,
    budget REAL DEFAULT 0,
    cpm REAL DEFAULT 1.0,
    impressions_goal INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_lineitem_targeting (
    id TEXT PRIMARY KEY,
    lineitem_id TEXT NOT NULL REFERENCES hs_lineitems(id),
    targeting_type TEXT NOT NULL,
    targeting_value TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_lineitem_units (
    lineitem_id TEXT NOT NULL REFERENCES hs_lineitems(id),
    unit_id TEXT NOT NULL,
    PRIMARY KEY (lineitem_id, unit_id)
);

CREATE TABLE IF NOT EXISTS hs_creatives (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    advertiser_id TEXT,
    type TEXT NOT NULL DEFAULT 'video',
    status TEXT NOT NULL DEFAULT 'draft',
    width INTEGER DEFAULT 1920,
    height INTEGER DEFAULT 1080,
    file_url TEXT,
    approval_status TEXT DEFAULT 'pending_review',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_creative_approvals (
    id TEXT PRIMARY KEY,
    creative_id TEXT NOT NULL REFERENCES hs_creatives(id),
    media_owner_id TEXT,
    approval_status TEXT NOT NULL DEFAULT 'pending',
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_creative_units (
    creative_id TEXT NOT NULL REFERENCES hs_creatives(id),
    unit_id TEXT NOT NULL,
    PRIMARY KEY (creative_id, unit_id)
);

CREATE TABLE IF NOT EXISTS hs_lineitem_creatives (
    id TEXT PRIMARY KEY,
    lineitem_id TEXT NOT NULL,
    creative_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_lineitem_deal_creatives (
    id TEXT PRIMARY KEY,
    lineitem_id TEXT NOT NULL,
    deal_id TEXT NOT NULL,
    creative_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_networks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_sites (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    network_id TEXT REFERENCES hs_networks(id),
    external_id TEXT UNIQUE,
    address TEXT,
    city TEXT,
    country TEXT DEFAULT 'USA',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_units (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    site_id TEXT REFERENCES hs_sites(id),
    external_id TEXT UNIQUE,
    width INTEGER DEFAULT 1920,
    height INTEGER DEFAULT 1080,
    venue_type_id INTEGER,
    unitpack_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_unitpacks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_unitpack_targeting (
    id TEXT PRIMARY KEY,
    unitpack_id TEXT NOT NULL REFERENCES hs_unitpacks(id),
    targeting_type TEXT,
    targeting_value TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_demographics (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    code TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_languages (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    code TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_locations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT,
    region TEXT,
    city TEXT
);

CREATE TABLE IF NOT EXISTS hs_mediatypes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS hs_unit_languages (
    id TEXT PRIMARY KEY,
    unit_id TEXT NOT NULL,
    language_id TEXT NOT NULL REFERENCES hs_languages(id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_unit_properties (
    id TEXT PRIMARY KEY,
    unit_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_concentrations (
    unit_id TEXT NOT NULL,
    day_of_week INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    audience_count REAL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (unit_id, day_of_week, hour)
);

CREATE TABLE IF NOT EXISTS hs_report_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    query_config TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hs_report_executions (
    id TEXT PRIMARY KEY,
    definition_id TEXT REFERENCES hs_report_definitions(id),
    status TEXT NOT NULL DEFAULT 'pending',
    download_url TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
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

-- ==================== AdsWizz Domain API v8 Tables ====================

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
    comments TEXT,
    external_reference TEXT,
    deal_id TEXT,
    archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL
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
    archived BOOLEAN DEFAULT FALSE,
    created_at TEXT NOT NULL
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
        ("Acme Corporation", "Jane Smith", "jane@acme.com", "Acme Corp Billing"),
        ("TechNova Inc", "Bob Chen", "bob@technova.com", "TechNova Billing"),
        ("GreenLeaf Brands", "Sara Johnson", "sara@greenleaf.com", "GreenLeaf LLC"),
    ]
    client_ids = []
    for name, contact, email, billing in client_data:
        cid = _uuid()
        client_ids.append(cid)
        cur.execute(
            "INSERT INTO clients VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (cid, name, contact, email, billing, "Synthetic test client", now),
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
        cur.execute(
            "INSERT INTO campaigns VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                cid, f"Campaign {i+1} - {'Spring' if i < 4 else 'Summer'} 2025",
                f"UGCID-{1000+i}", f"Initiative {'Alpha' if i < 4 else 'Beta'}",
                statuses[i % 3],
                round(random.uniform(10000, 500000), 2), "USD",
                _past_date(90 - i * 10), _future_date(30 + i * 10),
                client_ids[client_idx], now,
            ),
        )
        for kid in random.sample(kpi_ids, k=2):
            cur.execute(
                "INSERT INTO campaign_kpis VALUES (%s,%s,%s)",
                (cid, kid, round(random.uniform(0.5, 10.0), 2)),
            )

    # --- Line Items ---
    formats = ["display", "video", "native", "audio"]
    platforms = ["desktop", "mobile", "ctv", "all"]
    line_item_ids = []
    for camp_id in campaign_ids:
        for j in range(random.randint(2, 4)):
            lid = _uuid()
            line_item_ids.append((lid, camp_id))
            cur.execute(
                "INSERT INTO line_items VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    lid, camp_id, f"Line Item {j+1}",
                    random.choice(["direct", "dsp"]),
                    random.choice(formats), random.choice(platforms),
                    "300x250,728x90", random.choice(["cpm", "cpc", "cpv"]),
                    round(random.uniform(1, 25), 2),
                    round(random.uniform(5000, 100000), 2),
                    random.randint(10000, 500000),
                    random.choice(["programmatic", "direct", "social"]),
                    _past_date(60), _future_date(30), now,
                ),
            )

    # --- Addons ---
    for camp_id in campaign_ids[:4]:
        aid = _uuid()
        contracted = round(random.uniform(1000, 10000), 2)
        cur.execute(
            "INSERT INTO addons VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                aid, camp_id, random.choice(["Ad Serving Fee", "Data Fee", "Verification Fee"]),
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
        cur.execute(
            "INSERT INTO creatives VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                _uuid(), f"Creative {i+1} - {'Banner' if i % 2 == 0 else 'Video'}",
                random.choice(sources), f"EXT-REF-{10000+i}",
                random.choice(media_types),
                random.choice(["standard", "premium", "interactive"]),
                random.choice([300, 728, 160, 320, 1920]),
                random.choice([250, 90, 600, 50, 1080]),
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

    # --- Stats ---
    for lid, camp_id in line_item_ids:
        for day_offset in range(30):
            date = _past_date(30 - day_offset)
            impressions = random.randint(1000, 50000)
            clicks = random.randint(10, int(impressions * 0.05))
            spend = round(random.uniform(50, 2000), 2)
            cur.execute(
                "INSERT INTO stats (line_item_id,campaign_id,date,impressions,clicks,spend,"
                "viewability,video_completions,click_conversions,view_conversions,conversion_revenue) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    lid, camp_id, date, impressions, clicks, spend,
                    round(random.uniform(0.4, 0.95), 4),
                    random.randint(0, int(impressions * 0.3)),
                    random.randint(0, 20), random.randint(0, 10),
                    round(random.uniform(0, 500), 2),
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


def _seed_hivestack(cur, now):
    """Seed Hivestack DOOH mock data: publishers, screens, and deals."""
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

    # Accounts
    account_ids = []
    for aname in ["Horizon Media Group", "Omnicom Media", "Publicis Groupe"]:
        aid = str(uuid.uuid4())
        account_ids.append(aid)
        cur.execute(
            "INSERT INTO hs_accounts (id, name, type, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
            (aid, aname, "buyer", "active", now, now))

    # Advertisers
    advertiser_ids = []
    adv_data = [
        ("Acme Corp DOOH", 0), ("TechNova Outdoor", 0),
        ("GreenLeaf OOH", 1), ("Sunset Travel", 1), ("Amazon Ads", 2),
    ]
    for aname, acc_idx in adv_data:
        aid = str(uuid.uuid4())
        advertiser_ids.append(aid)
        cur.execute(
            "INSERT INTO hs_advertisers (id, name, account_id, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
            (aid, aname, account_ids[acc_idx], "active", now, now))

    # Campaigns
    campaign_ids = []
    for i, adv_id in enumerate(advertiser_ids):
        for j in range(2):
            cid = str(uuid.uuid4())
            campaign_ids.append(cid)
            cur.execute(
                "INSERT INTO hs_campaigns (id, name, advertiser_id, status, start_date, end_date, budget, currency, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (cid, f"DOOH Campaign {i+1}-{j+1}", adv_id,
                 random.choice(["active", "draft", "completed"]),
                 _past_date(60), _future_date(30),
                 round(random.uniform(5000, 100000), 2), "USD", now, now))

    # Line Items
    lineitem_ids = []
    for camp_id in campaign_ids[:6]:
        for k in range(random.randint(1, 3)):
            lid = str(uuid.uuid4())
            lineitem_ids.append(lid)
            cur.execute(
                "INSERT INTO hs_lineitems (id, name, campaign_id, status, start_date, end_date, budget, cpm, impressions_goal, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (lid, f"Line Item {k+1}", camp_id, random.choice(["active", "draft"]),
                 _past_date(30), _future_date(30),
                 round(random.uniform(1000, 20000), 2),
                 round(random.uniform(1.0, 15.0), 2),
                 random.randint(10000, 500000), now, now))

    # Creatives
    creative_ids = []
    for i in range(10):
        cid = str(uuid.uuid4())
        creative_ids.append(cid)
        cur.execute(
            "INSERT INTO hs_creatives (id, name, advertiser_id, type, status, width, height, file_url, approval_status, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (cid, f"DOOH Creative {i+1}", advertiser_ids[i % len(advertiser_ids)],
             random.choice(["video", "image"]), "active",
             random.choice([1920, 1080, 1280]), random.choice([1080, 1920, 720]),
             f"https://cdn.mock.local/creative_{i+1}.mp4",
             random.choice(["approved", "pending_review", "approved"]), now, now))

    # Creative approvals
    for cid in creative_ids[:6]:
        cur.execute(
            "INSERT INTO hs_creative_approvals (id, creative_id, media_owner_id, approval_status, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid.uuid4()), cid, pub_ids[0],
             random.choice(["approved", "pending", "approved"]), now, now))

    # Networks
    network_ids = []
    for nname in ["Downtown Digital Network", "Transit Media Network", "Mall Network", "Airport Network"]:
        nid = str(uuid.uuid4())
        network_ids.append(nid)
        cur.execute("INSERT INTO hs_networks (id, name, description, created_at, updated_at) VALUES (%s,%s,%s,%s,%s)",
                    (nid, nname, f"Mock {nname}", now, now))

    # Sites
    site_data = [
        ("Times Square Hub", 0, "1 Times Square", "New York", "USA"),
        ("Union Station", 0, "50 Massachusetts Ave", "Washington DC", "USA"),
        ("Eaton Centre", 1, "220 Yonge St", "Toronto", "CAN"),
        ("Pearson Airport T1", 3, "6301 Silver Dart Dr", "Mississauga", "CAN"),
        ("LAX Terminal 4", 2, "1 World Way", "Los Angeles", "USA"),
    ]
    site_ids = []
    for sname, net_idx, addr, city, country in site_data:
        sid = str(uuid.uuid4())
        site_ids.append(sid)
        cur.execute(
            "INSERT INTO hs_sites (id, name, network_id, external_id, address, city, country, created_at, updated_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (sid, sname, network_ids[net_idx], f"EXT-{sname[:4].upper()}", addr, city, country, now, now))

    # Units (linked to sites)
    unit_ids = []
    venue_types = [105, 205, 301, 401, 501]
    for i, site_id in enumerate(site_ids):
        for j in range(2):
            uid = str(uuid.uuid4())
            unit_ids.append(uid)
            w, h = random.choice([(1920, 1080), (1080, 1920), (1280, 720)])
            cur.execute(
                "INSERT INTO hs_units (id, name, site_id, external_id, width, height, venue_type_id, status, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (uid, f"Screen {i+1}-{j+1}", site_id, f"SCR-{i+1:02d}{j+1}",
                 w, h, venue_types[i % len(venue_types)], "active", now, now))

    # Unit Packs
    for pname in ["Premium NYC Pack", "Transit Pack", "Airport Pack"]:
        pid = str(uuid.uuid4())
        cur.execute("INSERT INTO hs_unitpacks (id, name, description, created_at, updated_at) VALUES (%s,%s,%s,%s,%s)",
                    (pid, pname, f"Mock {pname}", now, now))

    # Demographics
    demo_data = [
        ("Male 18-34", "GNDR-M|AGE-18-34"), ("Female 18-34", "GNDR-F|AGE-18-34"),
        ("Adults 25-54", "AGE-25-54"), ("High Income", "HHI-100K-*"),
        ("Young Adults", "AGE-18-24"), ("Seniors 65+", "AGE-65-*"),
    ]
    for dname, dcode in demo_data:
        cur.execute("INSERT INTO hs_demographics (id, name, code, description, created_at) VALUES (%s,%s,%s,%s,%s)",
                    (str(uuid.uuid4()), dname, dcode, f"Demographic: {dname}", now))

    # Languages
    lang_data = [("en", "English"), ("fr", "French"), ("es", "Spanish"),
                 ("zh", "Chinese"), ("de", "German"), ("ja", "Japanese")]
    lang_ids = {}
    for code, name in lang_data:
        lid = str(uuid.uuid4())
        lang_ids[code] = lid
        cur.execute("INSERT INTO hs_languages (id, name, code) VALUES (%s,%s,%s)", (lid, name, code))

    # Locations
    loc_data = [
        ("New York Metro", "USA", "NY", "New York"),
        ("Los Angeles Metro", "USA", "CA", "Los Angeles"),
        ("Toronto Metro", "CAN", "ON", "Toronto"),
        ("Chicago Metro", "USA", "IL", "Chicago"),
        ("Montreal Metro", "CAN", "QC", "Montreal"),
    ]
    for lname, country, region, city in loc_data:
        cur.execute("INSERT INTO hs_locations (id, name, country, region, city) VALUES (%s,%s,%s,%s,%s)",
                    (str(uuid.uuid4()), lname, country, region, city))

    # Mediatypes
    for mtname in ["Digital Billboard", "Street Furniture", "Transit", "Airport", "Retail"]:
        cur.execute("INSERT INTO hs_mediatypes (id, name, description) VALUES (%s,%s,%s)",
                    (str(uuid.uuid4()), mtname, f"DOOH {mtname} format"))

    # Unit Languages
    for uid in unit_ids[:4]:
        cur.execute("INSERT INTO hs_unit_languages (id, unit_id, language_id, created_at) VALUES (%s,%s,%s,%s)",
                    (str(uuid.uuid4()), uid, lang_ids["en"], now))

    # Unit Properties
    for uid in unit_ids[:4]:
        for key, val in [("brightness", "high"), ("orientation", "landscape"), ("audio", "0")]:
            cur.execute("INSERT INTO hs_unit_properties (id, unit_id, key, value, created_at) VALUES (%s,%s,%s,%s,%s)",
                        (str(uuid.uuid4()), uid, key, val, now))

    # Concentrations (sample hourly data for first 2 units)
    for uid in unit_ids[:2]:
        for dow in range(7):
            for hour in range(24):
                audience = round(random.uniform(5, 200), 1)
                cur.execute(
                    "INSERT INTO hs_concentrations (unit_id, day_of_week, hour, audience_count, updated_at) "
                    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (uid, dow, hour, audience, now))

    # Report Definitions
    report_def_ids = []
    for rname in ["Impressions by Unit", "Campaign Performance", "Audience Demographics Report"]:
        rid = str(uuid.uuid4())
        report_def_ids.append(rid)
        cur.execute("INSERT INTO hs_report_definitions (id, name, description, query_config, created_at) VALUES (%s,%s,%s,%s,%s)",
                    (rid, rname, f"Mock {rname}", "{}", now))

    # Report Executions
    for rid in report_def_ids:
        cur.execute(
            "INSERT INTO hs_report_executions (id, definition_id, status, download_url, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
            (str(uuid.uuid4()), rid, "completed",
             f"https://mock.adbridge.local/reports/{rid}.csv", now, now))

    # Custom Events
    event_ids = []
    for ename in ["Super Bowl Weekend", "Black Friday", "Holiday Season"]:
        eid = str(uuid.uuid4())
        event_ids.append(eid)
        cur.execute("INSERT INTO hs_custom_events (id, name, description, status, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
                    (eid, ename, f"Mock event: {ename}", "active", now, now))

    # LineItem-Creative associations
    for i, lid in enumerate(lineitem_ids[:5]):
        cid = creative_ids[i % len(creative_ids)]
        cur.execute("INSERT INTO hs_lineitem_creatives (id, lineitem_id, creative_id, created_at) VALUES (%s,%s,%s,%s)",
                    (str(uuid.uuid4()), lid, cid, now))
        cur.execute("INSERT INTO hs_lineitem_units (lineitem_id, unit_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (lid, unit_ids[i % len(unit_ids)]))


def _seed_adswizz(cur, now):
    """Seed AdsWizz Domain API v8 mock data."""
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
    for name, domain, contact, email in adv_data:
        cur.execute(
            "INSERT INTO aw_advertisers (name, domain, contact, email, status, ad_clashing, created_at) "
            "VALUES (%s,%s,%s,%s,'ACTIVE',false,%s) RETURNING id",
            (name, domain, contact, email, now))
        adv_ids.append(cur.fetchone()["id"])

    # --- Orders ---
    order_ids = []
    for i, oname in enumerate(["Q1 Audio Push", "Podcast Sponsorship", "Summer Streaming"]):
        cur.execute(
            "INSERT INTO aw_orders (name, advertiser_id, start_date, end_date, "
            "objective_type, objective_value, objective_currency, objective_unlimited, "
            "comments, archived, created_at) "
            "VALUES (%s,%s,%s,%s,'IMPRESSIONS',%s,'USD',false,%s,false,%s) RETURNING id",
            (oname, adv_ids[i % len(adv_ids)],
             "2025-01-01T00:00:00Z", "2025-06-30T23:59:59Z",
             500000 * (i + 1), f"Mock order {i+1}", now))
        order_ids.append(cur.fetchone()["id"])

    # --- Campaigns ---
    campaign_types = ["STANDARD", "FILLER", "STANDARD", "SPONSORSHIP", "STANDARD", "INTERACTIVE"]
    statuses = ["RUNNING", "PAUSED", "READY", "COMPLETED", "DRAFT", "RUNNING"]
    pacing_types = ["EVENLY", "ASAP", "EVENLY", "EVENLY", "ASAP", "EVENLY"]
    camp_ids = []
    for i in range(6):
        cur.execute(
            "INSERT INTO aw_campaigns "
            "(name, campaign_type, advertiser_id, order_id, status, billing, "
            "start_date, end_date, revenue_type, revenue_value, revenue_currency, "
            "objective_type, objective_value, objective_unlimited, "
            "pacing_type, pacing_priority, comments, external_reference, archived, created_at) "
            "VALUES (%s,%s,%s,%s,%s,'SOLD',%s,%s,'CPM',%s,'USD','IMPRESSIONS',%s,false,%s,%s,%s,%s,false,%s) "
            "RETURNING id",
            (f"Campaign {i+1} - Audio {'Spring' if i < 3 else 'Summer'}",
             campaign_types[i], adv_ids[i % len(adv_ids)],
             order_ids[i % len(order_ids)] if i < 3 else None,
             statuses[i],
             "2025-02-01T00:00:00Z", "2025-08-31T23:59:59Z",
             round(random.uniform(2.0, 12.0), 2),
             random.randint(50000, 500000),
             pacing_types[i], random.randint(1, 8),
             f"Mock campaign {i+1}", f"EXT-AW-{1000+i}", now))
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
