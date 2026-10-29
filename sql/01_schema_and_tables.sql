-- ==============================================================================
-- Deterministic Order Promise, Sourcing, and Inventory Allocation Orchestrator
-- Schema and Core Database Tables (PostgreSQL 18+)
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS order_scm;

-- Set search path for this session
SET search_path TO order_scm, public;

-- 1. Master Data: Customers
CREATE TABLE IF NOT EXISTS order_scm.customer_master (
    customer_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    priority_level INT NOT NULL DEFAULT 3, -- 1 (Strategic/VIP) to 5 (Standard)
    sla_tier VARCHAR(32) NOT NULL DEFAULT 'STANDARD', -- GOLD, SILVER, BRONZE, STANDARD
    credit_limit NUMERIC(12, 2) NOT NULL DEFAULT 50000.00,
    open_exposure NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    overdue_balance NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    payment_terms VARCHAR(32) NOT NULL DEFAULT 'NET30',
    allow_substitutes BOOLEAN NOT NULL DEFAULT true,
    allow_splits BOOLEAN NOT NULL DEFAULT true,
    max_split_count INT NOT NULL DEFAULT 3,
    country_code VARCHAR(2) NOT NULL DEFAULT 'US',
    postal_code VARCHAR(32),
    receiving_days VARCHAR(64) NOT NULL DEFAULT 'MON,TUE,WED,THU,FRI',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Master Data: Materials / SKUs
CREATE TABLE IF NOT EXISTS order_scm.material_master (
    sku VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(64) NOT NULL,
    uom VARCHAR(16) NOT NULL DEFAULT 'EA',
    active BOOLEAN NOT NULL DEFAULT true,
    unit_cost NUMERIC(10, 2) NOT NULL DEFAULT 10.00,
    unit_price NUMERIC(10, 2) NOT NULL DEFAULT 20.00,
    substitution_group VARCHAR(64),
    lead_time_days INT NOT NULL DEFAULT 2,
    hazardous BOOLEAN NOT NULL DEFAULT false,
    shelf_life_days INT,
    direct_ship_eligible BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Master Data: Fulfillment Locations (Warehouses, Stores, Plants)
CREATE TABLE IF NOT EXISTS order_scm.location_master (
    location_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location_type VARCHAR(32) NOT NULL, -- CENTRAL_DC, REGIONAL_DC, STORE, SUPPLIER_DIRECT, 3PL
    active BOOLEAN NOT NULL DEFAULT true,
    daily_order_capacity INT NOT NULL DEFAULT 1000,
    current_day_orders INT NOT NULL DEFAULT 0,
    warehouse_cut_off_time TIME NOT NULL DEFAULT '16:00:00',
    handling_lead_time_hours NUMERIC(4, 1) NOT NULL DEFAULT 4.0,
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    supported_carriers TEXT[] NOT NULL DEFAULT '{"FEDEX", "UPS", "DHL", "INTERNAL"}',
    reliability_score NUMERIC(4, 3) NOT NULL DEFAULT 0.980,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Master Data: Carrier Lanes and Transit Matrices
CREATE TABLE IF NOT EXISTS order_scm.carrier_lane (
    lane_id SERIAL PRIMARY KEY,
    origin_location_id VARCHAR(64) REFERENCES order_scm.location_master(location_id),
    destination_postal_zone VARCHAR(16) NOT NULL,
    carrier_code VARCHAR(32) NOT NULL,
    service_level VARCHAR(32) NOT NULL, -- GROUND, EXPEDITED, OVERNIGHT, FREIGHT
    transit_days INT NOT NULL DEFAULT 2,
    carrier_pickup_cutoff TIME NOT NULL DEFAULT '17:30:00',
    shipping_cost_base NUMERIC(8, 2) NOT NULL DEFAULT 12.50,
    cost_per_kg NUMERIC(6, 2) NOT NULL DEFAULT 0.75,
    on_time_reliability NUMERIC(4, 3) NOT NULL DEFAULT 0.950
);

-- 5. Inventory Positions (Multi-Bucket & Multi-Source)
CREATE TABLE IF NOT EXISTS order_scm.inventory_position (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(64) REFERENCES order_scm.material_master(sku),
    location_id VARCHAR(64) REFERENCES order_scm.location_master(location_id),
    source_type VARCHAR(32) NOT NULL, -- WMS_REALTIME, ERP_AGGREGATE, STORE_POS, SUPPLIER_EDI, IN_TRANSIT
    gross_on_hand INT NOT NULL DEFAULT 0,
    quality_blocked INT NOT NULL DEFAULT 0,
    warehouse_blocked INT NOT NULL DEFAULT 0,
    customer_reserved INT NOT NULL DEFAULT 0,
    order_allocated INT NOT NULL DEFAULT 0,
    in_transit_inward INT NOT NULL DEFAULT 0,
    supplier_confirmed INT NOT NULL DEFAULT 0,
    expected_receipt_qty INT NOT NULL DEFAULT 0,
    expected_receipt_date DATE,
    source_confidence_score NUMERIC(4, 3) NOT NULL DEFAULT 1.000, -- 1.0 = WMS, 0.85 = ERP, 0.70 = Stale
    last_snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_inv_sku_loc_src UNIQUE (sku, location_id, source_type)
);

-- 6. Shared State Object / Blackboard: Order Case
CREATE TABLE IF NOT EXISTS order_scm.order_case (
    case_id VARCHAR(64) PRIMARY KEY,
    channel VARCHAR(32) NOT NULL,
    external_order_ref VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    current_state VARCHAR(64) NOT NULL DEFAULT 'ORDER_RECEIVED',
    state_version INT NOT NULL DEFAULT 1,
    priority INT NOT NULL DEFAULT 3,
    customer_id VARCHAR(64) REFERENCES order_scm.customer_master(customer_id),
    requested_delivery_date DATE NOT NULL,
    raw_order_payload JSONB NOT NULL,
    normalized_order JSONB,
    validation_results JSONB,
    credit_fraud_results JSONB,
    inventory_convergence_results JSONB,
    atp_ctp_results JSONB,
    sourcing_results JSONB,
    allocation_plan JSONB,
    promise_details JSONB,
    holds JSONB NOT NULL DEFAULT '[]'::jsonb,
    exceptions JSONB NOT NULL DEFAULT '[]'::jsonb,
    next_action VARCHAR(64) NOT NULL DEFAULT 'VALIDATE_SCHEMA',
    retry_count INT NOT NULL DEFAULT 0,
    is_closed BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. Normalized Order Lines
CREATE TABLE IF NOT EXISTS order_scm.order_line (
    line_id VARCHAR(64) PRIMARY KEY,
    case_id VARCHAR(64) REFERENCES order_scm.order_case(case_id) ON DELETE CASCADE,
    line_number INT NOT NULL,
    sku VARCHAR(64) NOT NULL,
    ordered_qty INT NOT NULL,
    uom VARCHAR(16) NOT NULL DEFAULT 'EA',
    unit_price NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    line_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    allocated_qty INT NOT NULL DEFAULT 0,
    substitute_sku VARCHAR(64),
    is_substituted BOOLEAN NOT NULL DEFAULT false,
    backordered_qty INT NOT NULL DEFAULT 0,
    promised_delivery_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 8. Sourcing Candidates Evaluated
CREATE TABLE IF NOT EXISTS order_scm.sourcing_candidate (
    candidate_id SERIAL PRIMARY KEY,
    case_id VARCHAR(64) REFERENCES order_scm.order_case(case_id) ON DELETE CASCADE,
    line_id VARCHAR(64) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    location_id VARCHAR(64) NOT NULL,
    source_type VARCHAR(32) NOT NULL,
    available_qty INT NOT NULL,
    available_date DATE NOT NULL,
    fulfillment_cost NUMERIC(10, 2) NOT NULL,
    transit_days INT NOT NULL,
    confidence_score NUMERIC(4, 3) NOT NULL,
    is_feasible BOOLEAN NOT NULL DEFAULT true,
    rank_score NUMERIC(8, 4) NOT NULL DEFAULT 0.0000,
    rejection_reason VARCHAR(255)
);

-- 9. Allocation Decisions (Line-Node Assignment)
CREATE TABLE IF NOT EXISTS order_scm.allocation_plan (
    allocation_id SERIAL PRIMARY KEY,
    case_id VARCHAR(64) REFERENCES order_scm.order_case(case_id) ON DELETE CASCADE,
    line_id VARCHAR(64) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    location_id VARCHAR(64) NOT NULL,
    allocated_qty INT NOT NULL,
    estimated_cost NUMERIC(10, 2) NOT NULL,
    estimated_dispatch_date DATE NOT NULL,
    estimated_delivery_date DATE NOT NULL,
    is_split BOOLEAN NOT NULL DEFAULT false,
    split_group INT NOT NULL DEFAULT 1,
    status VARCHAR(32) NOT NULL DEFAULT 'RESERVED', -- RESERVED, RELEASED, PICKED, CANCELLED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 10. Final Promises Issued
CREATE TABLE IF NOT EXISTS order_scm.order_promise (
    promise_id SERIAL PRIMARY KEY,
    case_id VARCHAR(64) REFERENCES order_scm.order_case(case_id) ON DELETE CASCADE,
    promised_delivery_date DATE NOT NULL,
    earliest_possible_date DATE NOT NULL,
    confidence_score NUMERIC(4, 3) NOT NULL,
    confidence_tier VARCHAR(16) NOT NULL, -- HIGH, MEDIUM, LOW
    promise_source VARCHAR(64) NOT NULL,
    risk_flags TEXT[] NOT NULL DEFAULT '{}',
    alternatives JSONB NOT NULL DEFAULT '[]'::jsonb,
    customer_accepted BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 11. Exception Cases & HITL Queues
CREATE TABLE IF NOT EXISTS order_scm.order_exception (
    exception_id SERIAL PRIMARY KEY,
    case_id VARCHAR(64) REFERENCES order_scm.order_case(case_id) ON DELETE CASCADE,
    exception_type VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL, -- LOW, MEDIUM, HIGH, CRITICAL
    owner VARCHAR(64) NOT NULL DEFAULT 'OPS_AUTO',
    reason_code VARCHAR(64) NOT NULL,
    detail_message TEXT,
    suggested_action VARCHAR(64),
    status VARCHAR(32) NOT NULL DEFAULT 'OPEN', -- OPEN, IN_REVIEW, RESOLVED, OVERRIDDEN, DISMISSED
    resolution_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

-- 12. Full Immutable Audit Trail
CREATE TABLE IF NOT EXISTS order_scm.audit_event (
    event_id BIGSERIAL PRIMARY KEY,
    case_id VARCHAR(64) NOT NULL,
    actor VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    prev_state VARCHAR(64),
    new_state VARCHAR(64),
    rule_version VARCHAR(32) NOT NULL DEFAULT 'v1.0.0',
    result_summary TEXT NOT NULL,
    payload_snapshot JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 13. Deterministic KPI Fact Table
CREATE TABLE IF NOT EXISTS order_scm.kpi_fact (
    fact_id BIGSERIAL PRIMARY KEY,
    case_id VARCHAR(64) NOT NULL,
    channel VARCHAR(32) NOT NULL,
    customer_id VARCHAR(64),
    event_type VARCHAR(64) NOT NULL,
    promised_on_time BOOLEAN,
    first_pass_fill_rate NUMERIC(5, 4),
    is_oversell BOOLEAN NOT NULL DEFAULT false,
    is_backordered BOOLEAN NOT NULL DEFAULT false,
    is_split BOOLEAN NOT NULL DEFAULT false,
    split_count INT NOT NULL DEFAULT 1,
    is_cancelled BOOLEAN NOT NULL DEFAULT false,
    manual_intervention BOOLEAN NOT NULL DEFAULT false,
    intervention_type VARCHAR(64),
    cycle_time_seconds NUMERIC(10, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for lightning fast deterministic lookups
CREATE INDEX IF NOT EXISTS idx_order_case_fsm ON order_scm.order_case(current_state, next_action);
CREATE INDEX IF NOT EXISTS idx_order_case_cust ON order_scm.order_case(customer_id);
CREATE INDEX IF NOT EXISTS idx_inv_sku_loc ON order_scm.inventory_position(sku, location_id);
CREATE INDEX IF NOT EXISTS idx_audit_case ON order_scm.audit_event(case_id, created_at);
CREATE INDEX IF NOT EXISTS idx_kpi_facts ON order_scm.kpi_fact(case_id, event_type);
