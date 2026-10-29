-- ==============================================================================
-- Deterministic Order Promise, Sourcing, and Inventory Allocation Orchestrator
-- Master Seed Data (PostgreSQL 18+)
-- ==============================================================================

SET search_path TO order_scm, public;

-- Clean existing master data
TRUNCATE TABLE order_scm.carrier_lane CASCADE;
TRUNCATE TABLE order_scm.inventory_position CASCADE;
TRUNCATE TABLE order_scm.customer_master CASCADE;
TRUNCATE TABLE order_scm.material_master CASCADE;
TRUNCATE TABLE order_scm.location_master CASCADE;

-- 1. Customers
INSERT INTO order_scm.customer_master (
    customer_id, name, active, priority_level, sla_tier, credit_limit, open_exposure, overdue_balance,
    payment_terms, allow_substitutes, allow_splits, max_split_count, country_code, postal_code, receiving_days
) VALUES
('CUST-STRAT-001', 'Acme Global Enterprises', true, 1, 'GOLD', 500000.00, 45000.00, 0.00, 'NET60', true, true, 3, 'US', '10001', 'MON,TUE,WED,THU,FRI'),
('CUST-MID-002', 'Beta Retail Distribution', true, 3, 'SILVER', 60000.00, 15000.00, 0.00, 'NET30', true, true, 2, 'US', '60601', 'MON,TUE,WED,THU,FRI'),
('CUST-RISK-003', 'Overdue Risk Merchants', true, 4, 'STANDARD', 15000.00, 14500.00, 8500.00, 'NET15', false, false, 1, 'US', '90210', 'MON,TUE,WED,THU,FRI'),
('CUST-FRAUD-004', 'Phantom Velocity Traders', true, 5, 'STANDARD', 5000.00, 0.00, 0.00, 'PREPAID', false, false, 1, 'US', '33101', 'MON,WED,FRI'),
('CUST-INACTIVE-005', 'Dormant Accounts Corp', false, 5, 'STANDARD', 10000.00, 0.00, 0.00, 'NET30', false, false, 1, 'US', '75001', 'MON,TUE,WED');

-- 2. Materials / SKUs
INSERT INTO order_scm.material_master (
    sku, name, category, uom, active, unit_cost, unit_price, substitution_group, lead_time_days, hazardous, shelf_life_days, direct_ship_eligible
) VALUES
('SKU-LAPTOP-PRO', 'Apex Ultra 16-inch Laptop (i9/32GB)', 'COMPUTING', 'EA', true, 1400.00, 2200.00, 'GRP_LAPTOP_16', 2, false, NULL, true),
('SKU-LAPTOP-PLUS', 'Apex Ultra 16-inch Plus (i9/64GB)', 'COMPUTING', 'EA', true, 1600.00, 2450.00, 'GRP_LAPTOP_16', 2, false, NULL, true),
('SKU-MONITOR-4K', 'ProArt 32-inch 4K Studio Display', 'DISPLAYS', 'EA', true, 450.00, 799.00, 'GRP_MONITOR_32', 1, false, NULL, true),
('SKU-MONITOR-2K', 'ProArt 32-inch QHD Studio Display', 'DISPLAYS', 'EA', true, 320.00, 549.00, 'GRP_MONITOR_32', 1, false, NULL, true),
('SKU-DOCK-TB4', 'Universal Thunderbolt 4 Quad Dock', 'ACCESSORIES', 'EA', true, 110.00, 249.00, NULL, 1, false, NULL, true),
('SKU-CABLE-TB4', 'High-Speed TB4 Cable 2m Braided', 'ACCESSORIES', 'EA', true, 15.00, 49.00, NULL, 1, false, NULL, true),
('SKU-BATTERY-PACK', 'Industrial Li-Ion Hot-Swap Battery', 'POWER', 'EA', true, 80.00, 160.00, NULL, 3, true, 730, false),
('SKU-DISCONTINUED', 'Legacy Terminal Display Unit', 'LEGACY', 'EA', false, 50.00, 99.00, NULL, 10, false, NULL, false);

-- 3. Fulfillment Locations
INSERT INTO order_scm.location_master (
    location_id, name, location_type, active, daily_order_capacity, current_day_orders, warehouse_cut_off_time,
    handling_lead_time_hours, latitude, longitude, supported_carriers, reliability_score
) VALUES
('DC-EAST', 'East Coast Central Mega-DC (Newark, NJ)', 'CENTRAL_DC', true, 3000, 140, '16:00:00', 3.5, 40.735657, -74.172366, '{"FEDEX", "UPS", "DHL"}', 0.990),
('DC-WEST', 'West Coast Regional DC (Reno, NV)', 'REGIONAL_DC', true, 1800, 95, '15:30:00', 4.0, 39.529633, -119.813803, '{"FEDEX", "UPS", "DHL"}', 0.985),
('STORE-CENTRAL', 'Chicago Flagship Hub Store (Chicago, IL)', 'STORE', true, 350, 42, '14:00:00', 2.0, 41.878113, -87.629799, '{"UPS", "INTERNAL"}', 0.930),
('SUPPLIER-DIRECT', 'Apex Microtech Vendor Direct (Austin, TX)', 'SUPPLIER_DIRECT', true, 800, 60, '12:00:00', 24.0, 30.267153, -97.743061, '{"FEDEX", "DHL"}', 0.915);

-- 4. Carrier Lanes & Transit Time Matrices
INSERT INTO order_scm.carrier_lane (
    origin_location_id, destination_postal_zone, carrier_code, service_level, transit_days,
    carrier_pickup_cutoff, shipping_cost_base, cost_per_kg, on_time_reliability
) VALUES
-- From DC-EAST (Zone 1 = Northeast, Zone 2 = Midwest, Zone 3 = West)
('DC-EAST', '10001', 'FEDEX', 'GROUND', 1, '17:30:00', 10.50, 0.50, 0.985),
('DC-EAST', '60601', 'FEDEX', 'GROUND', 2, '17:30:00', 14.50, 0.70, 0.965),
('DC-EAST', '90210', 'FEDEX', 'GROUND', 4, '17:30:00', 24.00, 1.10, 0.940),
('DC-EAST', '90210', 'UPS', 'EXPEDITED', 2, '18:00:00', 38.00, 1.50, 0.970),
-- From DC-WEST
('DC-WEST', '90210', 'UPS', 'GROUND', 1, '17:00:00', 9.50, 0.50, 0.980),
('DC-WEST', '60601', 'FEDEX', 'GROUND', 3, '17:00:00', 16.00, 0.80, 0.955),
('DC-WEST', '10001', 'FEDEX', 'GROUND', 4, '17:00:00', 25.00, 1.15, 0.935),
-- From STORE-CENTRAL
('STORE-CENTRAL', '60601', 'INTERNAL', 'SAME_DAY', 1, '15:00:00', 15.00, 0.20, 0.970),
('STORE-CENTRAL', '10001', 'UPS', 'GROUND', 2, '15:00:00', 18.00, 0.80, 0.940),
('STORE-CENTRAL', '90210', 'UPS', 'GROUND', 3, '15:00:00', 21.00, 0.90, 0.930),
-- From SUPPLIER-DIRECT
('SUPPLIER-DIRECT', '10001', 'FEDEX', 'GROUND', 3, '13:00:00', 19.00, 0.85, 0.920),
('SUPPLIER-DIRECT', '60601', 'FEDEX', 'GROUND', 2, '13:00:00', 15.00, 0.70, 0.930),
('SUPPLIER-DIRECT', '90210', 'FEDEX', 'GROUND', 2, '13:00:00', 16.00, 0.75, 0.935);

-- 5. Inventory Positions (Multi-Bucket, Multi-Source)
INSERT INTO order_scm.inventory_position (
    sku, location_id, source_type, gross_on_hand, quality_blocked, warehouse_blocked, customer_reserved,
    order_allocated, in_transit_inward, supplier_confirmed, expected_receipt_qty, expected_receipt_date,
    source_confidence_score, last_snapshot_at
) VALUES
-- DC-EAST Stock
('SKU-LAPTOP-PRO', 'DC-EAST', 'WMS_REALTIME', 85, 0, 0, 10, 15, 0, 0, 0, NULL, 1.000, NOW()),
('SKU-MONITOR-4K', 'DC-EAST', 'WMS_REALTIME', 120, 0, 0, 0, 25, 0, 0, 0, NULL, 1.000, NOW()),
('SKU-DOCK-TB4',   'DC-EAST', 'WMS_REALTIME', 250, 0, 0, 0, 40, 0, 0, 0, NULL, 1.000, NOW()),
('SKU-CABLE-TB4',  'DC-EAST', 'WMS_REALTIME', 500, 0, 0, 0, 80, 0, 0, 0, NULL, 1.000, NOW()),

-- DC-WEST Stock (Notice Laptop Pro has 0, but Laptop Plus has stock, and Monitor 4K has stock)
('SKU-LAPTOP-PRO',  'DC-WEST', 'WMS_REALTIME', 0, 0, 0, 0, 0, 50, 0, 50, CURRENT_DATE + INTERVAL '5 days', 1.000, NOW()),
('SKU-LAPTOP-PLUS', 'DC-WEST', 'WMS_REALTIME', 60, 0, 0, 5, 10, 0, 0, 0, NULL, 1.000, NOW()),
('SKU-MONITOR-4K',  'DC-WEST', 'WMS_REALTIME', 45, 0, 0, 0, 5, 0, 0, 0, NULL, 1.000, NOW()),
('SKU-DOCK-TB4',    'DC-WEST', 'WMS_REALTIME', 150, 0, 0, 0, 20, 0, 0, 0, NULL, 1.000, NOW()),
('SKU-CABLE-TB4',   'DC-WEST', 'WMS_REALTIME', 300, 0, 0, 0, 15, 0, 0, 0, NULL, 1.000, NOW()),

-- STORE-CENTRAL Stock (Lower confidence POS snapshot, smaller quantities)
('SKU-LAPTOP-PRO', 'STORE-CENTRAL', 'STORE_POS', 6, 0, 0, 0, 2, 0, 0, 0, NULL, 0.880, NOW() - INTERVAL '30 minutes'),
('SKU-MONITOR-2K', 'STORE-CENTRAL', 'STORE_POS', 12, 0, 0, 0, 1, 0, 0, 0, NULL, 0.880, NOW() - INTERVAL '30 minutes'),
('SKU-DOCK-TB4',   'STORE-CENTRAL', 'STORE_POS', 15, 0, 0, 0, 3, 0, 0, 0, NULL, 0.880, NOW() - INTERVAL '30 minutes'),

-- SUPPLIER-DIRECT (Vendor stock with CTP lead time)
('SKU-LAPTOP-PRO',  'SUPPLIER-DIRECT', 'SUPPLIER_EDI', 0, 0, 0, 0, 0, 0, 200, 200, CURRENT_DATE + INTERVAL '4 days', 0.920, NOW() - INTERVAL '2 hours'),
('SKU-BATTERY-PACK','SUPPLIER-DIRECT', 'SUPPLIER_EDI', 0, 0, 0, 0, 0, 0, 150, 150, CURRENT_DATE + INTERVAL '3 days', 0.920, NOW() - INTERVAL '2 hours');
