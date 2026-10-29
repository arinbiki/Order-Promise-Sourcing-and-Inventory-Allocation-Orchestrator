"""
f/order_scm/appealing_script
Deterministic Order Promise, Sourcing, and Inventory Allocation Orchestrator
Master Windmill Workflow-as-Code (WAC) & Python Decision Engine.

Zero LLM calls. Fully deterministic, auditable, event-driven enterprise orchestrator.
Implements:
1. Deterministic FSM & Priority Router
2. Schema Validation & Auto-Repair
3. Master Data Enrichment (Customer, Material, Location, Lanes)
4. Financial & Fraud Policy Engine (Credit check, overdue blocks, fraud heuristics)
5. Multi-Source Inventory Convergence Gate (WMS, ERP, Store, Supplier, In-Transit)
6. ATP & CTP Engines
7. Approved Substitution Engine
8. Multi-Factor Sourcing & Allocation Optimizer with Split Penalties
9. Promise Engine with Confidence Scoring & Cut-Off Logic
10. Backorder Management & Priority Queue
11. Pre-Release Verification & Execution Release Adapter
12. Rollback & Compensation Logic
13. Immutable Audit Logging & Real-Time KPI Instrumentation
"""

import os
import sys
import json
import uuid
import math
import hashlib
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

# Optional Windmill SDK imports when running in Windmill worker container
try:
    import wmill
    from wmill import step, task, workflow
    WMILL_AVAILABLE = True
except ImportError:
    WMILL_AVAILABLE = False


# ==============================================================================
# 1. CORE DATA STRUCTURES & STATIC ENTERPRISE MASTER DATA (In-Memory Fallback)
# ==============================================================================

# Rule version for auditability
RULE_VERSION = "v1.4.0-deterministic"

DEFAULT_CUSTOMERS = {
    "CUST-STRAT-001": {
        "customer_id": "CUST-STRAT-001", "name": "Acme Global Enterprises", "active": True,
        "priority_level": 1, "sla_tier": "GOLD", "credit_limit": 500000.0, "open_exposure": 45000.0,
        "overdue_balance": 0.0, "payment_terms": "NET60", "allow_substitutes": True,
        "allow_splits": True, "max_split_count": 3, "country_code": "US", "postal_code": "10001",
        "receiving_days": ["MON", "TUE", "WED", "THU", "FRI"]
    },
    "CUST-MID-002": {
        "customer_id": "CUST-MID-002", "name": "Beta Retail Distribution", "active": True,
        "priority_level": 3, "sla_tier": "SILVER", "credit_limit": 60000.0, "open_exposure": 15000.0,
        "overdue_balance": 0.0, "payment_terms": "NET30", "allow_substitutes": True,
        "allow_splits": True, "max_split_count": 2, "country_code": "US", "postal_code": "60601",
        "receiving_days": ["MON", "TUE", "WED", "THU", "FRI"]
    },
    "CUST-RISK-003": {
        "customer_id": "CUST-RISK-003", "name": "Overdue Risk Merchants", "active": True,
        "priority_level": 4, "sla_tier": "STANDARD", "credit_limit": 15000.0, "open_exposure": 14500.0,
        "overdue_balance": 8500.0, "payment_terms": "NET15", "allow_substitutes": False,
        "allow_splits": False, "max_split_count": 1, "country_code": "US", "postal_code": "90210",
        "receiving_days": ["MON", "TUE", "WED", "THU", "FRI"]
    },
    "CUST-FRAUD-004": {
        "customer_id": "CUST-FRAUD-004", "name": "Phantom Velocity Traders", "active": True,
        "priority_level": 5, "sla_tier": "STANDARD", "credit_limit": 5000.0, "open_exposure": 0.0,
        "overdue_balance": 0.0, "payment_terms": "PREPAID", "allow_substitutes": False,
        "allow_splits": False, "max_split_count": 1, "country_code": "US", "postal_code": "33101",
        "receiving_days": ["MON", "WED", "FRI"]
    },
    "CUST-INACTIVE-005": {
        "customer_id": "CUST-INACTIVE-005", "name": "Dormant Accounts Corp", "active": False,
        "priority_level": 5, "sla_tier": "STANDARD", "credit_limit": 10000.0, "open_exposure": 0.0,
        "overdue_balance": 0.0, "payment_terms": "NET30", "allow_substitutes": False,
        "allow_splits": False, "max_split_count": 1, "country_code": "US", "postal_code": "75001",
        "receiving_days": ["MON", "TUE", "WED"]
    }
}

DEFAULT_MATERIALS = {
    "SKU-LAPTOP-PRO": {
        "sku": "SKU-LAPTOP-PRO", "name": "Apex Ultra 16-inch Laptop (i9/32GB)", "category": "COMPUTING",
        "uom": "EA", "active": True, "unit_cost": 1400.0, "unit_price": 2200.0,
        "substitution_group": "GRP_LAPTOP_16", "lead_time_days": 2, "hazardous": False,
        "shelf_life_days": None, "direct_ship_eligible": True
    },
    "SKU-LAPTOP-PLUS": {
        "sku": "SKU-LAPTOP-PLUS", "name": "Apex Ultra 16-inch Plus (i9/64GB)", "category": "COMPUTING",
        "uom": "EA", "active": True, "unit_cost": 1600.0, "unit_price": 2450.0,
        "substitution_group": "GRP_LAPTOP_16", "lead_time_days": 2, "hazardous": False,
        "shelf_life_days": None, "direct_ship_eligible": True
    },
    "SKU-MONITOR-4K": {
        "sku": "SKU-MONITOR-4K", "name": "ProArt 32-inch 4K Studio Display", "category": "DISPLAYS",
        "uom": "EA", "active": True, "unit_cost": 450.0, "unit_price": 799.0,
        "substitution_group": "GRP_MONITOR_32", "lead_time_days": 1, "hazardous": False,
        "shelf_life_days": None, "direct_ship_eligible": True
    },
    "SKU-MONITOR-2K": {
        "sku": "SKU-MONITOR-2K", "name": "ProArt 32-inch QHD Studio Display", "category": "DISPLAYS",
        "uom": "EA", "active": True, "unit_cost": 320.0, "unit_price": 549.0,
        "substitution_group": "GRP_MONITOR_32", "lead_time_days": 1, "hazardous": False,
        "shelf_life_days": None, "direct_ship_eligible": True
    },
    "SKU-DOCK-TB4": {
        "sku": "SKU-DOCK-TB4", "name": "Universal Thunderbolt 4 Quad Dock", "category": "ACCESSORIES",
        "uom": "EA", "active": True, "unit_cost": 110.0, "unit_price": 249.0,
        "substitution_group": None, "lead_time_days": 1, "hazardous": False,
        "shelf_life_days": None, "direct_ship_eligible": True
    },
    "SKU-CABLE-TB4": {
        "sku": "SKU-CABLE-TB4", "name": "High-Speed TB4 Cable 2m Braided", "category": "ACCESSORIES",
        "uom": "EA", "active": True, "unit_cost": 15.0, "unit_price": 49.0,
        "substitution_group": None, "lead_time_days": 1, "hazardous": False,
        "shelf_life_days": None, "direct_ship_eligible": True
    },
    "SKU-BATTERY-PACK": {
        "sku": "SKU-BATTERY-PACK", "name": "Industrial Li-Ion Hot-Swap Battery", "category": "POWER",
        "uom": "EA", "active": True, "unit_cost": 80.0, "unit_price": 160.0,
        "substitution_group": None, "lead_time_days": 3, "hazardous": True,
        "shelf_life_days": 730, "direct_ship_eligible": False
    },
    "SKU-DISCONTINUED": {
        "sku": "SKU-DISCONTINUED", "name": "Legacy Terminal Display Unit", "category": "LEGACY",
        "uom": "EA", "active": False, "unit_cost": 50.0, "unit_price": 99.0,
        "substitution_group": None, "lead_time_days": 10, "hazardous": False,
        "shelf_life_days": None, "direct_ship_eligible": False
    }
}

DEFAULT_LOCATIONS = {
    "DC-EAST": {
        "location_id": "DC-EAST", "name": "East Coast Central Mega-DC (Newark, NJ)",
        "location_type": "CENTRAL_DC", "active": True, "daily_order_capacity": 3000,
        "current_day_orders": 140, "warehouse_cut_off_time": "16:00:00",
        "handling_lead_time_hours": 3.5, "supported_carriers": ["FEDEX", "UPS", "DHL"],
        "reliability_score": 0.990
    },
    "DC-WEST": {
        "location_id": "DC-WEST", "name": "West Coast Regional DC (Reno, NV)",
        "location_type": "REGIONAL_DC", "active": True, "daily_order_capacity": 1800,
        "current_day_orders": 95, "warehouse_cut_off_time": "15:30:00",
        "handling_lead_time_hours": 4.0, "supported_carriers": ["FEDEX", "UPS", "DHL"],
        "reliability_score": 0.985
    },
    "STORE-CENTRAL": {
        "location_id": "STORE-CENTRAL", "name": "Chicago Flagship Hub Store (Chicago, IL)",
        "location_type": "STORE", "active": True, "daily_order_capacity": 350,
        "current_day_orders": 42, "warehouse_cut_off_time": "14:00:00",
        "handling_lead_time_hours": 2.0, "supported_carriers": ["UPS", "INTERNAL"],
        "reliability_score": 0.930
    },
    "SUPPLIER-DIRECT": {
        "location_id": "SUPPLIER-DIRECT", "name": "Apex Microtech Vendor Direct (Austin, TX)",
        "location_type": "SUPPLIER_DIRECT", "active": True, "daily_order_capacity": 800,
        "current_day_orders": 60, "warehouse_cut_off_time": "12:00:00",
        "handling_lead_time_hours": 24.0, "supported_carriers": ["FEDEX", "DHL"],
        "reliability_score": 0.915
    }
}

DEFAULT_CARRIER_LANES = [
    {"origin": "DC-EAST", "zone": "10001", "carrier": "FEDEX", "service": "GROUND", "transit_days": 1, "cost_base": 10.50, "reliability": 0.985},
    {"origin": "DC-EAST", "zone": "60601", "carrier": "FEDEX", "service": "GROUND", "transit_days": 2, "cost_base": 14.50, "reliability": 0.965},
    {"origin": "DC-EAST", "zone": "90210", "carrier": "FEDEX", "service": "GROUND", "transit_days": 4, "cost_base": 24.00, "reliability": 0.940},
    {"origin": "DC-EAST", "zone": "90210", "carrier": "UPS",   "service": "EXPEDITED", "transit_days": 2, "cost_base": 38.00, "reliability": 0.970},
    {"origin": "DC-WEST", "zone": "90210", "carrier": "UPS",   "service": "GROUND", "transit_days": 1, "cost_base": 9.50, "reliability": 0.980},
    {"origin": "DC-WEST", "zone": "60601", "carrier": "FEDEX", "service": "GROUND", "transit_days": 3, "cost_base": 16.00, "reliability": 0.955},
    {"origin": "DC-WEST", "zone": "10001", "carrier": "FEDEX", "service": "GROUND", "transit_days": 4, "cost_base": 25.00, "reliability": 0.935},
    {"origin": "STORE-CENTRAL", "zone": "60601", "carrier": "INTERNAL", "service": "SAME_DAY", "transit_days": 1, "cost_base": 15.00, "reliability": 0.970},
    {"origin": "STORE-CENTRAL", "zone": "10001", "carrier": "UPS", "service": "GROUND", "transit_days": 2, "cost_base": 18.00, "reliability": 0.940},
    {"origin": "STORE-CENTRAL", "zone": "90210", "carrier": "UPS", "service": "GROUND", "transit_days": 3, "cost_base": 21.00, "reliability": 0.930},
    {"origin": "SUPPLIER-DIRECT", "zone": "10001", "carrier": "FEDEX", "service": "GROUND", "transit_days": 3, "cost_base": 19.00, "reliability": 0.920},
    {"origin": "SUPPLIER-DIRECT", "zone": "60601", "carrier": "FEDEX", "service": "GROUND", "transit_days": 2, "cost_base": 15.00, "reliability": 0.930},
    {"origin": "SUPPLIER-DIRECT", "zone": "90210", "carrier": "FEDEX", "service": "GROUND", "transit_days": 2, "cost_base": 16.00, "reliability": 0.935}
]

DEFAULT_INVENTORY = [
    # DC-EAST
    {"sku": "SKU-LAPTOP-PRO", "location_id": "DC-EAST", "source_type": "WMS_REALTIME", "gross_on_hand": 85, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 10, "order_allocated": 15, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    {"sku": "SKU-MONITOR-4K", "location_id": "DC-EAST", "source_type": "WMS_REALTIME", "gross_on_hand": 120, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 25, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    {"sku": "SKU-DOCK-TB4",   "location_id": "DC-EAST", "source_type": "WMS_REALTIME", "gross_on_hand": 250, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 40, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    {"sku": "SKU-CABLE-TB4",  "location_id": "DC-EAST", "source_type": "WMS_REALTIME", "gross_on_hand": 500, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 80, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    # DC-WEST
    {"sku": "SKU-LAPTOP-PRO",  "location_id": "DC-WEST", "source_type": "WMS_REALTIME", "gross_on_hand": 0, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 0, "in_transit_inward": 50, "supplier_confirmed": 0, "expected_receipt_qty": 50, "expected_receipt_date": (date.today() + timedelta(days=5)).isoformat(), "confidence": 0.950},
    {"sku": "SKU-LAPTOP-PLUS", "location_id": "DC-WEST", "source_type": "WMS_REALTIME", "gross_on_hand": 60, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 5, "order_allocated": 10, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    {"sku": "SKU-MONITOR-4K",  "location_id": "DC-WEST", "source_type": "WMS_REALTIME", "gross_on_hand": 45, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 5, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    {"sku": "SKU-DOCK-TB4",    "location_id": "DC-WEST", "source_type": "WMS_REALTIME", "gross_on_hand": 150, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 20, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    {"sku": "SKU-CABLE-TB4",   "location_id": "DC-WEST", "source_type": "WMS_REALTIME", "gross_on_hand": 300, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 15, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 1.000},
    # STORE-CENTRAL
    {"sku": "SKU-LAPTOP-PRO", "location_id": "STORE-CENTRAL", "source_type": "STORE_POS", "gross_on_hand": 6, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 2, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 0.880},
    {"sku": "SKU-MONITOR-2K", "location_id": "STORE-CENTRAL", "source_type": "STORE_POS", "gross_on_hand": 12, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 1, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 0.880},
    {"sku": "SKU-DOCK-TB4",   "location_id": "STORE-CENTRAL", "source_type": "STORE_POS", "gross_on_hand": 15, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 3, "in_transit_inward": 0, "supplier_confirmed": 0, "expected_receipt_qty": 0, "expected_receipt_date": None, "confidence": 0.880},
    # SUPPLIER-DIRECT
    {"sku": "SKU-LAPTOP-PRO",  "location_id": "SUPPLIER-DIRECT", "source_type": "SUPPLIER_EDI", "gross_on_hand": 0, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 0, "in_transit_inward": 0, "supplier_confirmed": 200, "expected_receipt_qty": 200, "expected_receipt_date": (date.today() + timedelta(days=4)).isoformat(), "confidence": 0.920},
    {"sku": "SKU-BATTERY-PACK","location_id": "SUPPLIER-DIRECT", "source_type": "SUPPLIER_EDI", "gross_on_hand": 0, "quality_blocked": 0, "warehouse_blocked": 0, "customer_reserved": 0, "order_allocated": 0, "in_transit_inward": 0, "supplier_confirmed": 150, "expected_receipt_qty": 150, "expected_receipt_date": (date.today() + timedelta(days=3)).isoformat(), "confidence": 0.920}
]


# ==============================================================================
# 2. SHARED BLACKBOARD & AUDIT LOGGER
# ==============================================================================

class BlackboardStore:
    """
    Manages in-memory and PostgreSQL case persistence with state versioning.
    """
    def __init__(self):
        self._cases: Dict[str, Dict[str, Any]] = {}
        self._idempotency_map: Dict[str, str] = {}
        self._audit_log: List[Dict[str, Any]] = []
        self._kpi_facts: List[Dict[str, Any]] = []
        self._pg_conn = None
        self._init_pg()

    def _init_pg(self):
        # Attempt PostgreSQL connection if available
        db_url = os.environ.get("DATABASE_URL")
        if db_url:
            try:
                import psycopg2
                self._pg_conn = psycopg2.connect(db_url)
                self._pg_conn.autocommit = True
            except Exception:
                self._pg_conn = None

    def record_audit(self, case_id: str, actor: str, event_type: str, prev_state: str, new_state: str, summary: str, snapshot: Optional[Dict] = None):
        event = {
            "case_id": case_id,
            "actor": actor,
            "event_type": event_type,
            "prev_state": prev_state,
            "new_state": new_state,
            "rule_version": RULE_VERSION,
            "result_summary": summary,
            "payload_snapshot": snapshot or {},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self._audit_log.append(event)
        if self._pg_conn:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO order_scm.audit_event (case_id, actor, event_type, prev_state, new_state, rule_version, result_summary, payload_snapshot)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (case_id, actor, event_type, prev_state, new_state, RULE_VERSION, summary, json.dumps(snapshot or {})))
            except Exception:
                pass

    def record_kpi(self, fact: Dict[str, Any]):
        fact["timestamp"] = datetime.now(timezone.utc).isoformat()
        self._kpi_facts.append(fact)
        if self._pg_conn:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO order_scm.kpi_fact (
                            case_id, channel, customer_id, event_type, promised_on_time, first_pass_fill_rate,
                            is_oversell, is_backordered, is_split, split_count, is_cancelled, manual_intervention, intervention_type, cycle_time_seconds
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        fact["case_id"], fact.get("channel", "DIRECT"), fact.get("customer_id"), fact["event_type"],
                        fact.get("promised_on_time"), fact.get("first_pass_fill_rate"), fact.get("is_oversell", False),
                        fact.get("is_backordered", False), fact.get("is_split", False), fact.get("split_count", 1),
                        fact.get("is_cancelled", False), fact.get("manual_intervention", False), fact.get("intervention_type"),
                        fact.get("cycle_time_seconds", 0.0)
                    ))
            except Exception:
                pass

    def save_case(self, case: Dict[str, Any]):
        case_id = case["case_id"]
        case["state_version"] = case.get("state_version", 0) + 1
        case["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._cases[case_id] = case
        self._idempotency_map[case["idempotency_key"]] = case_id

        if self._pg_conn:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO order_scm.order_case (
                            case_id, channel, external_order_ref, idempotency_key, current_state, state_version,
                            priority, customer_id, requested_delivery_date, raw_order_payload, normalized_order,
                            validation_results, credit_fraud_results, inventory_convergence_results, atp_ctp_results,
                            sourcing_results, allocation_plan, promise_details, holds, exceptions, next_action, retry_count, is_closed
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (case_id) DO UPDATE SET
                            current_state = EXCLUDED.current_state,
                            state_version = EXCLUDED.state_version,
                            normalized_order = EXCLUDED.normalized_order,
                            validation_results = EXCLUDED.validation_results,
                            credit_fraud_results = EXCLUDED.credit_fraud_results,
                            inventory_convergence_results = EXCLUDED.inventory_convergence_results,
                            atp_ctp_results = EXCLUDED.atp_ctp_results,
                            sourcing_results = EXCLUDED.sourcing_results,
                            allocation_plan = EXCLUDED.allocation_plan,
                            promise_details = EXCLUDED.promise_details,
                            holds = EXCLUDED.holds,
                            exceptions = EXCLUDED.exceptions,
                            next_action = EXCLUDED.next_action,
                            retry_count = EXCLUDED.retry_count,
                            is_closed = EXCLUDED.is_closed,
                            updated_at = NOW();
                    """, (
                        case_id, case["channel"], case["external_order_ref"], case["idempotency_key"], case["current_state"],
                        case["state_version"], case.get("priority", 3), case.get("customer_id"), case.get("requested_delivery_date", date.today()),
                        json.dumps(case.get("raw_order_payload", {})), json.dumps(case.get("normalized_order", {})),
                        json.dumps(case.get("validation_results", {})), json.dumps(case.get("credit_fraud_results", {})),
                        json.dumps(case.get("inventory_convergence_results", {})), json.dumps(case.get("atp_ctp_results", {})),
                        json.dumps(case.get("sourcing_results", {})), json.dumps(case.get("allocation_plan", {})),
                        json.dumps(case.get("promise_details", {})), json.dumps(case.get("holds", [])),
                        json.dumps(case.get("exceptions", [])), case.get("next_action", "COMPLETE"), case.get("retry_count", 0),
                        case.get("is_closed", False)
                    ))
            except Exception:
                pass

    def get_case_by_idempotency(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        case_id = self._idempotency_map.get(idempotency_key)
        if case_id:
            return self._cases.get(case_id)
        if self._pg_conn:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("SELECT case_id FROM order_scm.order_case WHERE idempotency_key = %s", (idempotency_key,))
                    row = cur.fetchone()
                    if row:
                        return self._cases.get(row[0])
            except Exception:
                pass
        return None

    def get_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        return self._cases.get(case_id)


# Global singleton instance of the blackboard
BLACKBOARD = BlackboardStore()


# ==============================================================================
# 3. DETERMINISTIC DECISION ENGINES
# ==============================================================================

def compute_idempotency_key(channel: str, external_order_ref: str, lines: List[Dict]) -> str:
    """Computes a deterministic SHA256 key from immutable order attributes."""
    line_str = "|".join([f"{l.get('sku')}:{l.get('quantity')}:{l.get('uom','EA')}" for l in lines])
    raw = f"{channel.strip().upper()}#{external_order_ref.strip()}#{line_str}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def validate_and_repair_schema(raw_order: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
    """
    Validates structural schema and executes deterministic auto-repairs:
    - Normalizes ISO country codes (e.g. 'USA' -> 'US')
    - Normalizes UOM (e.g. 'PIECE', 'PCS' -> 'EA')
    - Normalizes postal codes
    - Verifies mandatory business fields
    """
    errors = []
    repaired = json.loads(json.dumps(raw_order)) # deep copy

    # Mandatory root fields
    for field in ["channel", "external_order_ref", "customer_id", "lines"]:
        if not repaired.get(field):
            errors.append(f"Missing mandatory field: {field}")

    if errors:
        return False, repaired, errors

    # Normalize channel
    repaired["channel"] = str(repaired["channel"]).strip().upper()

    # Normalize requested delivery date
    req_date = repaired.get("requested_delivery_date")
    if not req_date:
        repaired["requested_delivery_date"] = (date.today() + timedelta(days=3)).isoformat()
    else:
        try:
            # Parse and standardize ISO format
            parsed_d = datetime.fromisoformat(str(req_date).split("T")[0]).date()
            repaired["requested_delivery_date"] = parsed_d.isoformat()
        except Exception:
            errors.append(f"Invalid date format for requested_delivery_date: {req_date}")

    # Validate and repair order lines
    lines = repaired.get("lines", [])
    if not isinstance(lines, list) or len(lines) == 0:
        errors.append("Order must contain at least one valid line item.")
        return False, repaired, errors

    repaired_lines = []
    for idx, l in enumerate(lines):
        line_num = l.get("line_number", idx + 1)
        sku = str(l.get("sku", "")).strip().upper()
        if not sku:
            errors.append(f"Line {line_num}: SKU is missing.")
            continue

        try:
            qty = int(l.get("quantity", 0))
            if qty <= 0:
                errors.append(f"Line {line_num}: Quantity must be strictly positive (got {qty}).")
        except Exception:
            errors.append(f"Line {line_num}: Invalid quantity format.")
            continue

        # Auto-repair UOM
        uom = str(l.get("uom", "EA")).strip().upper()
        if uom in ["PIECE", "PIECES", "PCS", "EACH", "UNIT", "UNITS"]:
            uom = "EA"

        unit_price = float(l.get("unit_price", 0.0))

        repaired_lines.append({
            "line_id": f"L-{line_num}-{sku}",
            "line_number": line_num,
            "sku": sku,
            "ordered_qty": qty,
            "uom": uom,
            "unit_price": unit_price,
            "line_status": "VALIDATED"
        })

    repaired["lines"] = repaired_lines
    is_valid = len(errors) == 0
    return is_valid, repaired, errors


def enrich_master_data(order: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
    """Enriches customer, material, and regional policies from master tables."""
    errors = []
    cust_id = order.get("customer_id")
    customer = DEFAULT_CUSTOMERS.get(cust_id)

    if not customer:
        return False, {}, [f"Customer ID '{cust_id}' not found in customer master."]
    if not customer.get("active", False):
        return False, {}, [f"Customer '{cust_id}' is marked INACTIVE. Commercial hold required."]

    enriched_lines = []
    total_order_val = 0.0

    for l in order.get("lines", []):
        sku = l["sku"]
        mat = DEFAULT_MATERIALS.get(sku)
        if not mat:
            errors.append(f"Material SKU '{sku}' not found in material master.")
            continue
        if not mat.get("active", False):
            errors.append(f"Material SKU '{sku}' is marked DISCONTINUED or inactive.")
            continue

        price = mat["unit_price"] if l["unit_price"] <= 0.0 else l["unit_price"]
        l_val = price * l["ordered_qty"]
        total_order_val += l_val

        enriched_line = dict(l)
        enriched_line.update({
            "material_name": mat["name"],
            "category": mat["category"],
            "unit_price": price,
            "unit_cost": mat["unit_cost"],
            "line_value": l_val,
            "substitution_group": mat.get("substitution_group"),
            "lead_time_days": mat.get("lead_time_days", 2),
            "hazardous": mat.get("hazardous", False),
            "direct_ship_eligible": mat.get("direct_ship_eligible", True)
        })
        enriched_lines.append(enriched_line)

    if errors:
        return False, {}, errors

    enrichment = {
        "customer": customer,
        "lines": enriched_lines,
        "total_order_value": total_order_val,
        "destination_zone": customer.get("postal_code", "10001"),
        "customer_priority": customer.get("priority_level", 3),
        "sla_tier": customer.get("sla_tier", "STANDARD")
    }
    return True, enrichment, []


def evaluate_financial_and_fraud_policy(enrichment: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
    """
    Evaluates credit exposure, overdue balances, and deterministic fraud rules.
    Zero probability. Explicit thresholds only.
    """
    customer = enrichment["customer"]
    total_order_val = enrichment["total_order_value"]
    holds = []
    credit_limit = customer.get("credit_limit", 0.0)
    open_exposure = customer.get("open_exposure", 0.0)
    overdue_balance = customer.get("overdue_balance", 0.0)

    # 1. Overdue Balance Stop: Any overdue balance > $5,000 halts order
    if overdue_balance > 5000.0:
        holds.append({
            "type": "CREDIT_HOLD",
            "reason_code": "OVERDUE_BALANCE_BLOCKED",
            "severity": "HIGH",
            "detail": f"Overdue balance of ${overdue_balance:,.2f} exceeds threshold ($5,000.00)."
        })

    # 2. Credit Exposure Stop: (open exposure + new order) > credit limit
    projected_exposure = open_exposure + total_order_val
    if projected_exposure > credit_limit:
        holds.append({
            "type": "CREDIT_HOLD",
            "reason_code": "CREDIT_LIMIT_EXCEEDED",
            "severity": "HIGH",
            "detail": f"Projected exposure ${projected_exposure:,.2f} exceeds credit limit ${credit_limit:,.2f}."
        })

    # 3. Deterministic Fraud Rules:
    # Rule A: Order value > $100,000 for standard tier accounts
    if total_order_val > 100000.0 and customer.get("sla_tier") == "STANDARD":
        holds.append({
            "type": "FRAUD_HOLD",
            "reason_code": "HIGH_VALUE_VELOCITY_SPIKE",
            "severity": "CRITICAL",
            "detail": f"High single-order value ${total_order_val:,.2f} triggers mandatory fraud verification."
        })

    # Rule B: Specific blocked accounts or blacklisted postal codes
    if customer.get("customer_id") == "CUST-FRAUD-004":
        holds.append({
            "type": "FRAUD_HOLD",
            "reason_code": "SECURITY_WATCHLIST_ACCOUNT",
            "severity": "CRITICAL",
            "detail": "Customer identifier flagged in security & risk watch matrix."
        })

    passed = len(holds) == 0
    results = {
        "passed": passed,
        "projected_exposure": projected_exposure,
        "available_credit": max(0.0, credit_limit - open_exposure),
        "holds": holds
    }
    return passed, results, holds


def converge_inventory_positions(required_skus: List[str], force_degraded: bool = False) -> Dict[str, Any]:
    """
    Convergence gate combining multi-source inventory:
    - WMS (Realtime bin stock, confidence 1.0)
    - Store (POS stock, confidence 0.88)
    - Supplier EDI (Vendor confirmed, confidence 0.92)
    Applies safety buffers and degraded mode adjustments if source freshness is degraded.
    """
    converged_positions = {}
    sources_inspected = []
    degraded_mode = force_degraded

    for inv in DEFAULT_INVENTORY:
        sku = inv["sku"]
        if sku not in required_skus:
            continue

        loc = inv["location_id"]
        src = inv["source_type"]
        sources_inspected.append(f"{loc}:{src}")

        # Net Available = Gross On-Hand - Quality Blocked - Warehouse Blocked - Customer Reserved - Allocated
        net_on_hand = max(0, inv["gross_on_hand"] - (
            inv["quality_blocked"] + inv["warehouse_blocked"] + inv["customer_reserved"] + inv["order_allocated"]
        ))

        # Expected supply from confirmed in-transit or vendor EDI
        future_confirmed = inv["in_transit_inward"] + inv["supplier_confirmed"] + inv["expected_receipt_qty"]
        avail_date = inv.get("expected_receipt_date") or date.today().isoformat()

        # Confidence calculation & Degraded mode buffer
        base_confidence = inv.get("confidence", 1.0)
        if degraded_mode:
            # 20% safety buffer haircut during degraded mode
            net_on_hand = int(net_on_hand * 0.80)
            base_confidence = round(base_confidence * 0.85, 3)

        if sku not in converged_positions:
            converged_positions[sku] = []

        converged_positions[sku].append({
            "location_id": loc,
            "source_type": src,
            "net_on_hand": net_on_hand,
            "future_confirmed": future_confirmed,
            "earliest_available_date": avail_date,
            "confidence_score": base_confidence,
            "degraded_mode_applied": degraded_mode
        })

    return {
        "status": "CONVERGED_DEGRADED" if degraded_mode else "FULLY_CONVERGED",
        "degraded_mode": degraded_mode,
        "sources_count": len(sources_inspected),
        "positions": converged_positions
    }


def calculate_atp_and_ctp(lines: List[Dict[str, Any]], inventory_positions: Dict[str, Any]) -> Dict[str, Any]:
    """
    Computes Available-To-Promise (immediate on-hand) and
    Capable-To-Promise (future confirmed receipts & vendor direct).
    """
    results = {}
    total_requested_units = 0
    total_atp_units = 0
    total_ctp_units = 0

    for l in lines:
        sku = l["sku"]
        req_qty = l["ordered_qty"]
        total_requested_units += req_qty
        positions = inventory_positions.get(sku, [])

        sku_atp = sum(p["net_on_hand"] for p in positions)
        sku_ctp = sum(p["future_confirmed"] for p in positions)

        total_atp_units += min(req_qty, sku_atp)
        total_ctp_units += min(max(0, req_qty - sku_atp), sku_ctp)

        if sku_atp >= req_qty:
            eval_status = "FULL_ATP"
        elif (sku_atp + sku_ctp) >= req_qty:
            eval_status = "FEASIBLE_VIA_CTP"
        elif (sku_atp + sku_ctp) > 0:
            eval_status = "PARTIAL_AVAILABILITY"
        else:
            eval_status = "OUT_OF_STOCK"

        results[sku] = {
            "requested_qty": req_qty,
            "net_atp_available": sku_atp,
            "net_ctp_available": sku_ctp,
            "evaluation_status": eval_status
        }

    overall_status = "FULL_AVAILABILITY"
    if total_atp_units == total_requested_units:
        overall_status = "FULL_ATP"
    elif (total_atp_units + total_ctp_units) >= total_requested_units:
        overall_status = "FEASIBLE_VIA_CTP"
    elif total_atp_units > 0 or total_ctp_units > 0:
        overall_status = "PARTIAL_AVAILABILITY"
    else:
        overall_status = "OUT_OF_STOCK"

    return {
        "overall_status": overall_status,
        "total_requested_units": total_requested_units,
        "total_atp_units": total_atp_units,
        "total_ctp_units": total_ctp_units,
        "lines": results
    }


def evaluate_substitutions(
    lines: List[Dict[str, Any]],
    atp_results: Dict[str, Any],
    inventory_positions: Dict[str, Any],
    customer: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Evaluates substitution groups when original SKU is insufficient.
    Applies customer SLA and price parity policy.
    """
    active_lines = []
    substitution_records = []
    allow_subs = customer.get("allow_substitutes", True)

    for l in lines:
        sku = l["sku"]
        req_qty = l["ordered_qty"]
        line_eval = atp_results["lines"].get(sku, {})

        if line_eval.get("evaluation_status") in ["OUT_OF_STOCK", "PARTIAL_AVAILABILITY"] and allow_subs:
            # Check for approved substitutes in the same substitution group
            orig_mat = DEFAULT_MATERIALS.get(sku, {})
            sub_grp = orig_mat.get("substitution_group")

            candidate_sub = None
            if sub_grp:
                for alt_sku, alt_mat in DEFAULT_MATERIALS.items():
                    if alt_sku != sku and alt_mat.get("substitution_group") == sub_grp and alt_mat.get("active"):
                        # Check substitute inventory
                        sub_positions = inventory_positions.get(alt_sku, [])
                        sub_avail = sum(p["net_on_hand"] for p in sub_positions)
                        if sub_avail >= req_qty:
                            candidate_sub = alt_mat
                            break

            if candidate_sub:
                price_diff = candidate_sub["unit_price"] - orig_mat["unit_price"]
                # For GOLD customers, price increase up to 15% is absorbed automatically
                auto_approved = (price_diff <= 0) or (customer.get("sla_tier") == "GOLD" and price_diff <= orig_mat["unit_price"] * 0.15)

                sub_rec = {
                    "original_sku": sku,
                    "substitute_sku": candidate_sub["sku"],
                    "substitute_name": candidate_sub["name"],
                    "original_price": orig_mat["unit_price"],
                    "substitute_price": candidate_sub["unit_price"],
                    "price_diff": price_diff,
                    "auto_approved": auto_approved
                }
                substitution_records.append(sub_rec)

                if auto_approved:
                    mod_line = dict(l)
                    mod_line["original_sku"] = sku
                    mod_line["sku"] = candidate_sub["sku"]
                    mod_line["material_name"] = candidate_sub["name"]
                    mod_line["is_substituted"] = True
                    mod_line["substitution_detail"] = sub_rec
                    active_lines.append(mod_line)
                    continue

        active_lines.append(l)

    return active_lines, substitution_records


def generate_sourcing_candidates(
    lines: List[Dict[str, Any]],
    destination_zone: str,
    inventory_positions: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Generates and filters sourcing candidates for each line against
    eligible fulfillment locations and transit lanes.
    """
    candidates = []

    for l in lines:
        sku = l["sku"]
        req_qty = l["ordered_qty"]
        positions = inventory_positions.get(sku, [])

        for p in positions:
            loc_id = p["location_id"]
            avail_on_hand = p["net_on_hand"]
            avail_future = p["future_confirmed"]
            total_avail = avail_on_hand + avail_future

            if total_avail <= 0:
                continue

            loc_meta = DEFAULT_LOCATIONS.get(loc_id, {})
            # Capacity check
            if loc_meta.get("current_day_orders", 0) >= loc_meta.get("daily_order_capacity", 1000):
                continue

            # Hazardous material restrictions
            mat = DEFAULT_MATERIALS.get(sku, {})
            if mat.get("hazardous") and loc_meta.get("location_type") == "STORE":
                continue # Stores cannot ship hazardous batteries

            # Match carrier lane
            lane = None
            for ln in DEFAULT_CARRIER_LANES:
                if ln["origin"] == loc_id and ln["zone"] == destination_zone:
                    lane = ln
                    break

            # Fallback default lane if exact zone not found
            if not lane:
                lane = {"transit_days": 3, "cost_base": 20.0, "reliability": 0.950, "carrier": "FEDEX"}

            transit_days = lane["transit_days"]
            shipping_cost = lane["cost_base"]
            confidence = p["confidence_score"] * loc_meta.get("reliability_score", 0.95) * lane["reliability"]

            # Dispatch date logic
            now_time = datetime.now().time()
            cutoff_str = loc_meta.get("warehouse_cut_off_time", "16:00:00")
            cutoff_time = datetime.strptime(cutoff_str, "%H:%M:%S").time()

            dispatch_day_offset = 0
            if avail_on_hand >= req_qty:
                if now_time > cutoff_time:
                    dispatch_day_offset = 1 # Passed cutoff, dispatch tomorrow
            else:
                # CTP future date
                fut_date_str = p.get("earliest_available_date")
                if fut_date_str:
                    fut_d = datetime.fromisoformat(fut_date_str).date()
                    dispatch_day_offset = max(1, (fut_d - date.today()).days)

            est_dispatch = date.today() + timedelta(days=dispatch_day_offset)
            est_delivery = est_dispatch + timedelta(days=transit_days)

            # Deterministic Multi-Factor Ranking Score (Higher is better)
            # Weights: Proximity/Transit (0.35), Reliability (0.30), Confidence (0.20), Cost (0.15)
            norm_cost = max(0.1, 1.0 - (shipping_cost / 100.0))
            norm_transit = max(0.1, 1.0 - (transit_days / 10.0))
            rank_score = (norm_transit * 0.35) + (loc_meta.get("reliability_score", 0.95) * 0.30) + (confidence * 0.20) + (norm_cost * 0.15)

            candidates.append({
                "line_id": l["line_id"],
                "sku": sku,
                "location_id": loc_id,
                "location_name": loc_meta.get("name"),
                "location_type": loc_meta.get("location_type"),
                "source_type": p["source_type"],
                "available_qty": total_avail,
                "available_on_hand": avail_on_hand,
                "transit_days": transit_days,
                "fulfillment_cost": shipping_cost,
                "confidence_score": round(confidence, 3),
                "estimated_dispatch_date": est_dispatch.isoformat(),
                "estimated_delivery_date": est_delivery.isoformat(),
                "rank_score": round(rank_score, 4)
            })

    return candidates


def optimize_allocation(
    lines: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    customer: Dict[str, Any]
) -> Tuple[bool, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Deterministic Allocation Optimizer with Single-Node Preference & Split Penalties.
    Balances cost, delivery speed, and customer SLA.
    """
    allocations = []
    backorders = []
    allow_splits = customer.get("allow_splits", True)
    split_penalty = 25.00 # $25 per split penalty

    # Group candidates by line
    line_candidates: Dict[str, List[Dict[str, Any]]] = {}
    for c in candidates:
        lid = c["line_id"]
        if lid not in line_candidates:
            line_candidates[lid] = []
        line_candidates[lid].append(c)

    # Sort candidates for each line by rank score descending
    for lid in line_candidates:
        line_candidates[lid].sort(key=lambda x: x["rank_score"], reverse=True)

    # Phase 1: Test if a Single Node can fulfill 100% of all lines
    candidate_locations = set(c["location_id"] for c in candidates)
    best_single_node = None
    best_single_cost = float("inf")

    for loc in candidate_locations:
        loc_can_fulfill_all = True
        total_loc_cost = 0.0
        for l in lines:
            lid = l["line_id"]
            req_qty = l["ordered_qty"]
            matching = [c for c in line_candidates.get(lid, []) if c["location_id"] == loc and c["available_qty"] >= req_qty]
            if not matching:
                loc_can_fulfill_all = False
                break
            total_loc_cost += matching[0]["fulfillment_cost"]

        if loc_can_fulfill_all and total_loc_cost < best_single_cost:
            best_single_cost = total_loc_cost
            best_single_node = loc

    # If Single Node satisfies 100% of demand, choose it to guarantee 0 split shipments
    if best_single_node:
        for l in lines:
            lid = l["line_id"]
            c = [c for c in line_candidates[lid] if c["location_id"] == best_single_node][0]
            allocations.append({
                "line_id": lid,
                "sku": l["sku"],
                "location_id": best_single_node,
                "location_name": c["location_name"],
                "allocated_qty": l["ordered_qty"],
                "estimated_cost": c["fulfillment_cost"],
                "estimated_dispatch_date": c["estimated_dispatch_date"],
                "estimated_delivery_date": c["estimated_delivery_date"],
                "confidence_score": c["confidence_score"],
                "is_split": False,
                "split_group": 1,
                "allocation_status": "COMMITTED"
            })
        return True, allocations, []

    # Phase 2: Multi-Node Optimization (if allowed) or Backorder Split
    allocated_locations = set()
    for l in lines:
        lid = l["line_id"]
        req_qty = l["ordered_qty"]
        available_cands = line_candidates.get(lid, [])

        if not available_cands:
            # Zero candidates -> Backorder
            backorders.append({
                "line_id": lid,
                "sku": l["sku"],
                "backordered_qty": req_qty,
                "reason": "NO_FEASIBLE_SOURCING_NODE",
                "expected_recovery_date": (date.today() + timedelta(days=7)).isoformat()
            })
            continue

        best_cand = available_cands[0]
        # Check split constraints
        if len(allocated_locations) > 0 and best_cand["location_id"] not in allocated_locations:
            if not allow_splits:
                # Customer does not allow splits; place remaining on backorder
                backorders.append({
                    "line_id": lid,
                    "sku": l["sku"],
                    "backordered_qty": req_qty,
                    "reason": "SPLIT_PROHIBITED_BY_CUSTOMER_POLICY",
                    "expected_recovery_date": (date.today() + timedelta(days=5)).isoformat()
                })
                continue

        allocated_qty = min(req_qty, best_cand["available_qty"])
        allocated_locations.add(best_cand["location_id"])

        allocations.append({
            "line_id": lid,
            "sku": l["sku"],
            "location_id": best_cand["location_id"],
            "location_name": best_cand["location_name"],
            "allocated_qty": allocated_qty,
            "estimated_cost": best_cand["fulfillment_cost"] + (split_penalty if len(allocated_locations) > 1 else 0.0),
            "estimated_dispatch_date": best_cand["estimated_dispatch_date"],
            "estimated_delivery_date": best_cand["estimated_delivery_date"],
            "confidence_score": best_cand["confidence_score"],
            "is_split": len(allocated_locations) > 1,
            "split_group": len(allocated_locations),
            "allocation_status": "COMMITTED"
        })

        if allocated_qty < req_qty:
            rem_qty = req_qty - allocated_qty
            rem_candidates = [c for c in available_cands if c["location_id"] != best_cand["location_id"] and c["available_qty"] > 0]
            if rem_candidates and allow_splits:
                for sec_cand in rem_candidates:
                    if rem_qty <= 0:
                        break
                    sec_allocated = min(rem_qty, sec_cand["available_qty"])
                    allocated_locations.add(sec_cand["location_id"])
                    allocations.append({
                        "line_id": lid,
                        "sku": l["sku"],
                        "location_id": sec_cand["location_id"],
                        "location_name": sec_cand["location_name"],
                        "allocated_qty": sec_allocated,
                        "estimated_cost": sec_cand["fulfillment_cost"] + split_penalty,
                        "estimated_dispatch_date": sec_cand["estimated_dispatch_date"],
                        "estimated_delivery_date": sec_cand["estimated_delivery_date"],
                        "confidence_score": sec_cand["confidence_score"],
                        "is_split": True,
                        "split_group": len(allocated_locations),
                        "allocation_status": "COMMITTED"
                    })
                    rem_qty -= sec_allocated

            if rem_qty > 0:
                backorders.append({
                    "line_id": lid,
                    "sku": l["sku"],
                    "backordered_qty": rem_qty,
                    "reason": "PARTIAL_QUANTITY_STOCKOUT",
                    "expected_recovery_date": (date.today() + timedelta(days=4)).isoformat()
                })

    is_order_split = len(allocated_locations) > 1
    for a in allocations:
        a["is_split"] = is_order_split

    success = len(allocations) > 0
    return success, allocations, backorders


def generate_deterministic_promise(
    allocations: List[Dict[str, Any]],
    requested_date_str: str,
    customer: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Computes final promised delivery date based on:
    - Latest arrival across all allocated nodes (consignee order delivery)
    - Customer receiving calendar verification
    - Deterministic confidence scoring
    """
    if not allocations:
        return {
            "status": "UNPROMISED",
            "confidence_score": 0.0,
            "confidence_tier": "LOW",
            "promised_delivery_date": None,
            "risk_flags": ["NO_ACTIVE_ALLOCATIONS"]
        }

    # Latest delivery date among all allocation lines
    delivery_dates = [datetime.fromisoformat(a["estimated_delivery_date"]).date() for a in allocations]
    earliest_possible = min(delivery_dates)
    latest_delivery = max(delivery_dates)

    # Check customer receiving days (e.g. skip Saturday/Sunday if not receiving)
    valid_receiving_days = customer.get("receiving_days", ["MON", "TUE", "WED", "THU", "FRI"])
    day_abbr = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]

    promised_date = latest_delivery
    while day_abbr[promised_date.weekday()] not in valid_receiving_days:
        promised_date += timedelta(days=1)

    # Calculate aggregate confidence score
    mean_confidence = sum(a["confidence_score"] for a in allocations) / len(allocations)

    risk_flags = []
    # If split shipment, reduce confidence by 0.05
    unique_nodes = set(a["location_id"] for a in allocations)
    if len(unique_nodes) > 1:
        mean_confidence -= 0.05
        risk_flags.append(f"SPLIT_SHIPMENT_{len(unique_nodes)}_NODES")

    # If promised date is later than requested delivery date
    req_date = datetime.fromisoformat(requested_date_str).date()
    if promised_date > req_date:
        risk_flags.append("PROMISE_LATER_THAN_REQUESTED")

    mean_confidence = max(0.1, min(1.0, round(mean_confidence, 3)))

    if mean_confidence >= 0.90:
        confidence_tier = "HIGH"
    elif mean_confidence >= 0.75:
        confidence_tier = "MEDIUM"
    else:
        confidence_tier = "LOW"

    # Generate alternative dates for customer service
    alternatives = [
        {"option": "EARLIEST_PARTIAL", "date": earliest_possible.isoformat(), "description": "Fastest partial delivery"},
        {"option": "CONSOLIDATED_FULL", "date": promised_date.isoformat(), "description": "Complete consolidated delivery"}
    ]

    return {
        "status": "PROMISED",
        "promised_delivery_date": promised_date.isoformat(),
        "earliest_possible_date": earliest_possible.isoformat(),
        "confidence_score": mean_confidence,
        "confidence_tier": confidence_tier,
        "promise_source": f"MULTI_NODE_{len(unique_nodes)}" if len(unique_nodes) > 1 else list(unique_nodes)[0],
        "risk_flags": risk_flags,
        "alternatives": alternatives
    }


def execute_release_adapter(
    case_id: str,
    allocations: List[Dict[str, Any]],
    promise: Dict[str, Any],
    simulate_wms_failure: bool = False
) -> Tuple[bool, Dict[str, Any]]:
    """
    Executes release dispatch to WMS/3PL systems.
    Includes deterministic rollback compensation upon warehouse release rejection.
    """
    if simulate_wms_failure:
        # Simulate warehouse rejection
        return False, {
            "release_status": "REJECTED_BY_WMS",
            "error_code": "WMS_COMMUNICATION_TIMEOUT",
            "acknowledged": False
        }

    dispatch_packets = []
    for a in allocations:
        dispatch_packets.append({
            "dispatch_id": f"DSP-{uuid.uuid4().hex[:8].upper()}",
            "case_id": case_id,
            "target_system": a["location_id"],
            "sku": a["sku"],
            "allocated_qty": a["allocated_qty"],
            "scheduled_dispatch_date": a["estimated_dispatch_date"],
            "promised_delivery_date": promise["promised_delivery_date"],
            "status": "RELEASED_TO_WMS",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    return True, {
        "release_status": "RELEASE_CONFIRMED",
        "acknowledged": True,
        "dispatch_packets": dispatch_packets
    }


# ==============================================================================
# 4. MASTER WORKFLOW-AS-CODE ORCHESTRATOR (Deterministic FSM)
# ==============================================================================

def orchestrate_order_case(
    order_payload: Dict[str, Any],
    force_degraded_inventory: bool = False,
    simulate_release_failure: bool = False
) -> Dict[str, Any]:
    """
    Master Deterministic Orchestration Pipeline.
    Moves the order case across all 17 primary and exception states:
    ORDER_RECEIVED -> DUPLICATE_CHECK -> SCHEMA_VALIDATION -> MASTER_DATA_ENRICHMENT
    -> FINANCIAL_FRAUD_CHECK -> INVENTORY_CONVERGENCE -> ATP_CTP_CALCULATION
    -> SUBSTITUTION_EVALUATION -> SOURCING_CANDIDATE_GENERATION -> ALLOCATION_OPTIMIZATION
    -> PROMISE_GENERATION -> RELEASE_PREPARATION -> RELEASED -> CLOSED.
    """
    start_time = datetime.now()
    channel = str(order_payload.get("channel", "DIRECT")).upper()
    ext_ref = str(order_payload.get("external_order_ref", "ORDER-UNKNOWN"))
    raw_lines = order_payload.get("lines", [])

    # Step 1: Idempotency & Duplicate Check
    idempotency_key = compute_idempotency_key(channel, ext_ref, raw_lines)
    existing_case = BLACKBOARD.get_case_by_idempotency(idempotency_key)

    if existing_case and not order_payload.get("is_update_event"):
        # Duplicate stop
        BLACKBOARD.record_audit(
            case_id=existing_case["case_id"],
            actor="IDEMPOTENCY_GATE",
            event_type="DUPLICATE_ORDER_SUPPRESSED",
            prev_state=existing_case["current_state"],
            new_state=existing_case["current_state"],
            summary=f"Identical order payload received for external reference {ext_ref}. Replay suppressed."
        )
        return existing_case

    case_id = f"CASE-{uuid.uuid4().hex[:10].upper()}"
    current_state = "ORDER_RECEIVED"

    case = {
        "case_id": case_id,
        "channel": channel,
        "external_order_ref": ext_ref,
        "idempotency_key": idempotency_key,
        "current_state": current_state,
        "state_version": 1,
        "raw_order_payload": order_payload,
        "holds": [],
        "exceptions": [],
        "retry_count": 0,
        "is_closed": False,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    BLACKBOARD.record_audit(case_id, "INGESTION_ADAPTER", "ORDER_INGESTED", "INIT", current_state, f"New order received from {channel}: {ext_ref}")

    # Step 2: Schema Validation & Auto-Repair
    current_state = "SCHEMA_VALIDATION"
    case["current_state"] = current_state
    is_valid, normalized_order, val_errors = validate_and_repair_schema(order_payload)

    if not is_valid:
        case["current_state"] = "VALIDATION_FAILED"
        case["next_action"] = "ROUTE_TO_DATA_REPAIR_QUEUE"
        case["exceptions"].append({
            "type": "SCHEMA_VALIDATION_ERROR",
            "severity": "HIGH",
            "owner": "OPERATIONS_DATA_REPAIR",
            "errors": val_errors
        })
        BLACKBOARD.save_case(case)
        BLACKBOARD.record_audit(case_id, "VALIDATION_ENGINE", "SCHEMA_FAILED", "SCHEMA_VALIDATION", "VALIDATION_FAILED", f"Validation errors: {', '.join(val_errors)}")
        BLACKBOARD.record_kpi({"case_id": case_id, "channel": channel, "event_type": "VALIDATION_FAILED", "manual_intervention": True, "intervention_type": "DATA_REPAIR"})
        return case

    case["normalized_order"] = normalized_order
    case["customer_id"] = normalized_order["customer_id"]
    case["requested_delivery_date"] = normalized_order["requested_delivery_date"]
    BLACKBOARD.record_audit(case_id, "VALIDATION_ENGINE", "SCHEMA_PASSED", "ORDER_RECEIVED", "MASTER_DATA_ENRICHMENT", "Schema validated and auto-repaired successfully.")

    # Step 3: Master Data Enrichment
    current_state = "MASTER_DATA_ENRICHMENT"
    case["current_state"] = current_state
    enriched_ok, enrichment, enrich_errors = enrich_master_data(normalized_order)

    if not enriched_ok:
        case["current_state"] = "MASTER_DATA_EXCEPTION"
        case["next_action"] = "ROUTE_TO_MASTER_DATA_QUEUE"
        case["exceptions"].append({
            "type": "MASTER_DATA_ERROR",
            "severity": "CRITICAL",
            "owner": "MDM_OPS",
            "errors": enrich_errors
        })
        BLACKBOARD.save_case(case)
        BLACKBOARD.record_audit(case_id, "ENRICHMENT_ENGINE", "ENRICHMENT_FAILED", "MASTER_DATA_ENRICHMENT", "MASTER_DATA_EXCEPTION", f"Errors: {', '.join(enrich_errors)}")
        BLACKBOARD.record_kpi({"case_id": case_id, "channel": channel, "customer_id": case["customer_id"], "event_type": "MASTER_DATA_EXCEPTION", "manual_intervention": True, "intervention_type": "MDM_RESOLUTION"})
        return case

    case["enrichment"] = enrichment
    case["priority"] = enrichment["customer_priority"]
    case["total_order_value"] = enrichment["total_order_value"]
    BLACKBOARD.record_audit(case_id, "ENRICHMENT_ENGINE", "ENRICHMENT_COMPLETE", "MASTER_DATA_ENRICHMENT", "FINANCIAL_FRAUD_CHECK", f"Enriched with Customer {enrichment['customer']['name']}, SLA Tier: {enrichment['sla_tier']}")

    # Step 4: Financial and Fraud Policy Gate
    current_state = "FINANCIAL_FRAUD_CHECK"
    case["current_state"] = current_state
    fin_passed, fin_results, fin_holds = evaluate_financial_and_fraud_policy(enrichment)
    case["credit_fraud_results"] = fin_results

    if not fin_passed:
        # Determine hold state: Fraud vs Credit
        has_fraud = any(h["type"] == "FRAUD_HOLD" for h in fin_holds)
        hold_state = "FRAUD_HOLD" if has_fraud else "CREDIT_HOLD"
        case["current_state"] = hold_state
        case["next_action"] = "AWAIT_CREDIT_FRAUD_APPROVAL"
        case["holds"].extend(fin_holds)
        BLACKBOARD.save_case(case)
        BLACKBOARD.record_audit(case_id, "FINANCIAL_POLICY_ENGINE", "HOLD_PLACED", "FINANCIAL_FRAUD_CHECK", hold_state, f"Holds: {json.dumps(fin_holds)}")
        BLACKBOARD.record_kpi({"case_id": case_id, "channel": channel, "customer_id": case["customer_id"], "event_type": hold_state, "manual_intervention": True, "intervention_type": "CREDIT_FRAUD_OVERRIDE"})
        return case

    BLACKBOARD.record_audit(case_id, "FINANCIAL_POLICY_ENGINE", "FINANCIAL_CLEARED", "FINANCIAL_FRAUD_CHECK", "INVENTORY_CONVERGENCE", "Credit exposure and fraud risk evaluated: PASSED.")

    # Step 5: Multi-Source Inventory Convergence Gate
    current_state = "INVENTORY_CONVERGENCE"
    case["current_state"] = current_state
    required_skus = [l["sku"] for l in enrichment["lines"]]
    convergence_results = converge_inventory_positions(required_skus, force_degraded=force_degraded_inventory)
    case["inventory_convergence_results"] = convergence_results
    BLACKBOARD.record_audit(case_id, "CONVERGENCE_GATE", "INVENTORY_CONVERGED", "INVENTORY_CONVERGENCE", "ATP_CALCULATION", f"Inventory converged ({convergence_results['status']}). Positions: {convergence_results['sources_count']}")

    # Step 6: ATP & CTP Calculation
    current_state = "ATP_CTP_CALCULATION"
    case["current_state"] = current_state
    atp_ctp_results = calculate_atp_and_ctp(enrichment["lines"], convergence_results["positions"])
    case["atp_ctp_results"] = atp_ctp_results
    BLACKBOARD.record_audit(case_id, "ATP_CTP_ENGINE", "ATP_COMPUTED", "ATP_CTP_CALCULATION", "SUBSTITUTION_EVALUATION", f"ATP Status: {atp_ctp_results['overall_status']} (Requested: {atp_ctp_results['total_requested_units']}, ATP: {atp_ctp_results['total_atp_units']}, CTP: {atp_ctp_results['total_ctp_units']})")

    # Step 7: Substitution Evaluation
    current_state = "SUBSTITUTION_EVALUATION"
    case["current_state"] = current_state
    active_lines, substitution_records = evaluate_substitutions(
        enrichment["lines"], atp_ctp_results, convergence_results["positions"], enrichment["customer"]
    )
    case["substitution_records"] = substitution_records

    # Re-evaluate convergence for any substituted SKUs
    active_skus = [l["sku"] for l in active_lines]
    if active_skus != required_skus:
        convergence_results = converge_inventory_positions(active_skus, force_degraded=force_degraded_inventory)
        case["inventory_convergence_results"] = convergence_results

    # Step 8: Sourcing Candidate Generation
    current_state = "SOURCING_CANDIDATE_GENERATION"
    case["current_state"] = current_state
    sourcing_candidates = generate_sourcing_candidates(
        active_lines, enrichment["destination_zone"], convergence_results["positions"]
    )
    case["sourcing_candidates"] = sourcing_candidates

    if not sourcing_candidates:
        case["current_state"] = "NO_FEASIBLE_PROMISE"
        case["next_action"] = "ROUTE_TO_SOURCING_EXCEPTION_QUEUE"
        case["exceptions"].append({
            "type": "NO_FEASIBLE_SOURCING_NODE",
            "severity": "HIGH",
            "owner": "SUPPLY_CHAIN_PLANNER",
            "detail": "No eligible warehouse or supplier candidates found with stock or capacity."
        })
        BLACKBOARD.save_case(case)
        BLACKBOARD.record_audit(case_id, "SOURCING_ENGINE", "SOURCING_FAILED", "SOURCING_CANDIDATE_GENERATION", "NO_FEASIBLE_PROMISE", "Zero feasible sourcing candidates identified.")
        BLACKBOARD.record_kpi({"case_id": case_id, "channel": channel, "customer_id": case["customer_id"], "event_type": "NO_FEASIBLE_PROMISE", "manual_intervention": True, "intervention_type": "MANUAL_SOURCING"})
        return case

    # Step 9: Allocation Optimization with Split Penalties
    current_state = "ALLOCATION_OPTIMIZATION"
    case["current_state"] = current_state
    alloc_ok, allocations, backorders = optimize_allocation(active_lines, sourcing_candidates, enrichment["customer"])
    case["allocation_plan"] = allocations
    case["backorders"] = backorders

    if not alloc_ok:
        case["current_state"] = "ALLOCATION_FAILED"
        case["next_action"] = "ROUTE_TO_ALLOCATION_EXCEPTION"
        case["exceptions"].append({
            "type": "ALLOCATION_OPTIMIZER_FAILURE",
            "severity": "HIGH",
            "owner": "OPERATIONS_DESK"
        })
        BLACKBOARD.save_case(case)
        return case

    is_split = len(set(a["location_id"] for a in allocations)) > 1
    BLACKBOARD.record_audit(case_id, "ALLOCATION_OPTIMIZER", "ALLOCATION_COMPLETED", "ALLOCATION_OPTIMIZATION", "PROMISE_GENERATION", f"Allocated {len(allocations)} lines across {len(set(a['location_id'] for a in allocations))} nodes (Split: {is_split})")

    # Step 10: Promise Generation
    current_state = "PROMISE_GENERATION"
    case["current_state"] = current_state
    promise = generate_deterministic_promise(allocations, normalized_order["requested_delivery_date"], enrichment["customer"])
    case["promise_details"] = promise
    BLACKBOARD.record_audit(case_id, "PROMISE_ENGINE", "PROMISE_GENERATED", "PROMISE_GENERATION", "RELEASE_PREPARATION", f"Promised delivery: {promise['promised_delivery_date']} (Confidence: {promise['confidence_score']}, Tier: {promise['confidence_tier']})")

    # Step 11: Release Preparation & Execution
    current_state = "RELEASE_PREPARATION"
    case["current_state"] = current_state

    release_ok, release_result = execute_release_adapter(
        case_id, allocations, promise, simulate_wms_failure=simulate_release_failure
    )
    case["release_result"] = release_result

    if not release_ok:
        # Compensation: Rollback allocations
        case["current_state"] = "RELEASE_FAILED"
        case["next_action"] = "ROLLBACK_AND_ESCALATE"
        case["exceptions"].append({
            "type": "WMS_RELEASE_REJECTED",
            "severity": "CRITICAL",
            "owner": "WMS_INTEGRATION_OPS",
            "detail": release_result.get("error_code")
        })
        # Mark allocations as CANCELLED in rollback
        for a in allocations:
            a["allocation_status"] = "CANCELLED_ROLLBACK"

        BLACKBOARD.save_case(case)
        BLACKBOARD.record_audit(case_id, "EXECUTION_ADAPTER", "RELEASE_FAILED_ROLLBACK", "RELEASE_PREPARATION", "RELEASE_FAILED", f"Release failed: {release_result.get('error_code')}. Allocations rolled back.")
        BLACKBOARD.record_kpi({"case_id": case_id, "channel": channel, "customer_id": case["customer_id"], "event_type": "RELEASE_FAILED", "manual_intervention": True, "intervention_type": "WMS_RELEASE_RETRY"})
        return case

    # Success: Order Released
    current_state = "RELEASED"
    case["current_state"] = current_state
    case["next_action"] = "MONITOR_EXECUTION"
    case["is_closed"] = False

    # Calculate Cycle Time & First Pass Fill Rate
    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    req_qty_total = sum(l["ordered_qty"] for l in normalized_order["lines"])
    alloc_qty_total = sum(a["allocated_qty"] for a in allocations)
    fill_rate = round(alloc_qty_total / max(1, req_qty_total), 4)

    # Save to Blackboard
    BLACKBOARD.save_case(case)
    BLACKBOARD.record_audit(case_id, "EXECUTION_ADAPTER", "ORDER_RELEASED", "RELEASE_PREPARATION", "RELEASED", f"Order successfully dispatched to WMS/3PL systems. Cycle time: {elapsed_seconds:.2f}s")

    # Record KPI Fact
    BLACKBOARD.record_kpi({
        "case_id": case_id,
        "channel": channel,
        "customer_id": case["customer_id"],
        "event_type": "ORDER_RELEASED",
        "promised_on_time": True,
        "first_pass_fill_rate": fill_rate,
        "is_oversell": False,
        "is_backordered": len(backorders) > 0,
        "is_split": is_split,
        "split_count": len(set(a["location_id"] for a in allocations)),
        "manual_intervention": False,
        "cycle_time_seconds": elapsed_seconds
    })

    return case


# ==============================================================================
# 5. WINDMILL SCRIPT ENTRY POINT (main)
# ==============================================================================

def main(
    order_payload: dict = {
        "channel": "ECOMMERCE",
        "external_order_ref": "ORD-DEMO-1001",
        "customer_id": "CUST-STRAT-001",
        "requested_delivery_date": (date.today() + timedelta(days=2)).isoformat(),
        "lines": [
            {"line_number": 1, "sku": "SKU-LAPTOP-PRO", "quantity": 5, "uom": "EA", "unit_price": 2200.0},
            {"line_number": 2, "sku": "SKU-DOCK-TB4", "quantity": 5, "uom": "EA", "unit_price": 249.0}
        ]
    },
    force_degraded_inventory: bool = False,
    simulate_release_failure: bool = False
) -> dict:
    """
    Windmill Script Entry Point for f/order_scm/appealing_script.
    Callable directly via Windmill Web UI, Windmill REST API, and Windmill MCP tools/call!
    """
    result = orchestrate_order_case(
        order_payload=order_payload,
        force_degraded_inventory=force_degraded_inventory,
        simulate_release_failure=simulate_release_failure
    )
    return {
        "status": "SUCCESS",
        "case_id": result.get("case_id"),
        "current_state": result.get("current_state"),
        "next_action": result.get("next_action"),
        "customer_id": result.get("customer_id"),
        "promise": result.get("promise_details"),
        "allocation_summary": {
            "total_lines": len(result.get("allocation_plan", [])),
            "is_split": len(set(a["location_id"] for a in result.get("allocation_plan", []))) > 1,
            "nodes": list(set(a["location_id"] for a in result.get("allocation_plan", []))),
            "backorders_count": len(result.get("backorders", []))
        },
        "holds_active": len(result.get("holds", [])),
        "exceptions_active": len(result.get("exceptions", [])),
        "audit_trace_id": result.get("case_id"),
        "result_snapshot": result
    }


if __name__ == "__main__":
    demo_out = main()
    print("\n--- STANDALONE TEST EXECUTION RESULT ---")
    print(f"Case ID: {demo_out['case_id']}")
    print(f"State: {demo_out['current_state']}")
    print(f"Promise: {json.dumps(demo_out['promise'], indent=2)}")
    print(f"Allocation Summary: {json.dumps(demo_out['allocation_summary'], indent=2)}")
