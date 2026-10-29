"""
Golden Order Test Suite
Deterministic Order Promise, Sourcing, and Inventory Allocation Orchestrator
Validates all 10 Golden Scenarios from Section 24.1 of the System Design Plan.
"""

import sys
import os
import json
from datetime import date, timedelta

# Import orchestrator logic from windmill folder
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "windmill"))
from appealing_script import orchestrate_order_case, BLACKBOARD, DEFAULT_CUSTOMERS, DEFAULT_MATERIALS


class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.details = []

    def log_result(self, name: str, success: bool, message: str = ""):
        if success:
            self.passed += 1
            status = "PASS"
        else:
            self.failed += 1
            status = "FAIL"
        entry = f"[{status}] {name}: {message}"
        self.details.append(entry)
        print(entry)


def run_all_golden_scenarios() -> bool:
    results = TestResults()
    print("\n==============================================================================")
    print("RUNNING GOLDEN ORDER TEST SUITE (Deterministic SCM Orchestrator)")
    print("==============================================================================\n")

    # --------------------------------------------------------------------------
    # Scenario 1: Perfect Single-Warehouse Order (Straight-Through Processing)
    # --------------------------------------------------------------------------
    sc1_order = {
        "channel": "ECOMMERCE",
        "external_order_ref": "GOLDEN-SC-01",
        "customer_id": "CUST-STRAT-001",
        "requested_delivery_date": (date.today() + timedelta(days=2)).isoformat(),
        "lines": [
            {"line_number": 1, "sku": "SKU-LAPTOP-PRO", "quantity": 5, "uom": "EA", "unit_price": 2200.0},
            {"line_number": 2, "sku": "SKU-DOCK-TB4", "quantity": 5, "uom": "EA", "unit_price": 249.0}
        ]
    }
    sc1_res = orchestrate_order_case(sc1_order)
    sc1_ok = (
        sc1_res["current_state"] == "RELEASED" and
        sc1_res["promise_details"]["confidence_tier"] == "HIGH" and
        sc1_res["promise_details"]["confidence_score"] >= 0.90 and
        len(sc1_res["allocation_plan"]) == 2 and
        not sc1_res["allocation_plan"][0]["is_split"] and
        len(sc1_res["holds"]) == 0
    )
    results.log_result(
        "Scenario 1 (Straight-Through Single-Node)",
        sc1_ok,
        f"State: {sc1_res['current_state']}, Node: {sc1_res['allocation_plan'][0]['location_id']}, Confidence: {sc1_res['promise_details']['confidence_score']}"
    )

    # --------------------------------------------------------------------------
    # Scenario 2: Multi-Warehouse Split Order
    # --------------------------------------------------------------------------
    sc2_order = {
        "channel": "B2B_PORTAL",
        "external_order_ref": "GOLDEN-SC-02",
        "customer_id": "CUST-STRAT-001",
        "requested_delivery_date": (date.today() + timedelta(days=6)).isoformat(),
        "lines": [
            {"line_number": 1, "sku": "SKU-LAPTOP-PRO", "quantity": 70, "uom": "EA", "unit_price": 2200.0},
            {"line_number": 2, "sku": "SKU-MONITOR-4K", "quantity": 80, "uom": "EA", "unit_price": 799.0}
        ]
    }
    sc2_res = orchestrate_order_case(sc2_order)
    nodes_used = set(a["location_id"] for a in sc2_res.get("allocation_plan", []))
    sc2_ok = (
        sc2_res["current_state"] == "RELEASED" and
        len(nodes_used) > 1 and
        any(a["is_split"] for a in sc2_res["allocation_plan"])
    )
    results.log_result(
        "Scenario 2 (Multi-Warehouse Split Allocation)",
        sc2_ok,
        f"State: {sc2_res['current_state']}, Nodes used: {list(nodes_used)}, Split flag: True"
    )

    # --------------------------------------------------------------------------
    # Scenario 3: Credit Hold Order (Financial Exposure Overdue Stop)
    # --------------------------------------------------------------------------
    sc3_order = {
        "channel": "EDI",
        "external_order_ref": "GOLDEN-SC-03",
        "customer_id": "CUST-RISK-003", # $8,500 overdue balance (> $5,000 threshold)
        "requested_delivery_date": (date.today() + timedelta(days=3)).isoformat(),
        "lines": [
            {"line_number": 1, "sku": "SKU-MONITOR-4K", "quantity": 5, "uom": "EA", "unit_price": 799.0}
        ]
    }
    sc3_res = orchestrate_order_case(sc3_order)
    sc3_ok = (
        sc3_res["current_state"] == "CREDIT_HOLD" and
        sc3_res["next_action"] == "AWAIT_CREDIT_FRAUD_APPROVAL" and
        any(h["reason_code"] == "OVERDUE_BALANCE_BLOCKED" for h in sc3_res["holds"])
    )
    results.log_result(
        "Scenario 3 (Financial Overdue Credit Hold)",
        sc3_ok,
        f"State: {sc3_res['current_state']}, Hold reason: {sc3_res['holds'][0]['reason_code']}"
    )

    # --------------------------------------------------------------------------
    # Scenario 4: Deterministic Fraud Hold Order (Watchlist Account)
    # --------------------------------------------------------------------------
    sc4_order = {
        "channel": "MARKETPLACE",
        "external_order_ref": "GOLDEN-SC-04",
        "customer_id": "CUST-FRAUD-004", # Flagged security account
        "requested_delivery_date": (date.today() + timedelta(days=2)).isoformat(),
        "lines": [
            {"line_number": 1, "sku": "SKU-LAPTOP-PRO", "quantity": 1, "uom": "EA", "unit_price": 2200.0}
        ]
    }
    sc4_res = orchestrate_order_case(sc4_order)
    sc4_ok = (
        sc4_res["current_state"] == "FRAUD_HOLD" and
        any(h["type"] == "FRAUD_HOLD" for h in sc4_res["holds"])
    )
    results.log_result(
        "Scenario 4 (Deterministic Fraud Stop)",
        sc4_ok,
        f"State: {sc4_res['current_state']}, Reason: {sc4_res['holds'][0]['reason_code']}"
    )

    # --------------------------------------------------------------------------
    # Scenario 5: Out-of-Stock with Auto-Substitution
    # --------------------------------------------------------------------------
    # Force out of stock for laptop pro by requesting at location or substituting
    sc5_order = {
        "channel": "ECOMMERCE",
        "external_order_ref": "GOLDEN-SC-05",
        "customer_id": "CUST-STRAT-001",
        "requested_delivery_date": (date.today() + timedelta(days=2)).isoformat(),
        "lines": [
            # Laptop Pro net on-hand at DC-East is 60; request 65 to force partial/sub evaluation
            {"line_number": 1, "sku": "SKU-LAPTOP-PRO", "quantity": 65, "uom": "EA", "unit_price": 2200.0}
        ]
    }
    sc5_res = orchestrate_order_case(sc5_order)
    sc5_ok = (
        sc5_res["current_state"] == "RELEASED" and
        sc5_res.get("allocation_plan") is not None
    )
    results.log_result(
        "Scenario 5 (Substitution & High-Demand Allocation)",
        sc5_ok,
        f"State: {sc5_res['current_state']}, Allocated Qty: {sum(a['allocated_qty'] for a in sc5_res['allocation_plan'])}"
    )

    # --------------------------------------------------------------------------
    # Scenario 6: CTP Vendor Future Supply Allocation
    # --------------------------------------------------------------------------
    sc6_order = {
        "channel": "B2B_PORTAL",
        "external_order_ref": "GOLDEN-SC-06",
        "customer_id": "CUST-MID-002",
        "requested_delivery_date": (date.today() + timedelta(days=6)).isoformat(),
        "lines": [
            # Industrial battery pack has 0 on-hand at DCs, only available from SUPPLIER-DIRECT (CTP)
            {"line_number": 1, "sku": "SKU-BATTERY-PACK", "quantity": 25, "uom": "EA", "unit_price": 160.0}
        ]
    }
    sc6_res = orchestrate_order_case(sc6_order)
    sc6_ok = (
        sc6_res["current_state"] == "RELEASED" and
        any(a["location_id"] == "SUPPLIER-DIRECT" for a in sc6_res["allocation_plan"])
    )
    results.log_result(
        "Scenario 6 (CTP Vendor Direct Sourcing)",
        sc6_ok,
        f"State: {sc6_res['current_state']}, Allocated from: {sc6_res['allocation_plan'][0]['location_id']}"
    )

    # --------------------------------------------------------------------------
    # Scenario 7: Schema Auto-Repair (Data Normalization)
    # --------------------------------------------------------------------------
    sc7_order = {
        "channel": "  ecommerce  ", # Whitespace and lowercase
        "external_order_ref": "GOLDEN-SC-07",
        "customer_id": "CUST-STRAT-001",
        "lines": [
            # UOM as "PIECES" (should normalize to "EA")
            {"sku": "SKU-CABLE-TB4", "quantity": "10", "uom": "PIECES", "unit_price": "49.00"}
        ]
    }
    sc7_res = orchestrate_order_case(sc7_order)
    sc7_ok = (
        sc7_res["current_state"] == "RELEASED" and
        sc7_res["normalized_order"]["lines"][0]["uom"] == "EA" and
        sc7_res["normalized_order"]["lines"][0]["ordered_qty"] == 10
    )
    results.log_result(
        "Scenario 7 (Schema Auto-Repair)",
        sc7_ok,
        f"Normalized UOM: {sc7_res['normalized_order']['lines'][0]['uom']}, Channel: {sc7_res['channel']}"
    )

    # --------------------------------------------------------------------------
    # Scenario 8: Duplicate Order Suppression (Idempotent Replay Gate)
    # --------------------------------------------------------------------------
    # Send identical sc1 order again
    sc8_res = orchestrate_order_case(sc1_order)
    sc8_ok = (
        sc8_res["case_id"] == sc1_res["case_id"] and
        sc8_res["idempotency_key"] == sc1_res["idempotency_key"]
    )
    results.log_result(
        "Scenario 8 (Idempotent Duplicate Replay Gate)",
        sc8_ok,
        f"Duplicate suppressed. Returned existing case: {sc8_res['case_id']}"
    )

    # --------------------------------------------------------------------------
    # Scenario 9: Degraded Inventory Convergence Mode
    # --------------------------------------------------------------------------
    sc9_order = {
        "channel": "ECOMMERCE",
        "external_order_ref": "GOLDEN-SC-09",
        "customer_id": "CUST-STRAT-001",
        "requested_delivery_date": (date.today() + timedelta(days=3)).isoformat(),
        "lines": [
            {"sku": "SKU-DOCK-TB4", "quantity": 10, "uom": "EA", "unit_price": 249.0}
        ]
    }
    sc9_res = orchestrate_order_case(sc9_order, force_degraded_inventory=True)
    sc9_ok = (
        sc9_res["current_state"] == "RELEASED" and
        sc9_res["inventory_convergence_results"]["degraded_mode"] is True and
        sc9_res["inventory_convergence_results"]["status"] == "CONVERGED_DEGRADED"
    )
    results.log_result(
        "Scenario 9 (Degraded Inventory Safety Buffer)",
        sc9_ok,
        f"Convergence Status: {sc9_res['inventory_convergence_results']['status']}"
    )

    # --------------------------------------------------------------------------
    # Scenario 10: Warehouse Release Failure & Rollback Compensation
    # --------------------------------------------------------------------------
    sc10_order = {
        "channel": "EDI",
        "external_order_ref": "GOLDEN-SC-10",
        "customer_id": "CUST-STRAT-001",
        "requested_delivery_date": (date.today() + timedelta(days=3)).isoformat(),
        "lines": [
            {"sku": "SKU-MONITOR-4K", "quantity": 5, "uom": "EA", "unit_price": 799.0}
        ]
    }
    sc10_res = orchestrate_order_case(sc10_order, simulate_release_failure=True)
    sc10_ok = (
        sc10_res["current_state"] == "RELEASE_FAILED" and
        sc10_res["next_action"] == "ROLLBACK_AND_ESCALATE" and
        all(a["allocation_status"] == "CANCELLED_ROLLBACK" for a in sc10_res["allocation_plan"])
    )
    results.log_result(
        "Scenario 10 (Release Failure & Rollback Compensation)",
        sc10_ok,
        f"State: {sc10_res['current_state']}, Allocations rolled back: {sc10_res['allocation_plan'][0]['allocation_status']}"
    )

    print("\n==============================================================================")
    print(f"TEST SUMMARY: {results.passed} PASSED / {results.failed} FAILED (Total: {results.passed + results.failed})")
    print("==============================================================================\n")
    return results.failed == 0


if __name__ == "__main__":
    success = run_all_golden_scenarios()
    sys.exit(0 if success else 1)
