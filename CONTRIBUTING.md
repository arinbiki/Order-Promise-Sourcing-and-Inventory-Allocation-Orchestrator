# Contributing to the Deterministic Order SCM Orchestrator

Thank you for your interest in contributing to the **Deterministic Order Promise, Sourcing, and Inventory Allocation Orchestrator**! We welcome bug fixes, documentation improvements, new carrier transit models, and integration adapters.

---

## Code of Conduct

We are committed to providing a friendly, safe, and welcoming environment for all contributors, regardless of experience level, background, or identity.

---

## Core Architectural Invariants

Before submitting any Pull Request, ensure that your proposed changes strictly uphold the project's foundational architectural rules:

1. **Zero LLM Calls In the Execution Path**:
   * All order validation, credit scoring, inventory convergence, sourcing selection, and cut-off scheduling must remain **100% deterministic, closed-form, and reproducible**.
   * No probabilistic API calls, stochastic models, or external generative AI inference may be introduced into the core state machine.

2. **Immutable Finite State Machine (FSM)**:
   * State transitions must follow the explicit 17-state Moore/Mealy machine defined in [`windmill/appealing_script.py`](./windmill/appealing_script.py).
   * All state transitions must record an audit entry in `order_scm.audit_event`.

3. **Sub-Millisecond Performance**:
   * Critical-path single-order orchestration must maintain a **p50 latency under 1.0 ms**.
   * Batch throughput should sustain at least 15,000 orders/second across 8 CPU worker threads.

---

## Development Workflow

### 1. Fork & Branch
```bash
git checkout -b feat/your-feature-name
```

### 2. Run Verification Tests
All 10 golden scenarios must pass with zero defects:
```bash
python tests/golden_order_test_suite.py
```

### 3. Run Benchmark Regression Test
Ensure throughput and concurrency scale without regression:
```bash
python tests/stress_test_10m.py
```

### 4. Commit Message Guidelines
Follow the [Conventional Commits](https://www.conventionalcommits.org/) format:
* `feat:` A new feature or integration adapter
* `fix:` A bug fix or boundary condition repair
* `docs:` Documentation improvements
* `perf:` A code change that improves throughput or reduces latency
* `test:` Adding or refactoring test suites

---

## Submitting a Pull Request
1. Ensure all SQL migrations in `sql/` are idempotent.
2. Ensure no hardcoded API tokens, database passwords, or personal hostnames are committed.
3. Open a Pull Request against the `main` branch with a clear description of the problem solved.
