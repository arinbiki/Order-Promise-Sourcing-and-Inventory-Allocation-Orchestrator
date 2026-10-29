# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0.0 | :x:                |

---

## Reporting a Vulnerability

The project team takes security seriously. If you discover a security vulnerability in this repository, please report it privately:

1. **Do not open a public GitHub issue.**
2. Send an email to the repository maintainers with:
   * A detailed description of the vulnerability.
   * Reproduction steps or a minimal proof of concept (PoC).
   * Potential impact on order promising, financial credit gates, or inventory allocations.
3. You will receive an initial response within 48 hours acknowledging receipt of your report.
4. A coordinated disclosure date will be agreed upon once a patch or remediation is available.

---

## Security Invariants

* **Deterministic Isolation**: Core mathematical allocation algorithms execute locally in isolated worker memory without external network calls.
* **Audit Immutability**: All security watchlist intercepts and credit limit breaches produce append-only records in `order_scm.audit_event`.
* **Zero Credential Storage**: Workflows and scripts must consume credentials exclusively via secure environment variables or vault references.
