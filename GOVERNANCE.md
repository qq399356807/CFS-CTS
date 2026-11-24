# Project Governance and Originator Intent

## Project scope preserved from the existing project record

CFS-CTS Core is a Python prototype audit component for chronological trust and security. Its documented scope includes ordered event records, explicit causal-parent references, clock observations, a SHA-256 hash chain, and a local checkpoint journal. The current documentation explicitly states that it is not a production tamper-proof logging system, trusted timestamping authority, or production compliance system.

The security model, known limitations, adversarial tests, and production release gates remain authoritative technical documents. This summary does not weaken or replace them.

## Originator's collaboration intent

The originator's standing instruction for project work is:

- Routine research, engineering review, validation, documentation, and maintenance should be handled autonomously by the research assistant within the established project scope.
- Do not interrupt the originator for routine progress, ordinary test results, or maintenance tasks that can be handled safely and reversibly.
- Contact the originator only when an important matter genuinely requires attention, such as a consequential architecture or security decision, a critical vulnerability or limitation that changes the risk assessment, a substantial change in project direction or scope, or explicit approval for a governance-sensitive action.
- Do not make consequential decisions on the originator's behalf. When escalation is necessary, explain the evidence, severity, practical implications, uncertainties, and the specific decision required.
- Preserve the distinction between the originator's stated intent and the assistant's interpretation, implementation choices, or proposed improvements. Never present an assistant-authored interpretation as the originator's original statement.
- Do not overstate security properties. Passing tests is evidence only for the tested assertions under the tested conditions; it is not proof of production security, tamper-proofing, or compliance.
- Make routine changes on dedicated branches. Do not merge to `main` without the originator's explicit approval.

## Security and change discipline

1. Preserve explicit threat assumptions, trust boundaries, and known limitations.
2. Treat external anchoring, key management, crash consistency, recovery, and production readiness as separate engineering requirements; do not imply they exist before implementation and validation.
3. Keep test results, threat-model claims, and release claims traceable to specific versions and evidence.
4. Prefer fail-closed behavior for integrity-critical operations; preserve forensic evidence rather than silently repairing or discarding it.
5. Escalate changes that materially alter the threat model, security guarantees, data format, or compatibility contract.

## Provenance note

This file records the originator's collaboration and governance instruction and summarizes the project's documented scope. It is not a claim that every historical project-specific idea or conversation has been reconstructed. Detailed technical intent remains anchored in the existing README, SECURITY_MODEL.md, tests, and versioned change history.
