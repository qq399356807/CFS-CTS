# CFS-CTS Core 0.1.4

Chronological Trust & Security core, Python 3.11+. Standard library only. This is a prototype audit component, **not a production tamper-proof logging system**.

## Run

```bash
python3 run_tests.py
```

The command executes the deterministic adversarial test suite and writes `reports/test_report.json` and `.md`. The report distinguishes PASS/FAIL/BLOCKED/INCONCLUSIVE. Test data lives in temporary directories. A nonzero process status indicates a failing test.

## API quick start

```python
from src.timebase import TimeBase
from src.event_ledger import EventLedger
from src.audit_anchor import LocalSimulatedAnchor
from src.integrity_checker import IntegrityChecker

clock = TimeBase()
ledger = EventLedger('events.jsonl', clock)
event = ledger.append('evt-001', {'action': 'created'})
batch = ledger.append_many('request-001', [
    {'event_id': 'evt-002', 'payload': {'action': 'updated'}},
    {'event_id': 'evt-003', 'payload': {'action': 'linked'}, 'parents': ['evt-002']},
])
# Repeating the exact batch_id and event data returns the original records.
assert ledger.append_many('request-001', [
    {'event_id': 'evt-002', 'payload': {'action': 'updated'}},
    {'event_id': 'evt-003', 'payload': {'action': 'linked'}, 'parents': ['evt-002']},
]) == batch
anchor = LocalSimulatedAnchor('anchor.jsonl')  # simulated, same-host trust only
ledger.anchor(anchor)
print(IntegrityChecker().check(ledger.events(), anchor))
```

## Batch idempotency contract

- `append_many(batch_id, events)` requires a stable, non-empty caller-supplied batch ID and explicit event IDs.
- The same batch ID and same normalized event data return the original records without writing new events or sampling new timestamps.
- Reusing a batch ID with different data is rejected. Event IDs already present outside that batch are rejected.
- Events are validated in order; a batch event may reference earlier ledger events or earlier events in the same batch, never a later event.
- Batch metadata and a digest of the normalized request are included in every event and covered by the normal hash chain.
- The batch is serialized under the same cross-process lock and appended in one write/flush/fsync sequence.

This is retry idempotency, **not a filesystem transaction guarantee**. A process crash, power loss, or filesystem failure can leave an incomplete batch. A retry detects the incomplete batch and fails closed; it does not append a duplicate, silently repair, or discard forensic evidence. Inspect and preserve the ledger before any manual recovery. A truncated JSONL tail remains a hard read error.

## Components

- `TimeBase`: UTC and monotonic samples, process logical counter, boot/session ID, rollback and UTC jump flags. Default boot ID is a random process-session identifier, not an OS-certified boot UUID.
- `EventLedger`: canonical UTF-8 JSONL, sequence, unique IDs, SHA-256 chain, parent validation, bounded event size, fsync and best-effort cooperating-process lock; includes retry-idempotent batch append.
- `CausalTracker`: explicit parent/related edges; timestamp proximity creates no causal edge.
- `IntegrityChecker`: strict event-schema validation, exact integer sequence checks (booleans rejected), independent canonical JSON/hash verification, causal-reference checks, and fail-closed anchor validation.
- `LocalSimulatedAnchor`: local journal with checkpoint schema validation and cooperating-process advisory locks. It is **not** an external trust root.
- `AnchorProtocol`: extension interface for remote independently administered service; no remote implementation/credentials are included.

## Security boundaries and known risks

A same-privilege attacker can replace both ledger and local anchor, or truncate both. CORE-06 deliberately tests that condition; only a separately administered, authenticated remote anchor can detect it. The current local journal is not cryptographically signed. SHA-256 is unkeyed. External anchoring is not configured. Batch retries are idempotent, but incomplete writes require forensic review. Ledger and local-anchor locks coordinate cooperating processes only. Network filesystems, hostile writers that ignore the lock, power-loss durability, and platform-specific semantics require separate validation. Boot ID is session-generated. Wall time is advisory, monotonic clocks reset across reboot. Checksums do not ensure confidentiality or availability. Do not use as the sole source for safety-critical or legal audit decisions.

## 0.1.4 hardening\n\n- Integrity verification now requires the complete event schema and rejects boolean/non-integer sequence values.\n- Missing, malformed, or empty anchor history fails closed when an anchor is supplied.\n- Anchor checkpoints are schema/digest checked and local anchor reads/writes use a cross-process advisory lock.\n- Appends refuse to extend an existing ledger that fails independent integrity validation.\n- Single-event append validates parent/related collections instead of silently splitting strings.\n- Regression tests cover forged minimal records, boolean sequence values, empty anchors, malformed records, corrupt-history append, and malformed checkpoints.\n\n## Layout

`src/` implementation; `tests/` attack/regression cases; `reports/` generated actual run results. Test fixtures and the tested classes are distinct; integrity verification has a separately written canonicalization/hash path. This reduces common-mode risk but is not formal verification.

## Development checks

GitHub Actions runs the adversarial suite on Python 3.11–3.13 across Linux, Windows, and macOS. Local checks:

```bash
python -m compileall -q src tests
python verify_checksums.py
python run_tests.py
```

See [SECURITY_MODEL.md](SECURITY_MODEL.md) for the threat assumptions and production release gates, and [CHANGELOG.md](CHANGELOG.md) for hardening history.
