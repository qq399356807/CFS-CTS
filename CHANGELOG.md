# Changelog

## 0.1.4 — verifier, storage, and anchor hardening

- Reject duplicate JSON object keys and non-standard JSON constants in ledger and anchor readers.
- Normalize single-append payloads through canonical JSON so the returned record matches persisted data.
- Enforce an exact checkpoint field allowlist for incoming, historical, and independently verified checkpoints.
- Reject final-component symlinks and non-regular files for ledger, anchor, and lock paths; complete short writes and report fsync failures.
- Validate 45 adversarial cases across Ubuntu, Windows, and macOS with Python 3.11–3.13 (9 CI matrix jobs).
- Parent-directory symlink traversal is not pinned; deployments must use trusted, access-controlled parent directories. Crash/power-loss atomicity and automatic forensic recovery are not provided.


- Require complete event schemas and strict integer sequence values; booleans no longer pass as sequence 1.
- Fail closed for missing, empty, or malformed anchor histories and validate checkpoint schemas.
- Add cooperating-process advisory locking to local anchor history reads and writes.
- Validate existing ledger history before any single or batch append; refuse to extend corrupted history.
- Validate single-event parent and related references as explicit lists of non-empty strings.
- Capture anchor checkpoints from a validated ledger snapshot while holding the ledger's cooperating-process lock.
- Add regression cases for forged minimal events, malformed rows, empty anchors, corrupt-history append, malformed edges, and malformed checkpoints.
- The local anchor remains simulated and same-host only; SHA-256 remains unkeyed and external anchoring is still not implemented.

## 0.1.3 — historical anchor prefix verification

- Verify each historical checkpoint's ledger sequence is an integer within the supplied ledger range.
- Verify each checkpoint head matches the hash at its exact ledger prefix, not only the latest ledger head.
- Add adversarial cases for a wrong prefix with a recomputed valid checkpoint digest, out-of-range checkpoint sequence, and valid multi-checkpoint history.
- Preserve the local-anchor trust limitation; no external trust root or automatic recovery is added.
- Validate the historical anchor-prefix checks across Linux, Windows, and macOS on Python 3.11–3.13: 26 PASS, 0 FAIL, 0 BLOCKED.

## 0.1.2 — batch retry idempotency

- Implement `EventLedger.append_many(batch_id, events)` with stable caller-supplied batch IDs.
- Return original event records for exact retries without adding duplicate rows or generating new timestamps.
- Reject conflicting batch-ID reuse, duplicate event IDs, forward causal references, and malformed batch inputs.
- Include batch index, size, and normalized-request digest in the hash-chained event records.
- Normalize payloads through canonical JSON before digesting so retries are stable across JSON-compatible container types.
- Serialize the complete batch under the existing fail-closed cross-process lock.
- Detect incomplete prior batches and reject retries for forensic review; do not silently repair or discard ledger data.
- Batch append is not a crash-atomic filesystem transaction. External anchoring remains unconfigured.

## 0.1.1 — hardening

- Fail closed when the advisory ledger lock cannot be acquired or released.
- Validate that event payloads are JSON objects and preserve explicitly supplied empty objects.
- Verify every checkpoint digest in the local anchor history, not only the latest checkpoint.
- Add regression tests for lock failure and intermediate checkpoint tampering.
- Add cross-platform CI for Python 3.11–3.13 on Linux, Windows, and macOS.
- No production security claim is implied; external anchoring and batch idempotency were not implemented in this release.
