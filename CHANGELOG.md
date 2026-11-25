# Changelog\n\n## 0.1.3 — historical anchor prefix verification\n\n- Verify each historical checkpoint's ledger sequence is an integer within the supplied ledger range.\n- Verify each checkpoint head matches the hash at its exact ledger prefix, not only the latest ledger head.\n- Add adversarial cases for a wrong prefix with a recomputed valid checkpoint digest, out-of-range checkpoint sequence, and valid multi-checkpoint history.\n- Preserve the local-anchor trust limitation; no external trust root or automatic recovery is added.
- Validate the historical anchor-prefix checks across Linux, Windows, and macOS on Python 3.11–3.13: 26 PASS, 0 FAIL, 0 BLOCKED.\n

## 0.1.2 — batch retry idempotency

- Implement `EventLedger.append_many(batch_id, events)` with stable caller-supplied batch IDs.
- Return original event records for exact retries without adding duplicate rows or generating new timestamps.
- Reject conflicting batch-ID reuse, duplicate event IDs, forward causal references, and malformed batch inputs.
- Include batch index, size, and normalized-request digest in the hash-chained event records.
- Normalize payloads through canonical JSON before digesting so retries are stable across JSON-compatible container types.
- Preserve JSONL record delimiters and the Windows lock byte as actual single-byte escapes.
- Add a regression case for JSON-compatible tuple payload normalization during idempotent retries.
- Serialize the complete batch under the existing fail-closed cross-process lock.
- Detect incomplete prior batches and reject retries for forensic review; do not silently repair or discard ledger data.
- Expand adversarial tests for exact retries, conflicting retries, incomplete batches, causal ordering, and lock failure.
- Record the 23-pass, 0-fail, 0-blocked cross-platform CI validation result.
- Batch append is not a crash-atomic filesystem transaction. External anchoring remains unconfigured; no production security claim is implied.

## 0.1.1 — hardening

- Fail closed when the advisory ledger lock cannot be acquired or released; do not continue an append unlocked.
- Validate that event payloads are JSON objects and preserve explicitly supplied empty objects.
- Verify every checkpoint digest in the local anchor history, not only the latest checkpoint.
- Add regression tests for lock failure and intermediate checkpoint tampering.
- Add cross-platform CI for Python 3.11–3.13 on Linux, Windows, and macOS.
- Remove empty .tmp artifacts from the test directory.
- No production security claim is implied; external anchoring and batch idempotency were not implemented in this release.

## 0.1.4 — verifier and anchor hardening

- Require complete event schemas and strict integer sequence values; booleans no longer pass as sequence 1.
- Fail closed for missing/empty/malformed anchor histories and validate checkpoint schemas.
- Add cooperating-process advisory locking to local anchor history reads and writes.
- Validate existing ledger history before any single or batch append; refuse to extend corrupted history.
- Validate single-event parent and related references as explicit lists of non-empty strings.
- Add regression cases for forged minimal events, malformed rows, empty anchors, corrupt-history append, malformed edges, and malformed checkpoints.
- The local anchor remains simulated and same-host only; SHA-256 remains unkeyed and external anchoring is still not implemented.
