# Changelog

## 0.1.2 — batch retry idempotency

- Implement `EventLedger.append_many(batch_id, events)` with stable caller-supplied batch IDs.
- Return original event records for exact retries without adding duplicate rows or generating new timestamps.
- Reject conflicting batch-ID reuse, duplicate event IDs, forward causal references, and malformed batch inputs.
- Include batch index, size, and normalized-request digest in the hash-chained event records.
- Normalize payloads through canonical JSON before digesting so retries are stable across JSON-compatible container types.
- Preserve JSONL record delimiters and the Windows lock byte as actual single-byte escapes.
- Serialize the complete batch under the existing fail-closed cross-process lock.
- Detect incomplete prior batches and reject retries for forensic review; do not silently repair or discard ledger data.
- Expand adversarial tests for exact retries, conflicting retries, incomplete batches, causal ordering, and lock failure.
- Batch append is not a crash-atomic filesystem transaction. External anchoring remains unconfigured; no production security claim is implied.

## 0.1.1 — hardening

- Fail closed when the advisory ledger lock cannot be acquired or released; do not continue an append unlocked.
- Validate that event payloads are JSON objects and preserve explicitly supplied empty objects.
- Verify every checkpoint digest in the local anchor history, not only the latest checkpoint.
- Add regression tests for lock failure and intermediate checkpoint tampering.
- Add cross-platform CI for Python 3.11–3.13 on Linux, Windows, and macOS.
- Remove empty .tmp artifacts from the test directory.
- No production security claim is implied; external anchoring and batch idempotency were not implemented in this release.
