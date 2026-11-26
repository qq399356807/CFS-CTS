# CFS-CTS Core 0.1.4 — Security model

## Scope

CFS-CTS Core records ordered events, explicit causal-parent references, clock observations, a SHA-256 hash chain, and a checkpoint journal. Version 0.1.2 adds retry-idempotent batch append. This remains a local prototype and audit aid. It is not a secure logging appliance, a trusted timestamping authority, or a production compliance system.

## Batch retry semantics

1. The caller supplies a stable batch ID and explicit event IDs.
2. A digest is computed over the normalized ordered event request (IDs, payloads, parents, and related references).
3. The batch is serialized under the same cooperating-process lock as single-event append. Each event records batch ID, zero-based index, batch size, and request digest in the hash-chained core.
4. An exact retry returns the original stored records and does not append rows or sample timestamps again.
5. Reusing a batch ID with different request data, reusing event IDs outside that batch, or using forward causal references is rejected.
6. If only part of a batch is present, a retry fails closed and requires forensic review. The implementation does not guess whether to complete, roll back, or discard data.

This is an application-level retry-idempotency contract, not an atomic storage transaction. A process crash, power loss, partial filesystem write, or storage-controller failure can leave an incomplete batch. The current JSONL format has no journaled commit marker or automatic recovery protocol. Preserve the evidence and inspect it before manual recovery. A truncated JSONL tail remains a hard read error.

## Threats the current tests exercise

- UTC rollback and forward jumps relative to a monotonic clock.
- Event deletion, reordering, payload modification, duplicate identifiers, missing causal parents, malformed/truncated tails, oversized payloads, and concurrent cooperating writers.
- Lock acquisition failure must abort an append rather than silently continue without serialization.
- Corruption of an intermediate local checkpoint must be reported by the verifier.
- Exact batch retry must not duplicate events; conflicting batch reuse and incomplete batches must be rejected.

## Trust assumptions and limits

1. The process and filesystem are not assumed to be safe from a same-privilege attacker.
2. SHA-256 is unkeyed. Anyone able to rewrite the ledger can recompute its chain and batch metadata.
3. LocalSimulatedAnchor is stored locally. An attacker able to rewrite both files can replace their history; CORE-06 intentionally demonstrates this limit.
4. A trustworthy external anchor requires a separately administered service, authenticated transport, key management, retention policy, and independently protected storage. No such service is implemented or configured.
5. File locks coordinate cooperating processes only. Network filesystems, hostile writers that ignore the lock, power-loss durability, and platform-specific semantics require separate validation.
6. UTC is advisory. Monotonic clocks are process/boot scoped and are not portable absolute timestamps.
7. The prototype does not provide confidentiality, availability, secure deletion, remote replication, automatic forensic recovery, or production-grade batch transaction atomicity.

## Release gates before production claims

- Implement an independently administered remote anchor with authenticated, replay-resistant checkpoints and key rotation.
- Specify crash-consistency and recovery semantics; never silently repair or discard forensic tails.
- Validate locking and fsync behavior on supported OS/filesystem combinations, including power-loss tests.
- Add property-based and fault-injection tests, reproducible builds, dependency/SBOM review, and signed releases.
- Conduct an independent security review and publish a versioned threat model.

Passing the local test suite demonstrates only that the tested assertions held in that environment. It does not establish resistance to an adaptive attacker or satisfy a legal/compliance standard.

## Historical anchor-to-ledger continuity (0.1.3)

When an anchor history is supplied to the verifier, every checkpoint is checked against the ledger prefix it claims to represent. The checkpoint's `ledger_seq` must be an integer (not a boolean) in the range zero through the supplied ledger length; its `head` must equal the hash at exactly that prefix (the genesis hash for sequence zero). The checkpoint digest and monotonic anchor/ledger sequence checks remain in force. This detects internally inconsistent local history, but does not make a locally stored anchor independently trustworthy: a same-privilege attacker can rewrite both files and recompute unkeyed digests.


## 0.1.4 verifier and local-anchor hardening

- The verifier requires the full event record schema, checks sequence values as integers excluding booleans, validates timestamp/payload/edge types, and rejects malformed records without allowing a successful result.
- Supplying an anchor requires a non-empty, readable history. Each checkpoint is checked for required fields, sequence types/ranges, digest integrity, and ledger-prefix continuity.
- Local anchor commit/read operations use a cooperating-process advisory lock in addition to an in-process lock. This reduces concurrent-writer races but does not protect against hostile writers that ignore locks or same-privilege replacement of the files.
- Single and batch appends first run the independent verifier over existing history; a corrupt history is not extended.
- Anchor creation validates the ledger and holds the ledger advisory lock through checkpoint commit, preventing an intervening cooperating append from making the snapshot stale.
- Single-event parent and related references must be explicit lists of non-empty strings; strings are not accepted as iterable collections.

These controls harden input validation and accidental/concurrent corruption detection. They do not turn the local anchor into an independent trust root, and do not defend against an attacker able to rewrite both files and recompute unkeyed SHA-256 values.


## 0.1.4 storage-path and write-completion controls

Ledger, local-anchor, and lock-file opens reject final-component symlinks (using `O_NOFOLLOW` where available) and reject non-regular files after opening. The fallback symlink check on platforms without `O_NOFOLLOW` is subject to filesystem races. Parent-directory components are not opened and pinned one-by-one; therefore directory-ancestor symlinks and replacement remain within the trusted-filesystem boundary. Store these files beneath a directory whose ownership and permissions prevent untrusted replacement.

Append paths loop until all bytes have been written and surface a failed or zero-progress write. File `fsync` failures are propagated. This does not imply directory-entry durability, power-loss atomicity, or equivalent guarantees across every filesystem. A partial JSONL tail or incomplete batch is rejected and preserved for forensic review; there is no automatic repair.

The 0.1.4 CI candidate passed 45 adversarial cases across 9 Ubuntu/Windows/macOS and Python 3.11/3.12/3.13 matrix jobs. These results cover the tested CI environments only and do not satisfy the independent review, remote-anchor, reproducible-build, signed-release, or power-loss testing gates above.
