"""Strict independent verifier for CFS-CTS ledger and simulated anchor."""
import hashlib
import json
import re

GENESIS = "0" * 64
_HEX = re.compile(r"^[0-9a-f]{64}$")
_EVENT_REQUIRED = {
    "version", "seq", "event_id", "timestamp", "payload",
    "parents", "related", "prev_hash", "hash",
}
_TS_REQUIRED = {"utc", "utc_epoch", "monotonic", "logical", "boot_id", "anomalies"}


class IntegrityChecker:
    @staticmethod
    def _oracle_hash(core):
        text = json.dumps(
            core, sort_keys=True, ensure_ascii=False,
            separators=(",", ":"), allow_nan=False
        )
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _issue(issues, kind, at, **details):
        issues.append({"type": kind, "at": at, **details})

    def check(self, events, anchor=None):
        issues = []
        if not isinstance(events, (list, tuple)):
            return {"ok": False, "count": 0, "issues": [{"type": "INVALID_LEDGER", "at": 0}], "head": GENESIS}

        ids = set()
        prev = GENESIS
        prefix_heads = {0: GENESIS}
        for idx, event in enumerate(events, 1):
            if not isinstance(event, dict):
                self._issue(issues, "INVALID_EVENT_TYPE", idx, found=type(event).__name__)
                continue

            missing = sorted(_EVENT_REQUIRED - set(event))
            if missing:
                self._issue(issues, "MISSING_EVENT_FIELDS", idx, fields=missing)
            if event.get("version") != "CFS-CTS-LEDGER-1":
                self._issue(issues, "INVALID_EVENT_VERSION", idx)
            seq = event.get("seq")
            if isinstance(seq, bool) or not isinstance(seq, int) or seq != idx:
                self._issue(issues, "SEQUENCE", idx, found=seq)

            eid = event.get("event_id")
            if not isinstance(eid, str) or not eid.strip():
                self._issue(issues, "INVALID_EVENT_ID", idx)
            elif eid in ids:
                self._issue(issues, "DUPLICATE_ID", idx, event_id=eid)
            else:
                ids.add(eid)

            ts = event.get("timestamp")
            if not isinstance(ts, dict) or not _TS_REQUIRED.issubset(ts):
                self._issue(issues, "INVALID_TIMESTAMP", idx)
            else:
                numeric = ("utc_epoch", "monotonic", "logical")
                if (any(isinstance(ts.get(k), bool) or not isinstance(ts.get(k), (int, float))
                        for k in numeric)
                        or not isinstance(ts.get("utc"), str)
                        or not isinstance(ts.get("boot_id"), str)
                        or not isinstance(ts.get("anomalies"), list)
                        or not all(isinstance(x, str) for x in ts.get("anomalies", []))):
                    self._issue(issues, "INVALID_TIMESTAMP", idx)

            if not isinstance(event.get("payload"), dict):
                self._issue(issues, "INVALID_PAYLOAD", idx)
            for field in ("parents", "related"):
                values = event.get(field)
                if (not isinstance(values, list)
                        or not all(isinstance(x, str) and x for x in values)
                        or (isinstance(values, list) and len(values) != len(set(values)))):
                    self._issue(issues, "INVALID_" + field.upper(), idx)

            if event.get("prev_hash") != prev:
                self._issue(issues, "ORDER_OR_DELETION", idx)
            core = {k: v for k, v in event.items() if k != "hash"}
            try:
                expected = self._oracle_hash(core)
            except (TypeError, ValueError, OverflowError) as exc:
                self._issue(issues, "CANONICALIZATION", idx, detail=str(exc))
                expected = None
            found_hash = event.get("hash")
            if expected is None or expected != found_hash or not isinstance(found_hash, str) or not _HEX.fullmatch(found_hash):
                self._issue(issues, "HASH_MISMATCH", idx)
            prev = found_hash if isinstance(found_hash, str) else ""
            prefix_heads[idx] = prev

            parents = event.get("parents")
            if isinstance(parents, list):
                seen_before = ids - ({eid} if isinstance(eid, str) else set())
                for parent in parents:
                    if not isinstance(parent, str) or parent not in seen_before:
                        self._issue(issues, "MISSING_OR_FORWARD_PARENT", idx, parent=repr(parent))

            # Batch metadata is all-or-nothing when present.
            batch_fields = {"batch_id", "batch_index", "batch_size", "batch_digest"}
            present = batch_fields.intersection(event)
            if present and present != batch_fields:
                self._issue(issues, "INVALID_BATCH_METADATA", idx)
            elif present:
                if (not isinstance(event["batch_id"], str) or not event["batch_id"]
                        or isinstance(event["batch_index"], bool) or not isinstance(event["batch_index"], int)
                        or isinstance(event["batch_size"], bool) or not isinstance(event["batch_size"], int)
                        or event["batch_size"] < 1 or not 0 <= event["batch_index"] < event["batch_size"]
                        or not isinstance(event["batch_digest"], str) or not _HEX.fullmatch(event["batch_digest"])):
                    self._issue(issues, "INVALID_BATCH_METADATA", idx)

        if anchor is not None:
            try:
                history = anchor.history()
                latest = anchor.latest()
                if not isinstance(history, list) or not history:
                    self._issue(issues, "ANCHOR_MISSING_OR_EMPTY", 0)
                else:
                    if not isinstance(latest, dict) or latest != history[-1]:
                        self._issue(issues, "ANCHOR_LATEST_HISTORY_MISMATCH", 0)
                    from .audit_anchor import checkpoint_digest
                    previous_ledger_seq = -1
                    for i, cp in enumerate(history, 1):
                        if not isinstance(cp, dict):
                            self._issue(issues, "INVALID_ANCHOR_CHECKPOINT", i)
                            continue
                        required = {"anchor_seq", "ledger_version", "ledger_seq", "head", "checkpoint_digest"}
                        if not required.issubset(cp):
                            self._issue(issues, "INVALID_ANCHOR_CHECKPOINT", i)
                            continue
                        anchor_seq = cp.get("anchor_seq")
                        ledger_seq = cp.get("ledger_seq")
                        if isinstance(anchor_seq, bool) or not isinstance(anchor_seq, int) or anchor_seq != i:
                            self._issue(issues, "ANCHOR_SEQUENCE", i)
                        if (isinstance(ledger_seq, bool) or not isinstance(ledger_seq, int)
                                or ledger_seq < 0 or ledger_seq > len(events)):
                            self._issue(issues, "ANCHOR_LEDGER_SEQUENCE_RANGE", i, found=ledger_seq, ledger_count=len(events))
                            prefix_matches = False
                        else:
                            expected_head = prefix_heads.get(ledger_seq)
                            prefix_matches = cp.get("head") == expected_head
                            if not prefix_matches:
                                self._issue(issues, "ANCHOR_PREFIX_MISMATCH", i, ledger_seq=ledger_seq)
                        try:
                            digest = checkpoint_digest(cp.get("ledger_version"), ledger_seq, cp.get("head"))
                        except (TypeError, ValueError):
                            digest = None
                        if cp.get("checkpoint_digest") != digest:
                            self._issue(issues, "ANCHOR_DIGEST_MISMATCH", i)
                        if (isinstance(ledger_seq, bool) or not isinstance(ledger_seq, int)
                                or ledger_seq < previous_ledger_seq):
                            self._issue(issues, "ANCHOR_LEDGER_ROLLBACK", i)
                        else:
                            previous_ledger_seq = ledger_seq
                    if history:
                        last = history[-1]
                        if isinstance(last, dict) and (
                            last.get("ledger_seq") != len(events)
                            or last.get("head") != (events[-1].get("hash") if events and isinstance(events[-1], dict) else GENESIS)
                        ):
                            self._issue(issues, "ANCHOR_MISMATCH", 0)
            except Exception as exc:
                self._issue(issues, "ANCHOR_READ_ERROR", 0, detail=f"{type(exc).__name__}: {exc}")

        return {"ok": not issues, "count": len(events), "issues": issues, "head": prev}
