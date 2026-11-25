"""Explicit causal relationships; proximity in time is never treated as causality."""
class CausalTracker:
    def validate(self, event_id, parents, known_ids, current_seq=None, parent_seq=None):
        if not isinstance(event_id, str) or not event_id:
            return {"ok": False, "issue": "INVALID_EVENT_ID"}
        if not isinstance(parents, (list, tuple)):
            return {"ok": False, "issue": "INVALID_PARENTS"}
        if not all(isinstance(parent, str) and parent for parent in parents):
            return {"ok": False, "issue": "INVALID_PARENT_ID"}
        if len(set(parents)) != len(parents):
            return {"ok": False, "issue": "DUPLICATE_PARENT"}
        missing = [parent for parent in parents if parent not in known_ids]
        if missing:
            return {"ok": False, "issue": "MISSING_PARENT", "missing": missing}
        if parent_seq and current_seq is not None and any(
            parent_seq.get(parent, current_seq) >= current_seq for parent in parents
        ):
            return {"ok": False, "issue": "CAUSAL_ORDER"}
        return {"ok": True, "issue": None}

    @staticmethod
    def unknown():
        return {"causality": "unknown", "parents": []}
