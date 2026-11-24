"""Append-only JSONL ledger. Writes fail closed when locking is unavailable."""
from __future__ import annotations
import hashlib, json, os, threading, uuid
from pathlib import Path
from .audit_anchor import canonical_bytes
from .causal_tracker import CausalTracker

GENESIS = '0' * 64


class EventLedger:
    VERSION = 'CFS-CTS-LEDGER-1'
    MAX_EVENT_BYTES = 1_048_576
    MAX_BATCH_EVENTS = 1_000
    MAX_BATCH_BYTES = 8_388_608

    def __init__(self, path, timebase, causal=None):
        self.path = Path(path)
        self.timebase = timebase
        self.causal = causal or CausalTracker()
        self.lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _read(self):
        if not self.path.exists():
            return []
        raw = self.path.read_bytes()
        if raw and not raw.endswith(b'\\n'):
            raise ValueError('truncated JSONL tail')
        rows = []
        for i, line in enumerate(raw.splitlines(), 1):
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError('record must be a JSON object')
                rows.append(row)
            except Exception as e:
                raise ValueError(f'invalid JSON at line {i}: {e}') from e
        return rows

    @staticmethod
    def _digest(core):
        return hashlib.sha256(canonical_bytes(core)).hexdigest()

    def append(self, event_id=None, payload=None, parents=None, related=None):
        with self.lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(str(self.path) + '.lock', 'a+b') as lockfile:
                _lock(lockfile)
                try:
                    rows = self._read()
                    ids = {r['event_id'] for r in rows}
                    seq = len(rows) + 1
                    eid = str(event_id or uuid.uuid4())
                    if not eid.strip():
                        raise ValueError('event_id must not be empty')
                    if eid in ids:
                        raise ValueError('duplicate event_id')
                    check = self.causal.validate(
                        eid, parents or [], ids, seq,
                        {r['event_id']: r['seq'] for r in rows}
                    )
                    if not check['ok']:
                        raise ValueError(check)
                    ts = self.timebase.sample()
                    event_payload = {} if payload is None else payload
                    if not isinstance(event_payload, dict):
                        raise TypeError('payload must be a JSON object')
                    core = {
                        'version': self.VERSION, 'seq': seq, 'event_id': eid,
                        'timestamp': ts, 'payload': event_payload,
                        'parents': list(parents or []), 'related': list(related or []),
                        'prev_hash': rows[-1]['hash'] if rows else GENESIS
                    }
                    encoded = canonical_bytes(core)
                    if len(encoded) > self.MAX_EVENT_BYTES:
                        raise ValueError('event exceeds maximum encoded size')
                    record = {**core, 'hash': self._digest(core)}
                    data = canonical_bytes(record) + b'\\n'
                    with self.path.open('ab') as f:
                        f.write(data)
                        f.flush()
                        os.fsync(f.fileno())
                    return record
                finally:
                    _unlock(lockfile)

    def append_many(self, batch_id, events):
        """Append a retry-safe batch. Replays return the original records.

        A batch is serialized under the ledger lock and written in one append
        operation. Filesystems do not promise crash-atomic multi-kilobyte writes:
        an incomplete batch is detected and rejected on retry, never auto-repaired.
        """
        if not isinstance(batch_id, str) or not batch_id.strip() or batch_id != batch_id.strip():
            raise ValueError('batch_id must be a non-empty trimmed string')
        if not isinstance(events, list) or not events:
            raise ValueError('events must be a non-empty list')
        if len(events) > self.MAX_BATCH_EVENTS:
            raise ValueError('batch exceeds maximum event count')

        normalized = []
        allowed = {'event_id', 'payload', 'parents', 'related'}
        for index, spec in enumerate(events):
            if not isinstance(spec, dict):
                raise TypeError(f'events[{index}] must be an object')
            unknown = set(spec) - allowed
            if unknown:
                raise ValueError(f'events[{index}] has unsupported fields: {sorted(unknown)}')
            eid = spec.get('event_id')
            if not isinstance(eid, str) or not eid.strip() or eid != eid.strip():
                raise ValueError(f'events[{index}].event_id must be a non-empty trimmed string')
            payload = {} if spec.get('payload') is None else spec.get('payload')
            if not isinstance(payload, dict):
                raise TypeError(f'events[{index}].payload must be a JSON object')
            try:
                payload = json.loads(canonical_bytes(payload))
            except (TypeError, ValueError) as exc:
                raise ValueError(f'events[{index}].payload is not canonical JSON: {exc}') from exc
            parents = spec.get('parents', [])
            related = spec.get('related', [])
            if not isinstance(parents, list) or not all(isinstance(x, str) and x for x in parents):
                raise TypeError(f'events[{index}].parents must be a list of non-empty strings')
            if not isinstance(related, list) or not all(isinstance(x, str) and x for x in related):
                raise TypeError(f'events[{index}].related must be a list of non-empty strings')
            if len(set(parents)) != len(parents):
                raise ValueError(f'events[{index}].parents contains duplicates')
            normalized.append({
                'event_id': eid, 'payload': payload,
                'parents': list(parents), 'related': list(related)
            })

        event_ids = [item['event_id'] for item in normalized]
        if len(set(event_ids)) != len(event_ids):
            raise ValueError('duplicate event_id within batch')
        try:
            batch_digest = hashlib.sha256(canonical_bytes(normalized)).hexdigest()
        except (TypeError, ValueError) as exc:
            raise ValueError(f'batch is not canonical JSON: {exc}') from exc

        with self.lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(str(self.path) + '.lock', 'a+b') as lockfile:
                _lock(lockfile)
                try:
                    rows = self._read()
                    prior_batch = [r for r in rows if r.get('batch_id') == batch_id]
                    if prior_batch:
                        expected_size = len(normalized)
                        indexes = [r.get('batch_index') for r in prior_batch]
                        if (len(prior_batch) != expected_size
                                or indexes != list(range(expected_size))
                                or any(r.get('batch_size') != expected_size
                                       or r.get('batch_digest') != batch_digest
                                       for r in prior_batch)):
                            raise RuntimeError(
                                'incomplete or conflicting prior batch; manual forensic review required'
                            )
                        start = prior_batch[0].get('seq')
                        if [r.get('seq') for r in prior_batch] != list(range(start, start + expected_size)):
                            raise RuntimeError('prior batch records are not contiguous; manual review required')
                        for index, (record, item) in enumerate(zip(prior_batch, normalized)):
                            core = {k: v for k, v in record.items() if k != 'hash'}
                            if (record.get('event_id') != item['event_id']
                                    or record.get('payload') != item['payload']
                                    or record.get('parents') != item['parents']
                                    or record.get('related') != item['related']
                                    or record.get('batch_index') != index
                                    or self._digest(core) != record.get('hash')):
                                raise RuntimeError('prior batch does not match retry or has invalid hash')
                        return prior_batch

                    existing_ids = {r['event_id'] for r in rows}
                    collisions = existing_ids.intersection(event_ids)
                    if collisions:
                        raise ValueError(f'event_id already exists outside this batch: {sorted(collisions)}')

                    known_ids = set(existing_ids)
                    parent_seq = {r['event_id']: r['seq'] for r in rows}
                    previous_hash = rows[-1]['hash'] if rows else GENESIS
                    next_seq = len(rows) + 1
                    records = []
                    chunks = []
                    total_bytes = 0
                    for index, item in enumerate(normalized):
                        eid = item['event_id']
                        check = self.causal.validate(
                            eid, item['parents'], known_ids, next_seq + index, parent_seq
                        )
                        if not check['ok']:
                            raise ValueError(check)
                        ts = self.timebase.sample()
                        core = {
                            'version': self.VERSION, 'seq': next_seq + index,
                            'event_id': eid, 'timestamp': ts, 'payload': item['payload'],
                            'parents': item['parents'], 'related': item['related'],
                            'prev_hash': previous_hash, 'batch_id': batch_id,
                            'batch_index': index, 'batch_size': len(normalized),
                            'batch_digest': batch_digest
                        }
                        encoded = canonical_bytes(core)
                        if len(encoded) > self.MAX_EVENT_BYTES:
                            raise ValueError(f'events[{index}] exceeds maximum encoded size')
                        record = {**core, 'hash': self._digest(core)}
                        chunk = canonical_bytes(record) + b'\\n'
                        chunks.append(chunk)
                        records.append(record)
                        total_bytes += len(chunk)
                        if total_bytes > self.MAX_BATCH_BYTES:
                            raise ValueError('batch exceeds maximum encoded size')
                        previous_hash = record['hash']
                        known_ids.add(eid)
                        parent_seq[eid] = next_seq + index

                    data = b''.join(chunks)
                    with self.path.open('ab') as f:
                        f.write(data)
                        f.flush()
                        os.fsync(f.fileno())
                    return records
                finally:
                    _unlock(lockfile)

    def events(self):
        with self.lock:
            return self._read()

    def head(self):
        rows = self.events()
        return (len(rows), rows[-1]['hash'] if rows else GENESIS)

    def anchor(self, anchor):
        seq, head = self.head()
        prior = anchor.latest()
        n = (prior['anchor_seq'] + 1) if prior else 1
        from .audit_anchor import checkpoint_digest
        cp = {'anchor_seq': n, 'ledger_version': self.VERSION, 'ledger_seq': seq, 'head': head}
        cp['checkpoint_digest'] = checkpoint_digest(self.VERSION, seq, head)
        return anchor.commit(cp)


def _lock(f):
    """Acquire an exclusive advisory lock or fail closed."""
    try:
        if os.name == 'nt':
            import msvcrt
            f.seek(0, os.SEEK_END)
            if f.tell() == 0:
                f.write(b'\\0')
                f.flush()
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
    except Exception as exc:
        raise RuntimeError(f'cannot acquire ledger lock; append aborted: {exc}') from exc


def _unlock(f):
    try:
        if os.name == 'nt':
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception as exc:
        raise RuntimeError(f'cannot release ledger lock cleanly: {exc}') from exc
