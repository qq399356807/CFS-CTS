import json, multiprocessing, os, tempfile, time
from pathlib import Path
from .common import Fixture
from src.integrity_checker import IntegrityChecker


def _worker(path, n):
    from src.event_ledger import EventLedger
    from src.timebase import TimeBase
    ledger = EventLedger(path, TimeBase())
    ledger.append(f'concurrent-{n}', {'n': n})


def test_concurrency():
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / 'l.jsonl')
        processes = [multiprocessing.Process(target=_worker, args=(path, i)) for i in range(12)]
        [p.start() for p in processes]
        [p.join(10) for p in processes]
        assert all(p.exitcode == 0 for p in processes)
        rows = [json.loads(x) for x in Path(path).read_text().splitlines()]
        assert len(rows) == 12
        assert len({x['event_id'] for x in rows}) == 12
        assert IntegrityChecker().check(rows)['ok']
        return {'events': len(rows), 'exitcodes': [p.exitcode for p in processes]}


def test_duplicate_id():
    fixture = Fixture()
    fixture.add('same')
    try:
        fixture.add('same')
    except ValueError:
        return {'rejected': True}
    raise AssertionError('duplicate accepted')


def test_crash_partial():
    fixture = Fixture()
    with fixture.ledger.path.open('ab') as stream:
        stream.write(b'{"partial":')
    try:
        fixture.ledger.events()
    except ValueError as exc:
        assert 'truncated' in str(exc)
        return {'detected': True, 'detail': str(exc)}
    raise AssertionError('truncated tail accepted')


def test_truncation():
    return test_crash_partial()


def test_anchor_rollback():
    fixture = Fixture()
    fixture.add()
    fixture.ledger.anchor(fixture.anchor)
    first = fixture.anchor.latest()
    fixture.add('second')
    fixture.ledger.anchor(fixture.anchor)
    history = fixture.anchor.history()
    assert len(history) == 2 and history[0] == first and history[1]['anchor_seq'] == 2
    return {'append_only_history': len(history), 'rollback_of_local_file': 'not protected against same-privilege rewrite'}


def test_missing_parent():
    fixture = Fixture()
    try:
        fixture.add('child', parents=['absent'])
    except ValueError:
        return {'rejected': True}
    raise AssertionError('missing parent accepted')


def test_large_event():
    from src.event_ledger import EventLedger
    fixture = Fixture()
    try:
        fixture.add('large', {'blob': 'x' * (EventLedger.MAX_EVENT_BYTES + 100)})
    except ValueError as exc:
        return {'rejected': True, 'detail': str(exc)}
    raise AssertionError('oversize event accepted')


def test_unicode_canonical():
    fixture = Fixture()
    fixture.add('unicode', {'text': '雪☃️ café', 'number': 1.0})
    assert fixture.checker.check(fixture.ledger.events())['ok']
    return {'utf8_roundtrip': True, 'oracle': 'independent checker implementation'}


def test_duplicate_batch():
    fixture = Fixture()
    batch = [
        {'event_id': 'batch-a', 'payload': {'action': 'created', 'labels': ('x', 'y')}},
        {'event_id': 'batch-b', 'payload': {'action': 'linked'}, 'parents': ['batch-a']}
    ]
    first = fixture.ledger.append_many('request-001', batch)
    after_first = fixture.ledger.head()
    retry = fixture.ledger.append_many('request-001', batch)
    assert retry == first
    assert fixture.ledger.head() == after_first
    assert len(fixture.ledger.events()) == 2
    assert fixture.checker.check(fixture.ledger.events())['ok']
    return {'records': len(first), 'retry_returns_original': True, 'ledger_unchanged_on_retry': True}


def test_batch_conflict():
    fixture = Fixture()
    fixture.ledger.append_many('request-002', [{'event_id': 'batch-c', 'payload': {'v': 1}}])
    try:
        fixture.ledger.append_many('request-002', [{'event_id': 'batch-c', 'payload': {'v': 2}}])
    except RuntimeError:
        return {'conflicting_retry_rejected': True}
    raise AssertionError('conflicting batch retry accepted')


def test_incomplete_batch():
    fixture = Fixture()
    batch = [
        {'event_id': 'batch-d1', 'payload': {'v': 1}},
        {'event_id': 'batch-d2', 'payload': {'v': 2}}
    ]
    fixture.ledger.append_many('request-003', batch)
    rows = fixture.ledger.events()
    fixture.ledger.path.write_bytes((json.dumps(rows[0], sort_keys=True, ensure_ascii=False, separators=(',', ':')) + '\n').encode())
    try:
        fixture.ledger.append_many('request-003', batch)
    except RuntimeError as exc:
        assert 'incomplete' in str(exc)
        return {'incomplete_batch_retry_rejected': True, 'automatic_repair': False}
    raise AssertionError('incomplete batch was silently accepted')


def test_batch_validation():
    fixture = Fixture()
    try:
        fixture.ledger.append_many('request-004', [
            {'event_id': 'batch-e1', 'payload': {}, 'parents': ['batch-e2']},
            {'event_id': 'batch-e2', 'payload': {}}
        ])
    except ValueError:
        assert fixture.ledger.events() == []
        return {'forward_parent_rejected': True, 'ledger_unchanged': True}
    raise AssertionError('forward causal parent accepted')


def test_bad_clock():
    from src.timebase import TimeBase
    def bad():
        return float('nan')
    try:
        TimeBase(bad, lambda: 1).sample()
    except RuntimeError as exc:
        return {'rejected': True, 'detail': str(exc)}
    raise AssertionError('bad clock accepted')


def test_lock_failure_closed():
    import src.event_ledger as module
    fixture = Fixture()
    original = module._lock
    def deny(_):
        raise RuntimeError('injected lock failure')
    module._lock = deny
    try:
        try:
            fixture.add('must-not-write')
        except RuntimeError as exc:
            assert 'injected lock failure' in str(exc)
            assert not fixture.ledger.path.exists() or fixture.ledger.path.stat().st_size == 0
            return {'append_aborted': True, 'ledger_unchanged': True}
        raise AssertionError('append continued after lock failure')
    finally:
        module._lock = original


def test_batch_lock_failure_closed():
    import src.event_ledger as module
    fixture = Fixture()
    original = module._lock
    def deny(_):
        raise RuntimeError('injected batch lock failure')
    module._lock = deny
    try:
        try:
            fixture.ledger.append_many('request-005', [{'event_id': 'locked', 'payload': {}}])
        except RuntimeError as exc:
            assert 'injected batch lock failure' in str(exc)
            assert not fixture.ledger.path.exists() or fixture.ledger.path.stat().st_size == 0
            return {'batch_aborted': True, 'ledger_unchanged': True}
        raise AssertionError('batch append continued after lock failure')
    finally:
        module._lock = original


def test_anchor_history_tamper():
    fixture = Fixture()
    fixture.add('one')
    fixture.ledger.anchor(fixture.anchor)
    fixture.add('two')
    fixture.ledger.anchor(fixture.anchor)
    history = fixture.anchor.history()
    history[0]['head'] = 'f' * 64
    fixture.anchor.path.write_text(''.join(
        json.dumps(x, sort_keys=True, separators=(',', ':')) + '\n' for x in history
    ))
    result = fixture.checker.check(fixture.ledger.events(), fixture.anchor)
    assert any(x['type'] == 'ANCHOR_DIGEST_MISMATCH' for x in result['issues'])
    return {'intermediate_checkpoint_tamper_detected': True}


def test_anchor_prefix_mismatch_with_valid_digest():
    from src.audit_anchor import checkpoint_digest
    fixture = Fixture()
    fixture.add('prefix-one')
    fixture.ledger.anchor(fixture.anchor)
    fixture.add('prefix-two')
    history = fixture.anchor.history()
    history[0]['head'] = 'f' * 64
    history[0]['checkpoint_digest'] = checkpoint_digest(
        history[0]['ledger_version'], history[0]['ledger_seq'], history[0]['head']
    )
    fixture.anchor.path.write_text(''.join(
        json.dumps(x, sort_keys=True, separators=(',', ':')) + '\\n' for x in history
    ))
    result = fixture.checker.check(fixture.ledger.events(), fixture.anchor)
    assert any(x['type'] == 'ANCHOR_PREFIX_MISMATCH' for x in result['issues'])
    assert not any(x['type'] == 'ANCHOR_DIGEST_MISMATCH' for x in result['issues'])
    return {'valid_digest_wrong_prefix_rejected': True}


def test_anchor_sequence_out_of_range():
    from src.audit_anchor import checkpoint_digest
    fixture = Fixture()
    fixture.add('range-one')
    fixture.ledger.anchor(fixture.anchor)
    history = fixture.anchor.history()
    history[0]['ledger_seq'] = 99
    history[0]['checkpoint_digest'] = checkpoint_digest(
        history[0]['ledger_version'], history[0]['ledger_seq'], history[0]['head']
    )
    fixture.anchor.path.write_text(''.join(
        json.dumps(x, sort_keys=True, separators=(',', ':')) + '\\n' for x in history
    ))
    result = fixture.checker.check(fixture.ledger.events(), fixture.anchor)
    assert any(x['type'] == 'ANCHOR_LEDGER_SEQUENCE_RANGE' for x in result['issues'])
    return {'out_of_range_checkpoint_rejected_without_exception': True}


def test_valid_anchor_prefix_history():
    fixture = Fixture()
    fixture.add('history-one')
    fixture.ledger.anchor(fixture.anchor)
    fixture.add('history-two')
    fixture.ledger.anchor(fixture.anchor)
    result = fixture.checker.check(fixture.ledger.events(), fixture.anchor)
    assert result['ok'], result['issues']
    return {'valid_historical_prefixes_accepted': True, 'checkpoints': len(fixture.anchor.history())}
