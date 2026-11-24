"""Independent integrity verifier. Uses a separately written canonicalization/hash path."""
import hashlib, json
GENESIS='0'*64


class IntegrityChecker:
    @staticmethod
    def _oracle_hash(core):
        # Deliberately separate from ledger._digest; this is an independent verification path.
        text=json.dumps(core,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def check(self,events,anchor=None):
        issues=[]; ids=set(); prev=GENESIS
        prefix_heads={0:GENESIS}
        for idx,e in enumerate(events,1):
            if e.get('seq')!=idx: issues.append({'type':'SEQUENCE','at':idx,'found':e.get('seq')})
            if e.get('event_id') in ids: issues.append({'type':'DUPLICATE_ID','at':idx})
            ids.add(e.get('event_id'))
            if e.get('prev_hash')!=prev: issues.append({'type':'ORDER_OR_DELETION','at':idx})
            core={k:v for k,v in e.items() if k!='hash'}
            try: expected=self._oracle_hash(core)
            except Exception as ex: issues.append({'type':'CANONICALIZATION','at':idx,'detail':str(ex)}); expected=None
            if expected != e.get('hash'): issues.append({'type':'HASH_MISMATCH','at':idx})
            prev=e.get('hash','')
            prefix_heads[idx]=prev
            for p in e.get('parents',[]):
                if p not in ids-{e.get('event_id')}: issues.append({'type':'MISSING_OR_FORWARD_PARENT','at':idx,'parent':p})
        if anchor is not None:
            latest=anchor.latest()
            hist=getattr(anchor,'history',lambda:[])()
            if latest:
                from .audit_anchor import checkpoint_digest
                previous_ledger_seq=-1
                for i,cp in enumerate(hist,1):
                    if cp.get('anchor_seq')!=i:
                        issues.append({'type':'ANCHOR_SEQUENCE','at':i})
                    ledger_seq=cp.get('ledger_seq')
                    if isinstance(ledger_seq,bool) or not isinstance(ledger_seq,int) or ledger_seq<0 or ledger_seq>len(events):
                        issues.append({'type':'ANCHOR_LEDGER_SEQUENCE_RANGE','at':i,'found':ledger_seq,'ledger_count':len(events)})
                        prefix_matches=False
                    else:
                        expected_head=prefix_heads[ledger_seq]
                        prefix_matches=cp.get('head')==expected_head
                        if not prefix_matches:
                            issues.append({'type':'ANCHOR_PREFIX_MISMATCH','at':i,'ledger_seq':ledger_seq,
                                           'expected_head':expected_head,'found_head':cp.get('head')})
                    expected_cp=checkpoint_digest(cp.get('ledger_version'),ledger_seq,cp.get('head'))
                    if cp.get('checkpoint_digest')!=expected_cp:
                        issues.append({'type':'ANCHOR_DIGEST_MISMATCH','at':i})
                    if isinstance(ledger_seq,bool) or not isinstance(ledger_seq,int) or ledger_seq<previous_ledger_seq:
                        issues.append({'type':'ANCHOR_LEDGER_ROLLBACK','at':i})
                    else:
                        previous_ledger_seq=ledger_seq
                if latest.get('ledger_seq')!=len(events) or latest.get('head')!=(events[-1].get('hash') if events else GENESIS):
                    issues.append({'type':'ANCHOR_MISMATCH'})
        return {'ok':not issues,'count':len(events),'issues':issues,'head':prev}
