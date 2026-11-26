"""Local simulated anchor. This file is not an independently trusted root."""
from __future__ import annotations
import hashlib
import json
import os
import re
import threading
from pathlib import Path
import stat

_HEX = re.compile(r"^[0-9a-f]{64}$")


def _open_nofollow(path, flags, mode=0o600):
    """Open a regular file without following a final-component symlink."""
    nofollow = getattr(os, 'O_NOFOLLOW', 0)
    if not nofollow:
        try:
            if stat.S_ISLNK(os.lstat(path).st_mode):
                raise OSError(f'refusing symlink file: {path}')
        except FileNotFoundError:
            pass
    # O_NONBLOCK prevents a hostile FIFO from hanging a read before fstat().
    flags |= (nofollow | getattr(os, 'O_CLOEXEC', 0)
              | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NONBLOCK', 0))
    fd = os.open(str(path), flags, mode)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(f'refusing non-regular file: {path}')
        return fd
    except Exception:
        os.close(fd)
        raise


def _write_all(fd, data):
    """Write every byte or fail; os.write is permitted to return short counts."""
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        try:
            written = os.write(fd, view[offset:])
        except InterruptedError:
            continue
        if written is None or written <= 0:
            raise OSError("storage write made no forward progress")
        offset += written


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value):
    raise ValueError(f"non-standard JSON constant: {value}")


def canonical_bytes(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def checkpoint_digest(version, seq, head):
    return hashlib.sha256(canonical_bytes({"version": version, "seq": seq, "head": head})).hexdigest()


class AnchorProtocol:
    def commit(self, checkpoint: dict) -> dict:
        raise NotImplementedError

    def latest(self) -> dict | None:
        raise NotImplementedError

    def history(self) -> list:
        raise NotImplementedError


class LocalSimulatedAnchor(AnchorProtocol):
    SIMULATED = True

    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.lock_path = Path(str(self.path) + ".lock")

    def _load(self):
        try:
            fd = _open_nofollow(self.path, os.O_RDONLY)
        except FileNotFoundError:
            return []
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read()
        if raw and not raw.endswith(b"\n"):
            raise ValueError("truncated anchor journal")
        rows = []
        for number, line in enumerate(raw.splitlines(), 1):
            try:
                item = json.loads(line, object_pairs_hook=_unique_json_object, parse_constant=_reject_json_constant)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(f"invalid anchor JSON at line {number}: {exc}") from exc
            if not isinstance(item, dict):
                raise ValueError(f"anchor record at line {number} must be an object")
            rows.append(item)
        return rows

    def _validate_history(self, history):
        if not isinstance(history, list):
            raise ValueError("anchor history must be a list")
        previous_ledger_seq = -1
        required = {"anchor_seq", "ledger_version", "ledger_seq", "head", "checkpoint_digest"}
        for index, item in enumerate(history, 1):
            if not isinstance(item, dict) or set(item) != required:
                raise ValueError(f"invalid existing checkpoint at sequence {index}")
            aseq, lseq = item["anchor_seq"], item["ledger_seq"]
            if isinstance(aseq, bool) or not isinstance(aseq, int) or aseq != index:
                raise ValueError(f"invalid existing anchor sequence at {index}")
            if isinstance(lseq, bool) or not isinstance(lseq, int) or lseq < 0 or lseq < previous_ledger_seq:
                raise ValueError(f"invalid existing ledger sequence at {index}")
            if item["ledger_version"] != "CFS-CTS-LEDGER-1":
                raise ValueError(f"invalid existing ledger version at {index}")
            if not isinstance(item["head"], str) or not _HEX.fullmatch(item["head"]):
                raise ValueError(f"invalid existing checkpoint head at {index}")
            expected = checkpoint_digest(item["ledger_version"], lseq, item["head"])
            if item["checkpoint_digest"] != expected:
                raise ValueError(f"existing checkpoint digest mismatch at {index}")
            previous_ledger_seq = lseq

    def _validate_checkpoint(self, checkpoint, history):
        self._validate_history(history)
        if not isinstance(checkpoint, dict):
            raise TypeError("checkpoint must be an object")
        required = {"anchor_seq", "ledger_version", "ledger_seq", "head", "checkpoint_digest"}
        if set(checkpoint) != required:
            raise ValueError("checkpoint fields do not match the supported schema")
        aseq, lseq = checkpoint["anchor_seq"], checkpoint["ledger_seq"]
        if isinstance(aseq, bool) or not isinstance(aseq, int) or aseq < 1:
            raise ValueError("anchor_seq must be a positive integer")
        if isinstance(lseq, bool) or not isinstance(lseq, int) or lseq < 0:
            raise ValueError("ledger_seq must be a non-negative integer")
        if checkpoint["ledger_version"] != "CFS-CTS-LEDGER-1":
            raise ValueError("unsupported ledger_version")
        if not isinstance(checkpoint["head"], str) or not _HEX.fullmatch(checkpoint["head"]):
            raise ValueError("head must be a lowercase SHA-256 hex digest")
        expected_digest = checkpoint_digest(checkpoint["ledger_version"], lseq, checkpoint["head"])
        if checkpoint["checkpoint_digest"] != expected_digest:
            raise ValueError("checkpoint digest mismatch")
        expected_seq = history[-1]["anchor_seq"] + 1 if history else 1
        if aseq != expected_seq:
            raise ValueError("anchor sequence must increment")
        if history and lseq < history[-1]["ledger_seq"]:
            raise ValueError("ledger rollback")

    def _locked(self):
        """Cross-process advisory lock; only coordinates cooperating writers."""
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = None
        stream = None
        try:
            fd = _open_nofollow(self.lock_path, os.O_RDWR | os.O_CREAT)
            stream = os.fdopen(fd, "r+b")
            fd = None
            if os.name == "nt":
                import msvcrt
                stream.seek(0, os.SEEK_END)
                if stream.tell() == 0:
                    stream.write(b"\0")
                    stream.flush()
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            return stream
        except Exception as exc:
            if stream is not None:
                stream.close()
            elif fd is not None:
                os.close(fd)
            raise RuntimeError(f"cannot acquire anchor lock: {exc}") from exc

    @staticmethod
    def _unlock(stream):
        try:
            if os.name == "nt":
                import msvcrt
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()

    def commit(self, checkpoint):
        with self.lock:
            lockfile = self._locked()
            try:
                history = self._load()
                self._validate_checkpoint(checkpoint, history)
                data = canonical_bytes(checkpoint) + b"\n"
                self.path.parent.mkdir(parents=True, exist_ok=True)
                data_fd = _open_nofollow(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
                try:
                    _write_all(data_fd, data)
                    os.fsync(data_fd)
                finally:
                    os.close(data_fd)
                return checkpoint
            finally:
                self._unlock(lockfile)

    def latest(self):
        with self.lock:
            lockfile = self._locked()
            try:
                history = self._load()
                return history[-1] if history else None
            finally:
                self._unlock(lockfile)

    def history(self):
        with self.lock:
            lockfile = self._locked()
            try:
                return self._load()
            finally:
                self._unlock(lockfile)
