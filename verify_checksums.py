#!/usr/bin/env python3
"""Verify SHA-256 entries using canonical LF bytes on every platform."""
import hashlib
from pathlib import Path

manifest = Path("SHA256SUMS.txt")
errors = []
checked = 0
for number, raw in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
    if not raw.strip():
        continue
    try:
        expected, relative = raw.split("  ", 1)
    except ValueError:
        errors.append(f"line {number}: expected '<sha256><two spaces><path>'")
        continue
    path = Path(relative)
    if not path.is_file():
        errors.append(f"{relative}: file missing")
        continue
    data = path.read_bytes().replace(b"\\r\\n", b"\\n")
    actual = hashlib.sha256(data).hexdigest()
    checked += 1
    if actual != expected:
        errors.append(f"{relative}: expected {expected}, got {actual}")
if errors:
    print("\\n".join(errors))
    raise SystemExit(1)
print(f"Verified {checked} SHA-256 entries (canonical LF representation).")
