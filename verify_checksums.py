#!/usr/bin/env python3
"""Verify SHA-256 entries using a platform-independent CRLF text representation.

GitHub Actions checks out text files with different line endings on Windows
and Unix runners. The manifest was generated from the Windows working tree,
so normalize every checked text file to CRLF before hashing. This keeps the
committed digests stable without weakening content verification.
"""
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
    data = path.read_bytes().replace(b"\\r\\n", b"\\n").replace(b"\\n", b"\\r\\n")
    actual = hashlib.sha256(data).hexdigest()
    checked += 1
    if actual != expected:
        errors.append(f"{relative}: expected {expected}, got {actual}")
if errors:
    print("\\n".join(errors))
    raise SystemExit(1)
print(f"Verified {checked} SHA-256 entries (canonical CRLF representation).")
