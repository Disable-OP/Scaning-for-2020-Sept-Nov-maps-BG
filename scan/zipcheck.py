#!/usr/bin/env python3
"""Independent archive verification shared by worker and final verifier.

A response only counts as a map if it is a structurally valid ZIP whose CRCs
all pass; HTML/error/injected content is rejected. SHA-256 is computed for the
exact bytes retrieved. No proxy observation is ever trusted here - callers
must pass bytes obtained directly from the origin host.
"""
from __future__ import annotations

import hashlib
import io
import zipfile


def verify_zip_bytes(data: bytes) -> dict:
    out = {
        "bytes": len(data),
        "zip_magic_ok": data[:4] in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"),
        "html_rejected": False,
        "zip_ok": False,
        "zip_crc_ok": False,
        "zip_entries": 0,
        "zip_names_sample": [],
        "sha256": hashlib.sha256(data).hexdigest(),
        "error": None,
    }
    head = data[:512].lower()
    if b"<html" in head or b"<!doctype html" in head or b"<script" in head:
        out["html_rejected"] = True
        out["error"] = "content looks like HTML, not a ZIP"
        return out
    if not out["zip_magic_ok"]:
        out["error"] = "missing PK magic"
        return out
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            bad = z.testzip()  # full CRC check of every member
            names = z.namelist()
            out["zip_entries"] = len(names)
            out["zip_names_sample"] = names[:12]
            out["zip_ok"] = True
            out["zip_crc_ok"] = (bad is None)
            if bad is not None:
                out["error"] = f"CRC failure in member: {bad}"
    except zipfile.BadZipFile as e:
        out["error"] = f"BadZipFile: {e}"
    except Exception as e:  # structural surprise = reject
        out["error"] = f"zip parse error: {e}"
    return out
