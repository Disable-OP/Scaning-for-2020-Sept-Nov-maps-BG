#!/usr/bin/env python3
"""Final independent verification of HTTP 200 candidates.

Runs DIRECTLY against the origin (no proxy, no cache tricks). A map is only
marked VERIFIED when: GET 200 + PK magic + valid ZIP structure + all-member
CRC pass + SHA-256 recorded + not HTML/injected. Used by the controller for
every found candidate and again at final aggregation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from zipcheck import verify_zip_bytes  # noqa: E402

UA = "blockman-go-archival-scan/4.0 (independent verification; direct origin fetch)"


def verify_url(url: str, out_dir: Path | None = None) -> dict:
    rec = {"url": url, "verified_at_utc": time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "get_status": None}
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            rec["get_status"] = r.status
            rec["content_type"] = r.headers.get("Content-Type", "")
            data = r.read()
    except urllib.error.HTTPError as e:
        rec["get_status"] = e.code
        rec["error"] = f"HTTPError {e.code} during direct verification"
        rec["verified"] = False
        return rec
    except Exception as e:
        rec["error"] = f"fetch error: {e}"
        rec["verified"] = False
        return rec
    rec["bytes"] = len(data)
    rec.update(verify_zip_bytes(data))
    rec["sha256"] = rec.get("sha256") or hashlib.sha256(data).hexdigest()
    rec["verified"] = bool(rec["zip_ok"] and rec["zip_crc_ok"]
                           and not rec["html_rejected"])
    if rec["verified"] and out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)
        name = url.rsplit("/", 1)[-1]
        (out_dir / name).write_bytes(data)
        rec["saved_as"] = str(out_dir / name)
    return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", nargs="+", required=True,
                    help="URLs to verify, or @file.json with {'urls':[...]}")
    ap.add_argument("--save-dir", default="")
    ap.add_argument("--out", default="verification.json")
    args = ap.parse_args()
    urls = []
    for u in args.urls:
        if u.startswith("@"):
            urls.extend(json.loads(Path(u[1:]).read_text())["urls"])
        else:
            urls.append(u)
    out_dir = Path(args.save_dir) if args.save_dir else None
    results = [verify_url(u, out_dir) for u in urls]
    Path(args.out).write_text(json.dumps({
        "verified_count": sum(1 for r in results if r["verified"]),
        "results": results}, indent=1) + "\n")
    for r in results:
        print(f"{'VERIFIED' if r['verified'] else 'REJECTED'} {r['url']} "
              f"{r.get('bytes', 0)}B {r.get('sha256', '')[:16]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
