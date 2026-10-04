#!/usr/bin/env python3
"""Deterministic candidate stream generator + interval chunker.

The candidate universe is fully defined by scan/spec_v1.json (frozen, hashed).
The stream is an ordered, pure function of the spec: tier list order, ranges
order, timestamps ascending within each range. Chunk k owns exactly the
half-open ordinal interval [k*chunk_size, (k+1)*chunk_size) - structural
no-overlap guarantee. Every restarted worker regenerates identical candidates
from the same spec digest.

No candidate is ever invented at scan time: anything not derivable from the
spec must go through a committed retry-batch file (explicit candidate list).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def load_spec(path: str | Path) -> dict:
    p = Path(path)
    raw = p.read_bytes()
    spec = json.loads(raw)
    spec["_sha256"] = hashlib.sha256(raw).hexdigest()
    return spec


def _tier_items(tier: dict, spec: dict):
    """Yield ('range', map_id, ts_ms) or ('anchor', map_id, ts_ms) lazily in order."""
    if tier.get("kind") == "anchors":
        ids = spec["all_map_ids"]
        anchors = [spec["anchors_ms"][a] for a in tier["anchors"]]
        exclude = {(e["map_id"], spec["anchors_ms"][a])
                   for e in tier.get("exclude", []) for a in e["anchors"]}
        for m in ids:
            for a in anchors:
                if (m, a) in exclude:
                    continue
                yield ("anchor", m, a)
        return
    for r in tier["ranges"]:
        base = r["base"]
        for ts in range(base + r["start_offset_ms"], base + r["end_offset_ms"]):
            yield ("range", r["map_id"], ts)


def tier_count(tier: dict, spec: dict) -> int:
    if tier.get("kind") == "anchors":
        exclude_n = sum(len(e["anchors"]) for e in tier.get("exclude", []))
        return len(spec["all_map_ids"]) * len(tier["anchors"]) - exclude_n
    return sum(max(0, r["end_offset_ms"] - r["start_offset_ms"]) for r in tier["ranges"])


def tier_offsets(spec: dict) -> list[tuple[str, int, int]]:
    """[(tier_id, start_ordinal, count)] in stream order."""
    out, off = [], 0
    for t in spec["tiers"]:
        c = tier_count(t, spec)
        out.append((t["tier_id"], off, c))
        off += c
    return out


def total_candidates(spec: dict) -> int:
    return sum(tier_count(t, spec) for t in spec["tiers"])


def num_chunks(spec: dict) -> int:
    return (total_candidates(spec) + spec["chunk_size"] - 1) // spec["chunk_size"]


def candidate_at(spec: dict, ordinal: int) -> tuple[str, int, str]:
    """Return (map_id, ts_ms, tier_id) for stream ordinal. O(#tiers*log(range))"""
    if ordinal < 0 or ordinal >= total_candidates(spec):
        raise IndexError(f"ordinal {ordinal} outside universe")
    for t in spec["tiers"]:
        c = tier_count(t, spec)
        if ordinal < c:
            if t.get("kind") == "anchors":
                items = list(_tier_items(t, spec))
                _kind, m, a = items[ordinal]
                return m, a, t["tier_id"]
            for r in t["ranges"]:
                span = r["end_offset_ms"] - r["start_offset_ms"]
                if ordinal < span:
                    return r["map_id"], r["base"] + r["start_offset_ms"] + ordinal, t["tier_id"]
                ordinal -= span
            raise IndexError("unreachable")
        ordinal -= c
    raise IndexError("unreachable")


def iter_chunk(spec: dict, chunk_id: int):
    """Yield (ordinal, map_id, ts_ms, tier_id) for one chunk's half-open slice."""
    cs = spec["chunk_size"]
    total = total_candidates(spec)
    start = chunk_id * cs
    end = min(start + cs, total)
    if start >= total:
        raise IndexError(f"chunk {chunk_id} empty (universe={total})")
    for o in range(start, end):
        m, ts, tier = candidate_at(spec, o)
        yield o, m, ts, tier


def chunk_bounds(spec: dict, chunk_id: int) -> tuple[int, int, int]:
    cs = spec["chunk_size"]
    total = total_candidates(spec)
    start = chunk_id * cs
    return start, min(start + cs, total), total


def load_retry_batch(path: str | Path) -> list[dict]:
    """Retry batch file: JSON {"batch_id":..., "candidates":[{"map_id","ts_ms","source_chunk","source_ordinal","prior_status":[...]}]}"""
    d = json.loads(Path(path).read_text())
    return d["candidates"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--spec", required=True)
    sub = ap.add_mutually_exclusive_group(required=True)
    sub.add_argument("--stats", action="store_true")
    sub.add_argument("--chunk", type=int)
    sub.add_argument("--chunk-info", type=int)
    sub.add_argument("--candidate-at", type=int)
    args = ap.parse_args()
    spec = load_spec(args.spec)
    if args.stats:
        per = {tid: {"start_ordinal": s, "count": c}
               for tid, s, c in tier_offsets(spec)}
        print(json.dumps({
            "spec_sha256": spec["_sha256"],
            "total_candidates": total_candidates(spec),
            "chunk_size": spec["chunk_size"],
            "num_chunks": num_chunks(spec),
            "tiers": per,
        }, indent=2))
        return 0
    if args.chunk is not None:
        w = sys.stdout.write
        for o, m, ts, tier in iter_chunk(spec, args.chunk):
            w(f"{o} {m} {ts} {tier}\n")
        return 0
    if args.chunk_info is not None:
        s, e, tot = chunk_bounds(spec, args.chunk_info)
        print(json.dumps({"chunk": args.chunk_info, "start_ordinal": s,
                          "end_ordinal": e, "size": e - s, "universe": tot}))
        return 0
    if args.candidate_at is not None:
        m, ts, tier = candidate_at(spec, args.candidate_at)
        print(json.dumps({"ordinal": args.candidate_at, "map_id": m,
                          "ts_ms": ts, "tier_id": tier}))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
