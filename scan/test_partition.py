#!/usr/bin/env python3
"""Property tests for the deterministic partitioning (run before launch).

1. coverage:      sum of chunk sizes == universe; ordinal bounds contiguous.
2. disjointness:  no (map_id, ts_ms) pair is owned by two different chunks
                  (checked exactly on full boundary neighbourhoods + a large
                  deterministic sample; structural guarantee: half-open
                  ordinal intervals over a single ordered stream).
3. determinism:   identical candidate streams across two independent
                  subprocesses (fresh interpreters, no shared state).
4. resume:        iter_chunk is a pure function - re-iteration identical.
5. hash crossref: for a sample, sha256(candidate) % TOTAL_CHUNKS yields a
                  well-defined owner label (report-only; the authoritative
                  scheme is interval partitioning, which guarantees exactly
                  one owner by construction).
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen_candidates as G  # noqa: E402

SPEC = Path(__file__).resolve().parent.parent / "scan" / "spec_v1.json"
SAMPLE_CHUNKS = [0, 1, 449, 450, 899, 900]
PROBE_CHUNKS = [0, 17, 300, 640, 900]


def subprocess_stream_digest(spec_path: str, chunk: int) -> str:
    out = subprocess.run([sys.executable, str(Path(__file__).parent / "gen_candidates.py"),
                          "--spec", spec_path, "--chunk", str(chunk)],
                         capture_output=True, text=True, check=True).stdout
    h = hashlib.sha256()
    n = 0
    for line in out.splitlines():
        h.update(line.encode())
        n += 1
    return h.hexdigest(), n


def main() -> int:
    spec = G.load_spec(SPEC)
    total = G.total_candidates(spec)
    nchunks = G.num_chunks(spec)
    cs = spec["chunk_size"]
    failures = []

    # 1. coverage arithmetic
    covered = 0
    prev_end = 0
    for c in range(nchunks):
        s, e, _ = G.chunk_bounds(spec, c)
        if s != prev_end:
            failures.append(f"gap/overlap at chunk {c}: start {s} != prev end {prev_end}")
        prev_end = e
        covered += e - s
    if covered != total or prev_end != total:
        failures.append(f"coverage {covered}/{total}, end {prev_end}")

    # 2. disjointness (content-level) on sampled chunks
    seen: dict[tuple, int] = {}
    for c in SAMPLE_CHUNKS:
        for o, m, ts, _t in G.iter_chunk(spec, c):
            k = (m, ts)
            if k in seen and seen[k] // cs != c:
                failures.append(f"candidate {k} owned by chunks {seen[k]//cs} and {c}")
            seen[k] = o
    # boundary neighbourhood: candidates around each sampled boundary
    for c in range(1, nchunks):
        s, _, _ = G.chunk_bounds(spec, c)
        if c in SAMPLE_CHUNKS or c in (1, nchunks - 1):
            for o in range(max(0, s - 3), s + 3):
                k = G.candidate_at(spec, o)
                key = (k[0], k[1])
                if key in seen:
                    owner_chunk = seen[key] // cs
                    this_chunk = o // cs
                    if owner_chunk != this_chunk:
                        failures.append(f"boundary dup {key}: {owner_chunk} vs {this_chunk}")
                seen[key] = o

    # 3. determinism across processes
    for c in PROBE_CHUNKS:
        d1, n1 = subprocess_stream_digest(str(SPEC), c)
        d2, n2 = subprocess_stream_digest(str(SPEC), c)
        if d1 != d2:
            failures.append(f"chunk {c} nondeterministic across subprocesses")
        local = G.iter_chunk(spec, c)
        n_local = sum(1 for _ in local)
        if n_local != n1:
            failures.append(f"chunk {c} size mismatch lib={n_local} cli={n1}")

    # 4. resume purity
    a = [(o, m, t) for o, m, t, _ in G.iter_chunk(spec, 449)]
    b = [(o, m, t) for o, m, t, _ in G.iter_chunk(spec, 449)]
    if a != b:
        failures.append("iter_chunk not pure")

    # 5. hash crossref (report-only sanity)
    hcount = {}
    for c in PROBE_CHUNKS:
        for o, m, ts, _t in G.iter_chunk(spec, c):
            owner = int(hashlib.sha256(f"{m}.{ts}".encode()).hexdigest(), 16) % nchunks
            hcount.setdefault(owner, 0)
            hcount[owner] += 1
    print(f"hash-crossref: {len(hcount)} distinct owners for "
          f"{sum(hcount.values())} sampled candidates (interval scheme is authoritative)")

    if failures:
        print("FAILURES:")
        for f in failures:
            print(" -", f)
        return 1
    print(json.dumps({
        "result": "ALL PARTITION TESTS PASSED",
        "universe": total, "chunks": nchunks, "chunk_size": cs,
        "spec_sha256": spec["_sha256"],
        "sampled_candidates_checked": len(seen),
    }, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
