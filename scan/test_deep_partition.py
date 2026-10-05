#!/usr/bin/env python3
"""Property tests for the deep-scan v5 universe. MUST pass before launch.

P1 determinism       build_epoch(n) twice -> identical canonical JSON/sha.
P2 no-overlap vs v1  sampled + boundary candidates of every epoch are NOT in
                     the stream-exact v1 covered set; the two disclosed v1-
                     dropped points (m901xS2, m901xS2B) MUST be present in
                     epoch 1.
P3 cross-epoch       sampled candidates of epoch n are NOT covered by
   disjointness      epochs 1..n-1.
P4 intra-epoch       sampled candidates are unique within an epoch stream.
P5 chunk math        iter_chunk(slice) == candidate_at(ordinal) for every
                     sampled chunk; chunk_bounds sizes correct; total =
                     candidates_emitted.
P6 sentinel gap fix  epoch 1 covers m1014_1 around S2/S2B (never probed by
                     v1) and does NOT re-probe m1014_1 around S1/S1B (v1
                     range tiers own those).
P7 frontier walk     walking (epoch, chunk) coordinates by ascending chunk id
                     per epoch covers the epoch stream exactly once.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import deep_universe as D  # noqa: E402
import gen_candidates as G  # noqa: E402

CFG = json.loads(Path("scan/deep_config.json").read_text())
SPEC_V1 = G.load_spec("scan/spec_v1.json")
SAMPLES_PER_EPOCH = 4000


def sample_candidates(spec: dict, n: int):
    """Deterministic sample across the epoch stream (front/middle/tail)."""
    total = spec["candidates_emitted"]
    if total == 0:
        return []
    cs = spec["chunk_size"]
    num = (total + cs - 1) // cs
    picks = []
    step = max(1, total // n)
    for o in range(0, total, step):
        picks.append(o)
        if len(picks) >= n:
            break
    # always include first/last candidate and chunk boundaries
    for extra in (0, total - 1, cs - 1, cs, (num // 2) * cs, (num - 1) * cs):
        if 0 <= extra < total:
            picks.append(extra)
    out = []
    for o in sorted(set(picks)):
        m, ts, _tier = G.candidate_at(spec, o)
        out.append((o, m, ts))
    return out


def main() -> int:
    ladder = CFG["epoch_ladder"]
    v1cov = D.v1_covered(SPEC_V1)
    print(f"v1 stream-exact covered: {D.covered_count(v1cov):,}")

    epochs: dict[int, dict] = {}
    cums: dict[int, dict] = {}
    cum = {m: list(iv) for m, iv in v1cov.items()}
    shas: dict[int, str] = {}

    # ---- P1 determinism + build all epochs
    for ent in ladder:
        n = ent["epoch"]
        s1, cum1 = D.build_epoch(CFG, SPEC_V1, n, _v1cov_cache=v1cov)
        s2, cum2 = D.build_epoch(CFG, SPEC_V1, n, _v1cov_cache=v1cov)
        assert s1 is not None, f"epoch {n} build failed"
        a = json.dumps(s1, sort_keys=True, separators=(",", ":"))
        b = json.dumps(s2, sort_keys=True, separators=(",", ":"))
        assert a == b, f"P1 FAIL determinism epoch {n}"
        shas[n] = D.epoch_sha256(s1)
        epochs[n] = s1
        cums[n] = cum1
        cum = {m: list(iv) for m, iv in cum1.items()}
    print(f"P1 PASS determinism ({len(epochs)} epochs, shas stable)")

    # ---- P2 no overlap with v1 (+ disclosed duplicate points present in ep1)
    for n, spec in epochs.items():
        cands = sample_candidates(spec, SAMPLES_PER_EPOCH)
        bad = [(o, m, ts) for o, m, ts in cands if D._contains(v1cov, m, ts)]
        assert not bad, f"P2 FAIL epoch {n}: {len(bad)} candidates inside v1 " \
                        f"e.g. {bad[:3]}"
    print(f"P2 PASS no v1 overlap ({SAMPLES_PER_EPOCH} samples x {len(epochs)} epochs)")

    # ---- P3 cross-epoch disjointness
    prior = {m: list(iv) for m, iv in v1cov.items()}
    for n in sorted(epochs):
        spec = epochs[n]
        cands = sample_candidates(spec, SAMPLES_PER_EPOCH)
        bad = [(o, m, ts) for o, m, ts in cands if D._contains(prior, m, ts)]
        assert not bad, f"P3 FAIL epoch {n}: {len(bad)} candidates covered by " \
                        f"prior epochs e.g. {bad[:3]}"
        # advance prior with this epoch's emission (exact from spec ranges)
        for r in spec["tiers"][0]["ranges"]:
            prior.setdefault(r["map_id"], []).append(
                (r["base"] + r["start_offset_ms"],
                 r["base"] + r["end_offset_ms"]))
        prior = {m: D._merge(iv) for m, iv in prior.items()}
    print("P3 PASS cross-epoch disjointness")

    # ---- P4 intra-epoch uniqueness
    for n, spec in epochs.items():
        cands = sample_candidates(spec, SAMPLES_PER_EPOCH)
        keys = [(m, ts) for _o, m, ts in cands]
        assert len(keys) == len(set(keys)), f"P4 FAIL epoch {n}: duplicates"
    print("P4 PASS intra-epoch uniqueness")

    # ---- P5 chunk math
    for n, spec in epochs.items():
        total = spec["candidates_emitted"]
        cs = spec["chunk_size"]
        num = (total + cs - 1) // cs
        assert total == sum(max(0, r["end_offset_ms"] - r["start_offset_ms"])
                            for t in spec["tiers"] for r in t["ranges"])
        for cid in {0, 1, num // 2, num - 2, num - 1}:
            if cid < 0 or cid >= num:
                continue
            s, e, tot = G.chunk_bounds(spec, cid)
            assert tot == total and e > s
            stream = [(o, m, ts) for o, m, ts, _t in G.iter_chunk(spec, cid)]
            assert stream[0][0] == s and stream[-1][0] == e - 1
            for o, m, ts in stream[:: max(1, len(stream) // 50)]:
                m2, ts2, _t = G.candidate_at(spec, o)
                assert (m, ts) == (m2, ts2), f"P5 FAIL epoch {n} ordinal {o}"
    print("P5 PASS chunk math (bounds + stream consistency)")

    # ---- P6 sentinel gap fix
    ep1 = epochs[1]
    S2 = CFG["anchors_ms"]["S2"]
    S2B = CFG["anchors_ms"]["S2B"]
    S1 = CFG["anchors_ms"]["S1"]
    S1B = CFG["anchors_ms"]["S1B"]

    def ep1_has(m: str, ts: int) -> bool:
        for r in ep1["tiers"][0]["ranges"]:
            if r["map_id"] == m and r["base"] <= ts < r["base"] + r["end_offset_ms"]:
                return True
        return False

    assert ep1_has("m1014_1", S2), "P6 FAIL: m1014_1xS2 window missing"
    assert ep1_has("m1014_1", S2B - 1000) and ep1_has("m1014_1", S2B + 1000), \
        "P6 FAIL: m1014_1xS2B neighborhood incomplete"
    assert not ep1_has("m1014_1", S1), "P6 FAIL: m1014_1xS1 must stay v1-owned"
    assert not ep1_has("m1014_1", S1 + 10000), "P6 FAIL: v1 range re-probed"
    assert ep1_has("m901", S2), "P2/P6 FAIL: disclosed m901xS2 point missing"
    assert ep1_has("m901", S2B), "P2/P6 FAIL: disclosed m901xS2B point missing"
    assert not ep1_has("m901", S1), "P6 FAIL: m901xS1 is v1-owned"
    print("P6 PASS sentinel gap fix (m1014_1 S2/S2B covered; v1 zones untouched; "
          "m901 S2/S2B re-probe present)")

    # ---- P7 frontier walk covers stream exactly once
    for n, spec in epochs.items():
        total = spec["candidates_emitted"]
        cs = spec["chunk_size"]
        num = (total + cs - 1) // cs
        seen = 0
        for cid in range(num):
            s, e, _t = G.chunk_bounds(spec, cid)
            seen += e - s
        assert seen == total, f"P7 FAIL epoch {n}: {seen} != {total}"
    print("P7 PASS frontier walk (chunk partition covers every epoch exactly)")

    total_all = sum(s["candidates_emitted"] for s in epochs.values())
    chunks_all = sum((s["candidates_emitted"] + s["chunk_size"] - 1) // s["chunk_size"]
                     for s in epochs.values())
    print(json.dumps({
        "result": "ALL PROPERTIES PASS",
        "epochs": len(epochs),
        "total_candidates": total_all,
        "total_chunks": chunks_all,
        "chunk_size": CFG["chunk_size"],
        "epoch_shas": {str(k): v[:16] + "..." for k, v in shas.items()},
    }, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
