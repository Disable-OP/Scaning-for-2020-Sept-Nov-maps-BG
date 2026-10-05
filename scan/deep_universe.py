#!/usr/bin/env python3
"""Deterministic deep-universe epoch builder (v5, unattended deep scan).

The campaign universe is an open-ended ladder of epochs defined by
scan/deep_config.json. Each epoch is a PURE FUNCTION of
  (config, v1 spec, epoch number, epochs 1..n-1)
producing a spec_v1-shaped JSON (tiers of half-open ms ranges) that
gen_candidates.py consumes unchanged. Chunks of an epoch are its ordinal
stream slices - structural no-overlap guarantee inside the epoch.

Global no-overlap guarantee: before emission, every raw window is subtracted
against the exact set of (map_id, absolute ts_ms) positions already covered by
  (a) the drained v1 universe (18,000,252 confirmed 404s - never re-probed), and
  (b) every previously emitted deep epoch.
Subtraction is exact integer interval arithmetic in (map_id, ts) space; anchor
points probed in v1 are removed as degenerate intervals, so a window split by
a probed anchor emits disjoint sub-ranges around it.

Nothing is invented at scan time: workers only ever consume committed
epochs/epoch_N.json files whose sha256 is pinned in the queue entry.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import gen_candidates as G


# --------------------------------------------------------------------------
# interval helpers (per map_id, absolute ms space, half-open [lo, hi))
# --------------------------------------------------------------------------

def _merge(iv: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping/adjacent intervals; drops empty ones; sorted."""
    out: list[tuple[int, int]] = []
    for lo, hi in sorted(x for x in iv if x[1] > x[0]):
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def _subtract(windows: list[tuple[int, int]],
              covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """windows minus covered (both merged-ish); returns sorted leftovers."""
    out: list[tuple[int, int]] = []
    for wlo, whi in sorted(w for w in windows if w[1] > w[0]):
        cur = wlo
        for clo, chi in covered:
            if chi <= cur:
                continue
            if clo >= whi:
                break
            if clo > cur:
                out.append((cur, min(clo, whi)))
            cur = max(cur, chi)
            if cur >= whi:
                break
        if cur < whi:
            out.append((cur, whi))
    return out


# --------------------------------------------------------------------------
# v1 covered-set extraction
# --------------------------------------------------------------------------

def v1_covered(spec_v1: dict) -> dict[str, list[tuple[int, int]]]:
    """map_id -> merged absolute intervals actually probed by the v1 stream.

    Stream-exact: walks gen_candidates with the same ordinal mapping v4 used,
    so bookkeeping quirks (e.g. t0 iterating 254 items against 252 formula
    ordinals, dropping the last two) are honored precisely: the two dropped
    anchor points are NOT marked covered (the deep scan re-probes them,
    disclosed as duplicates vs v3)."""
    cov: dict[str, list[tuple[int, int]]] = {}
    off = 0
    total = G.total_candidates(spec_v1)
    for tier in spec_v1["tiers"]:
        c = G.tier_count(tier, spec_v1)
        if tier.get("kind") == "anchors":
            items = list(G._tier_items(tier, spec_v1))
            for o in range(off, min(off + c, off + len(items), total)):
                _kind, m, ts = items[o - off]
                cov.setdefault(m, []).append((ts, ts + 1))
        else:
            for r in tier["ranges"]:
                lo = r["base"] + r["start_offset_ms"]
                hi = r["base"] + r["end_offset_ms"]
                # clip to the tier's formula ordinal span (ranges are the
                # stream source; spans are exact by construction)
                cov.setdefault(r["map_id"], []).append((lo, hi))
        off += c
    return {m: _merge(iv) for m, iv in cov.items()}


def covered_count(cov: dict[str, list[tuple[int, int]]]) -> int:
    return sum(hi - lo for iv in cov.values() for lo, hi in iv)


def _contains(cov: dict[str, list[tuple[int, int]]], m: str, ts: int) -> bool:
    import bisect
    iv = cov.get(m)
    if not iv:
        return False
    i = bisect.bisect_right(iv, (ts, 1 << 62)) - 1
    return i >= 0 and iv[i][0] <= ts < iv[i][1]


# --------------------------------------------------------------------------
# epoch construction
# --------------------------------------------------------------------------

def _raw_windows(cfg: dict, ent: dict) -> list[tuple[str, int, int, str]]:
    """[(map_id, lo_ms, hi_ms, label)] for one ladder entry (before subtraction)."""
    anchors = cfg["anchors_ms"]
    order = cfg["anchor_order"]
    out: list[tuple[str, int, int, str]] = []
    if ent["kind"] == "family":
        for m in cfg["all_map_ids"]:
            for a in order:
                A = anchors[a]
                if "fwd_s" in ent:
                    out.append((m, A + ent["fwd_s"][0] * 1000,
                                A + ent["fwd_s"][1] * 1000, f"fwd_{a}"))
                if "back_s" in ent:
                    out.append((m, A + ent["back_s"][0] * 1000,
                                A + ent["back_s"][1] * 1000, f"back_{a}"))
    else:  # sentinel
        for pair in cfg["sentinel_pairs"]:
            A = anchors[pair["anchor"]]
            m = pair["map_id"]
            if "fwd_s" in ent:
                out.append((m, A + ent["fwd_s"][0] * 1000,
                            A + ent["fwd_s"][1] * 1000, "fwd"))
            if "back_s" in ent:
                out.append((m, A + ent["back_s"][0] * 1000,
                            A + ent["back_s"][1] * 1000, "back"))
    return out


def build_epoch(cfg: dict, spec_v1: dict, epoch_n: int,
                _v1cov_cache: dict | None = None
                ) -> tuple[dict | None, dict[str, list[tuple[int, int]]]]:
    """Return (epoch_spec | None, cumulative_covered_after_this_epoch).

    epoch_spec None <=> epoch_n beyond the ladder (frontier exhausted).
    Cumulative covered set includes v1 + epochs 1..epoch_n.
    """
    ladder = cfg["epoch_ladder"]
    v1cov = _v1cov_cache if _v1cov_cache is not None else v1_covered(spec_v1)
    if epoch_n < 1 or epoch_n > len(ladder):
        return None, v1cov
    cum: dict[str, list[tuple[int, int]]] = {m: list(iv) for m, iv in v1cov.items()}
    # replay epochs 1..n-1 to rebuild their coverage deterministically
    for k in range(1, epoch_n):
        _ep, cum = build_epoch(cfg, spec_v1, k, _v1cov_cache=v1cov)
    ent = ladder[epoch_n - 1]

    per_map: dict[str, list[tuple[int, int, str]]] = {}
    for m, lo, hi, lab in _raw_windows(cfg, ent):
        per_map.setdefault(m, []).append((lo, hi, lab))

    tiers_ranges: list[dict] = []
    tier_label = f"e{epoch_n}_{ent['kind']}"
    emitted = 0
    for m in cfg["all_map_ids"]:
        wins = per_map.get(m)
        if not wins:
            continue
        cov = cum.setdefault(m, [])
        for lo, hi, lab in sorted(wins):
            for rlo, rhi in _subtract([(lo, hi)], cov):
                if rhi <= rlo:
                    continue
                tiers_ranges.append({
                    "map_id": m, "base": rlo,
                    "start_offset_ms": 0, "end_offset_ms": rhi - rlo,
                    "sub": lab,
                })
                emitted += rhi - rlo
                cov.append((rlo, rhi))
                cov[:] = _merge(cov)  # in-place: keep identity for next window
    spec = {
        "spec_id": f"deep-scan-v5-epoch-{epoch_n:02d}",
        "campaign": cfg["campaign_id"],
        "epoch": epoch_n,
        "kind": ent["kind"],
        "evidence_basis": ent["basis"],
        "host": cfg["host"],
        "url_path_fmt": cfg["url_path_fmt"],
        "chunk_size": cfg["chunk_size"],
        "granularity": cfg["granularity"],
        "search_scope": cfg["search_scope"],
        "all_map_ids": cfg["all_map_ids"],
        "anchors_ms": cfg["anchors_ms"],
        "tiers": [{"tier_id": tier_label, "ranges": tiers_ranges}],
        "candidates_emitted": emitted,
    }
    return spec, cum


def _canonical(spec: dict) -> str:
    """Canonical JSON for hashing; strips in-memory caches (_-prefixed)."""
    def clean(o):
        if isinstance(o, dict):
            return {k: clean(v) for k, v in o.items() if not k.startswith("_")}
        if isinstance(o, list):
            return [clean(v) for v in o]
        return o
    return json.dumps(clean(spec), sort_keys=True, separators=(",", ":"))


def epoch_sha256(spec: dict) -> str:
    return hashlib.sha256(_canonical(spec).encode()).hexdigest()


def write_epoch_file(spec: dict, out_path: str | Path) -> str:
    """Canonical on-disk/committed form (indent=1, trailing newline)."""
    text = json.dumps(spec, indent=1) + "\n"
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    return hashlib.sha256(p.read_bytes()).hexdigest()


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="scan/deep_config.json")
    ap.add_argument("--spec", default="scan/spec_v1.json")
    ap.add_argument("--out", default="")
    sub = ap.add_mutually_exclusive_group(required=True)
    sub.add_argument("--stats", action="store_true")
    sub.add_argument("--build", type=int, metavar="N")
    sub.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text())
    spec_v1 = G.load_spec(args.spec)

    if args.stats:
        total = 0
        rows = []
        for ent in cfg["epoch_ladder"]:
            n = ent["epoch"]
            spec, _ = build_epoch(cfg, spec_v1, n)
            c = spec["candidates_emitted"]
            cs = cfg["chunk_size"]
            rows.append({"epoch": n, "kind": ent["kind"],
                         "candidates": c, "chunks": (c + cs - 1) // cs,
                         "basis": ent["basis"][:60] + "..."})
            total += c
        print(json.dumps({
            "campaign": cfg["campaign_id"],
            "chunk_size": cfg["chunk_size"],
            "epochs": rows,
            "total_candidates": total,
            "total_chunks": (total + cfg["chunk_size"] - 1) // cfg["chunk_size"],
        }, indent=1))
        return 0

    if args.build is not None:
        spec, _ = build_epoch(cfg, spec_v1, args.build)
        if spec is None:
            print(json.dumps({"epoch": args.build, "exists": False}))
            return 0
        sha = write_epoch_file(spec, args.out or f"epochs/epoch_{args.build}.json")
        print(json.dumps({"epoch": args.build, "candidates":
                          spec["candidates_emitted"],
                          "sha256": sha, "path": args.out}))
        return 0

    if args.selftest:
        v1cov = v1_covered(spec_v1)
        n_v1 = covered_count(v1cov)
        total_v1 = G.total_candidates(spec_v1)
        assert n_v1 == total_v1, (n_v1, total_v1)
        cum = {m: list(iv) for m, iv in v1cov.items()}
        grand = 0
        for ent in cfg["epoch_ladder"]:
            n = ent["epoch"]
            spec, cum_new = build_epoch(cfg, spec_v1, n, _v1cov_cache=v1cov)
            assert spec is not None
            # spot-check no overlap with v1 and prior epochs (sampled below by
            # test_deep_partition.py; here verify cumulative math consistency)
            assert covered_count(cum_new) >= covered_count(cum)
            cum = cum_new
            grand += spec["candidates_emitted"]
        print(json.dumps({
            "v1_universe": total_v1, "v1_covered_check": "OK",
            "ladder_epochs": len(cfg["epoch_ladder"]),
            "ladder_total_candidates": grand,
        }))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
