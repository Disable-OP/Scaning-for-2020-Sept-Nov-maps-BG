#!/usr/bin/env python3
"""Grid v3 plan compiler (deterministic, pure function of the frozen spec).

Converts scan/spec_halloween_core.json (the frozen 28.5B-candidate universe)
into an ordered list of claimable work packages:

- Ordering: tiers by their `priority` field; WITHIN a tier the (series, hour)
  ranges are reordered HOUR-MAJOR (all series for hour h before hour h+1),
  hours ranked by corpus-mined upload density, days by release-window weight.
  Rationale: the g1008 family uploads its assets in correlated deploy bursts
  (evidence: v5 anchors - sibling zips within +/-2s, clone packs within
  +/-2min), so sweeping all series of one hour maximises the probability that
  an early fleet-hour contains the event upload. This is a scheduling choice;
  interval arithmetic below guarantees the union of packages equals the
  frozen universe exactly (no gaps, no overlaps, no invention).

- Packaging: consecutive ordinals are cut into packages of ~18M candidates
  (~50-100 min of scanning per package per worker at the AIMD fleet targets),
  the claim/heartbeat/resume unit of the grid.

- Honesty: the compiler never invents candidates. Every ordinal in a package
  maps back to (tier, path_fmt, map_id, ts_ms) of the frozen spec. The output
  plan embeds the spec sha256 and a coverage manifest; test_grid.py verifies
  totals == candidates_emitted and per-tier sums == candidates_per_tier.

CLI:
  python grid_plan.py gen   --spec scan/spec_halloween_core.json \
                            --densities /path/hour_densities.json (optional) \
                            --out scan/grid_plan.json
  python grid_plan.py validate --spec ... --plan scan/grid_plan.json
  python grid_plan.py stats --plan scan/grid_plan.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

PACKAGE_TARGET = 18_000_000


def sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def load_spec(path: Path) -> dict:
    raw = path.read_bytes()
    spec = json.loads(raw)
    spec["_sha256"] = hashlib.sha256(raw).hexdigest()
    return spec


# --------------------------------------------------------------------------
# universe flattening
# --------------------------------------------------------------------------
def tier_ranges(spec: dict) -> list[dict]:
    """Flatten spec tiers -> ordered item pool.

    Each item: {tier, priority, path_fmt, map_id, t0, t1} where [t0, t1) is a
    ms-exact half-open absolute interval. Ranges in the spec are relative to
    `base` (base + start_offset_ms .. base + end_offset_ms)."""
    items = []
    for tier in spec["tiers"]:
        prio = int(tier.get("priority", 99))
        path_fmt = tier.get("path_fmt") or spec["url_path_fmt"]
        for r in tier["ranges"]:
            t0 = int(r["base"]) + int(r["start_offset_ms"])
            t1 = int(r["base"]) + int(r["end_offset_ms"])
            if t1 <= t0:
                continue
            items.append({
                "tier": tier["tier_id"], "priority": prio,
                "path_fmt": path_fmt, "map_id": r["map_id"],
                "t0": t0, "t1": t1,
            })
    return items


def hour_key(t_ms: int) -> int:
    return t_ms // 3_600_000


def day_str(t_ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(t_ms / 1000.0))


def reorder_hour_major(items: list[dict], hour_density: dict[int, float],
                       day_weight: dict[str, float]) -> list[dict]:
    """Within each tier: hour-major order. Hour rank key =
    (-day_weight, day, -hour_density, hour). Ties broken by map_id for
    determinism."""
    def rank(it: dict):
        h = hour_key(it["t0"])
        d = day_str(it["t0"])
        hd = hour_density.get(h % 24, 0.0)
        dw = day_weight.get(d, 0.5)
        return (-dw, d, -hd, h, it["map_id"])

    out = []
    by_tier: dict[str, list[dict]] = {}
    for it in items:
        by_tier.setdefault(it["tier"], []).append(it)
    for tier, group in by_tier.items():
        prio = group[0]["priority"]
        group.sort(key=lambda it: (rank(it), it["t0"]))
        out.extend(group)
    out.sort(key=lambda it: (it["priority"], ))  # stable keeps hour-major inside tier
    return out


def cut_packages(items: list[dict], target: int) -> list[dict]:
    """Cut the ordered item stream into packages of ~target ordinals.
    Items may be split at ms boundaries; ordinal0 = cumulative stream ordinal
    at item start. Each package records its ordinal span [ord0, ord1)."""
    packages = []
    cur, cur_ord, ord_abs = [], 0, 0
    for it in items:
        remaining = it["t1"] - it["t0"]
        src = t0 = it["t0"]
        while remaining > 0:
            room = target - (ord_abs - cur_ord)
            take = min(room, remaining)
            cur.append({"path_fmt": it["path_fmt"], "map_id": it["map_id"],
                        "t0": t0, "t1": t0 + take, "ordinal0": ord_abs})
            ord_abs += take
            t0 += take
            remaining -= take
            if ord_abs - cur_ord >= target:
                packages.append({
                    "pid": f"b{len(packages):05d}",
                    "tier": it["tier"], "priority": it["priority"],
                    "ordinal0": cur_ord, "candidates": ord_abs - cur_ord,
                    "items": cur,
                })
                cur, cur_ord = [], ord_abs
    if cur:
        packages.append({
            "pid": f"b{len(packages):05d}",
            "tier": items[-1]["tier"], "priority": items[-1]["priority"],
            "ordinal0": cur_ord, "candidates": ord_abs - cur_ord,
            "items": cur,
        })
    return packages


# --------------------------------------------------------------------------
# gen / validate / stats
# --------------------------------------------------------------------------
def gen(spec_path: Path, densities_path: Path | None, out_path: Path,
        package_target: int = PACKAGE_TARGET) -> dict:
    spec = load_spec(spec_path)
    items = tier_ranges(spec)

    hour_density: dict[int, float] = {h: 0.0 for h in range(24)}
    day_weight: dict[str, float] = {}
    if densities_path and densities_path.exists():
        d = json.loads(densities_path.read_text())
        for k, v in (d.get("hour_density_utc") or {}).items():
            hour_density[int(k)] = float(v)
        for r in d.get("ranges", []):
            dw = float(r.get("day_weight", 0))
            day_weight[r["day"]] = max(day_weight.get(r["day"], 0.0), dw)

    ordered = reorder_hour_major(items, hour_density, day_weight)
    packages = cut_packages(ordered, package_target)

    # tier id map for per-item tier attribution after packaging (items may
    # straddle tiers only at boundaries; packaging preserves item tier)
    plan = {
        "schema": "grid-plan/1",
        "plan_id": "halloween-2021-grid-1",
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "spec": spec_path.name,
        "spec_sha256": spec["_sha256"],
        "package_target_candidates": package_target,
        "ordering": ("tiers by priority; within tier hour-major "
                     "(day_weight desc, day asc, hour_density desc); "
                     "correlated-deploy burst rationale"),
        "hour_density_utc": {str(h): round(hour_density[h], 6)
                             for h in range(24)},
        "day_weight": {d: round(w, 4) for d, w in sorted(day_weight.items())},
        "candidates_total": sum(p["candidates"] for p in packages),
        "packages": packages,
    }
    body = json.dumps(plan, indent=1, sort_keys=False).encode()
    plan_sha = hashlib.sha256(body).hexdigest()
    out_path.write_bytes(body)
    print(f"plan written: {out_path} ({len(body)/1e6:.1f} MB) "
          f"packages={len(packages)} candidates={plan['candidates_total']:,} "
          f"sha256={plan_sha}")
    return plan


def validate(spec_path: Path, plan_path: Path) -> bool:
    spec = load_spec(spec_path)
    plan = json.loads(plan_path.read_bytes())
    ok = True

    if plan["spec_sha256"] != spec["_sha256"]:
        print(f"FAIL: spec sha mismatch plan={plan['spec_sha256']}")
        ok = False

    # 1. total conservation
    total = sum(p["candidates"] for p in plan["packages"])
    if total != spec["candidates_emitted"]:
        print(f"FAIL: candidates {total:,} != spec {spec['candidates_emitted']:,}")
        ok = False

    # 2. ordinal continuity (no gaps/overlaps) + per-item arithmetic
    expect = 0
    per_tier: dict[str, int] = {}
    for p in plan["packages"]:
        if p["ordinal0"] != expect:
            print(f"FAIL: {p['pid']} ordinal0 {p['ordinal0']} != {expect}")
            ok = False
        span = 0
        for it in p["items"]:
            if it["ordinal0"] != expect + span:
                print(f"FAIL: {p['pid']} item ordinal0 discontinuity")
                ok = False
            span += it["t1"] - it["t0"]
        if span != p["candidates"]:
            print(f"FAIL: {p['pid']} items span {span} != candidates {p['candidates']}")
            ok = False
        expect += p["candidates"]
        per_tier[p["tier"]] = per_tier.get(p["tier"], 0) + p["candidates"]

    # 3. per-tier conservation vs spec candidates_per_tier
    spec_tiers = spec["candidates_per_tier"]
    for t, n in spec_tiers.items():
        got = per_tier.get(t, 0)
        if got != n:
            print(f"FAIL: tier {t}: plan {got:,} != spec {n:,}")
            ok = False

    # 4. replay: regenerate the URL of every Nth candidate and check the
    #    absolute timestamp lies inside the original spec tier range set
    spec_ranges: dict[str, set[tuple[int, int]]] = {}
    for tier in spec["tiers"]:
        for r in tier["ranges"]:
            spec_ranges.setdefault(tier["tier_id"], set()).add(
                (int(r["base"]) + int(r["start_offset_ms"]),
                 int(r["base"]) + int(r["end_offset_ms"])))
    import random
    rnd = random.Random(20211031)
    checked = 0
    for p in plan["packages"]:
        for it in p["items"]:
            if rnd.random() < 0.0005:
                mid = (it["t0"] + it["t1"]) // 2
                if not any(a <= mid < b for a, b in spec_ranges.get(p["tier"], ())):
                    print(f"FAIL: {p['pid']} item ts outside spec ranges")
                    ok = False
                checked += 1
    print(("PASS" if ok else "FAIL") +
          f": packages={len(plan['packages'])} candidates={total:,} "
          f"spot-checked={checked}")
    return ok


def stats(plan_path: Path) -> None:
    plan = json.loads(plan_path.read_bytes())
    per_tier: dict[str, list[int]] = {}
    for p in plan["packages"]:
        per_tier.setdefault(p["tier"], [0, 0])
        per_tier[p["tier"]][0] += p["candidates"]
        per_tier[p["tier"]][1] += 1
    total = sum(v[0] for v in per_tier.values())
    print(f"{'tier':38s} {'candidates':>15s} {'pkgs':>6s} {'h@60k':>8s}")
    for t, (n, k) in per_tier.items():
        print(f"{t:38s} {n:15,d} {k:6d} {n/3.6e6/60000:8.2f}")
    print(f"{'TOTAL':38s} {total:15,d} {len(plan['packages']):6d} "
          f"{total/3.6e6/60000:8.2f}")


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gen")
    g.add_argument("--spec", default="scan/spec_halloween_core.json")
    g.add_argument("--densities", default="")
    g.add_argument("--out", default="scan/grid_plan.json")
    g.add_argument("--package-target", type=int, default=PACKAGE_TARGET)
    v = sub.add_parser("validate")
    v.add_argument("--spec", default="scan/spec_halloween_core.json")
    v.add_argument("--plan", default="scan/grid_plan.json")
    s = sub.add_parser("stats")
    s.add_argument("--plan", default="scan/grid_plan.json")
    a = ap.parse_args()
    if a.cmd == "gen":
        gen(Path(a.spec), Path(a.densities) if a.densities else None,
            Path(a.out), a.package_target)
    elif a.cmd == "validate":
        return 0 if validate(Path(a.spec), Path(a.plan)) else 1
    else:
        stats(Path(a.plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
