#!/usr/bin/env python3
"""Grid v3 controller: fleet reconcile + budget governor + cascade + dispatch.

Replaces the v5/v6 queue.json mediator (which did not scale past ~1000 chunks)
with a thin reconciler over worker shards:

- Coverage truth: grid_state/shards/*.json (one file per worker run, written
  by workers). The controller aggregates them into grid_state/coverage.json
  {done: [pid], partial: {pid: max_probed_through}} - the fast path workers
  use to skip/resume packages.

- Claims: grid_state/claims/{pid}.json. The controller releases claim files
  whose pid is done or that no fresh worker is currently scanning (the
  worker's shard `current_pid` is the liveness signal). A released claim that
  a live worker just re-claimed causes at worst a bounded duplicate scan.

- Cascade: every verified hit (zip_ok+crc_ok in a shard) injects priority
  packages into grid_state/grid_plan_cascade.json (append-only, revisioned):
  c1 = same-path-prefix family siblings +/-90s and map bundles +/-2.5s around
  the anchor ts (corpus evidence: clone packs within +/-2min, bundle slots
  within seconds), c2 = all 124 series +/-600s. The controller dispatches an
  extra grid run for fresh cascades so the fleet pivots immediately.

- Budget governor: fleet-wide 5xx/err fractions from worker window telemetry
  drive the global budget_rps in global_rate.json (bounds 10k..120k). Workers
  divide the budget by the active-worker hint (AIMD per-worker cap).

- Dispatch: when no grid run is active (or a fresh cascade exists and fewer
  than 2 runs are live), dispatch grid.yml with a dynamic matrix of up to 20
  workers x 300 minutes each.

- GitHub API discipline: ~1 tree call + <= 20 shard reads + a few commits per
  cycle; cron every 10 min plus worker-done triggers.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gh import GH  # noqa: E402

STATE_BRANCH = "scan-state"
SHARD_DIR = "grid_state/shards"
CLAIM_DIR = "grid_state/claims"
COVERAGE_PATH = "grid_state/coverage.json"
OVERLAY_PATH = "grid_state/grid_plan_cascade.json"
STATUS_PATH = "monitoring/grid-status.json"
BUDGET_MIN, BUDGET_MAX = 10_000.0, 120_000.0
WORKER_FRESH_S = 15 * 60
CLAIM_STALE_S = 25 * 60
SHARD_PRUNE_S = 48 * 3600
CASCADE_PACKAGE_TARGET = 18_000_000


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(s: str) -> float:
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ") \
            .replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0.0


def log(msg: str) -> None:
    print(f"[controller {now_utc()}] {msg}", flush=True)


def parse_pid(pid: str) -> tuple[str, int]:
    if pid.startswith("b"):
        return ("base", int(pid[1:]))
    kind, n = pid.split("-", 1)
    return (kind, int(n))


def merge_ranges(rs: list) -> list[list[int]]:
    """Normalize [[a,b],...] into sorted, coalesced ranges."""
    out: list[list[int]] = []
    for a, b in sorted((int(x), int(y)) for x, y in (rs or [])):
        if out and a <= out[-1][1] + 1:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return out


def merge_partials(recs: list[dict]) -> dict:
    """Merge partial results for one pid from multiple worker shards.

    The claimant with the highest probed_through defines the covered prefix
    (its own recorded dirt stays as retry debt). Other claimants' dirt is
    kept only where it lies beyond that prefix (clipped), because the prefix
    ordinals were already completed by the max claimant.
    """
    if not recs:
        return {"through": 0, "dirty": []}
    best = max(recs, key=lambda r: int(r.get("probed_through", 0)))
    through = int(best.get("probed_through", 0))
    dirty = [list(r) for r in (best.get("dirty_ranges") or [])]
    for r in recs:
        if r is best:
            continue
        for a, b in (r.get("dirty_ranges") or []):
            a, b = int(a), int(b)
            a2 = max(a, through)
            if a2 <= b:
                dirty.append([a2, b])
    return {"through": through, "dirty": merge_ranges(dirty)}


def claim_sort_key(pid: str):
    kind, n = parse_pid(pid)
    # cascade first (c1 before c2), then base plan order
    prio = {"c1": 0, "c2": 1}.get(kind, 2)
    return (prio, n)


# --------------------------------------------------------------------------
def load_series_tables(spec: dict, plan: dict):
    """(family_maps, family_plugins, all_series) as map_id -> path_fmt."""
    fam_maps, fam_plugins = {}, {}
    for p in plan["packages"]:
        if not p["tier"].startswith("h1"):
            continue
        for it in p["items"]:
            if "/maps/" in it["path_fmt"]:
                fam_maps.setdefault(it["map_id"], it["path_fmt"])
            elif "/plugins/" in it["path_fmt"]:
                fam_plugins.setdefault(it["map_id"], it["path_fmt"])
    all_series = {}
    for tier in spec["tiers"]:
        fmt = tier.get("path_fmt") or spec["url_path_fmt"]
        for r in tier["ranges"]:
            all_series.setdefault(r["map_id"], fmt)
    return fam_maps, fam_plugins, all_series


def cut_cascade(items: list[dict], prefix: str, seq_start: int) -> list[dict]:
    """Cut ordered absolute windows into packages <= CASCADE_PACKAGE_TARGET."""
    pkgs, cur, cur_n, seq = [], [], 0, seq_start
    for it in items:
        n = it["t1"] - it["t0"]
        if cur_n + n > CASCADE_PACKAGE_TARGET and cur:
            pkgs.append({"pid": f"{prefix}-{seq:05d}", "tier": f"cascade-{prefix}",
                         "priority": 0, "ordinal0": 0, "candidates": cur_n,
                         "items": cur})
            seq += 1
            cur, cur_n = [], 0
        cur.append({"path_fmt": it["path_fmt"], "map_id": it["map_id"],
                    "t0": it["t0"], "t1": it["t1"], "ordinal0": cur_n})
        cur_n += n
    if cur:
        pkgs.append({"pid": f"{prefix}-{seq:05d}", "tier": f"cascade-{prefix}",
                     "priority": 0, "ordinal0": 0, "candidates": cur_n,
                     "items": cur})
    return pkgs


def build_cascade(spec: dict, plan: dict, hit: dict, seq_start: int):
    """c1: family siblings +/-90s (+ map bundles +/-2.5s for map hits)
    c2: all 124 series +/-600s (overlap with c1 disclosed)."""
    fam_maps, fam_plugins, all_series = load_series_tables(spec, plan)
    mid = hit["map_id"]
    ts = int(hit["ts_ms"])
    url = hit.get("url", "")
    c1 = []
    hit_is_map = "/maps/" in url
    hit_is_plugin = "/plugins/" in url
    for source, is_hit_group in (
            (fam_maps, hit_is_map), (fam_plugins, hit_is_plugin)):
        for s, fmt in sorted(source.items()):
            if s == mid:
                continue
            if is_hit_group:
                # m-bundles get their own tight window below
                if "/maps/" in fmt and s.startswith("m"):
                    continue
                c1.append({"map_id": s, "path_fmt": fmt,
                           "t0": ts - 90_000, "t1": ts + 90_000})
    if hit_is_map:
        for s, fmt in sorted(fam_maps.items()):
            if s != mid and s.startswith("m"):
                c1.append({"map_id": s, "path_fmt": fmt,
                           "t0": ts - 2_500, "t1": ts + 2_500})
    c2 = [{"map_id": s, "path_fmt": fmt,
           "t0": ts - 600_000, "t1": ts + 600_000}
          for s, fmt in sorted(all_series.items()) if s != mid]
    pkgs = cut_cascade(c1, "c1", seq_start)
    nxt = seq_start + len(pkgs)
    pkgs += cut_cascade(c2, "c2", nxt)
    return pkgs


# --------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-branch", default=STATE_BRANCH)
    ap.add_argument("--plan", default="scan/grid_plan.json")
    ap.add_argument("--spec", default="scan/spec_halloween_core.json")
    ap.add_argument("--max-active-workers", type=int, default=20)
    ap.add_argument("--worker-minutes", type=int, default=300)
    args = ap.parse_args()

    owner, repo = os.environ.get("GITHUB_REPOSITORY", "local/local").split("/")
    gh = GH(os.environ.get("GITHUB_TOKEN", ""), owner, repo)
    branch = args.state_branch

    def read_json(path: str):
        try:
            d = gh.get_file_json(path, branch)
            return d
        except Exception:
            return None

    def commit(files: dict, msg: str) -> bool:
        try:
            gh.commit_files(branch, files, msg)
            return True
        except Exception as e:
            log(f"commit failed: {e}")
            return False

    # ---- 0. kill switch -------------------------------------------------
    if gh.get_file("STOP", branch) is not None:
        log("STOP file present on state branch - no action")
        return 0

    plan = json.loads(Path(args.plan).read_bytes())
    spec = json.loads(Path(args.spec).read_bytes())
    tier_of_pid = {p["pid"]: p["tier"] for p in plan["packages"]}
    base_candidates = {p["pid"]: p["candidates"] for p in plan["packages"]}

    # ---- 1. inventory via one recursive tree ----------------------------
    st, tree, _ = gh.api("GET",
                         f"/repos/{owner}/{repo}/git/trees/{branch}?recursive=1")
    paths = []
    if st == 200:
        paths = [e["path"] for e in tree.get("tree", [])
                 if e.get("type") == "blob"]
    shard_paths = [p for p in paths if p.startswith(SHARD_DIR + "/")
                   and p.endswith(".json")]
    log(f"state tree: {len(paths)} blobs, {len(shard_paths)} shards")

    # ---- 2. aggregate shards --------------------------------------------
    shards = []
    for sp in shard_paths:
        d = read_json(sp)
        if isinstance(d, dict):
            d["_path"] = sp
            shards.append(d)
    now = time.time()
    fresh_workers = []
    covered_done: set[str] = set()
    partial_recs: dict[str, list[dict]] = {}
    verified_hits: list[dict] = []
    probed_total = 0
    fleet_bad = [0, 0]  # bad, all
    for sh in shards:
        hb = parse_utc(sh.get("heartbeat_utc", ""))
        fresh = (now - hb) < WORKER_FRESH_S if hb else False
        if fresh:
            fresh_workers.append(sh)
        for pid, rec in (sh.get("packages") or {}).items():
            if not isinstance(rec, dict):
                continue
            if rec.get("status") == "done":
                covered_done.add(pid)
            else:
                partial_recs.setdefault(pid, []).append(rec)
            probed_total += int(rec.get("probed_unique", 0))
            for h in rec.get("found") or []:
                if h.get("zip_ok") and h.get("zip_crc_ok"):
                    verified_hits.append(h)
        for w in sh.get("windows_tail") or []:
            try:
                rps, n200, n404, n503, nto, nne = w[1], w[2], w[3], w[4], w[5], w[6]
                alln = max(1, int(round(rps * w[0])))
                fleet_bad[1] += alln
                fleet_bad[0] += n503 + nto + nne
            except Exception:
                continue
    # prune stale shards
    prune = [sh["_path"] for sh in shards
             if now - (parse_utc(sh.get("heartbeat_utc", "")) or 0) > SHARD_PRUNE_S]

    # ---- 3. release stale claims (git refs; atomic CAS tokens) -----------
    # Claims are refs/claims/grid/{pid} created by workers (201=won). They are
    # ephemeral: release (delete) refs whose pid is done or that no fresh
    # worker is currently scanning. Coverage is truth; a released claim that a
    # live worker just won causes at worst a bounded duplicate scan.
    busy_pids = {sh.get("current_pid") for sh in fresh_workers
                 if sh.get("current_pid")}
    st, refs, _ = gh.api("GET",
                         f"/repos/{owner}/{repo}/git/matching-refs/claims/grid")
    live_claims = []
    if st == 200 and isinstance(refs, list):
        for e in refs:
            name = e.get("ref", "")
            if name.startswith("refs/claims/grid/"):
                live_claims.append(name.rsplit("/", 1)[-1])
    release = [p for p in sorted(set(live_claims)) if p not in busy_pids]
    released = 0
    for pid in release:
        st, _, _ = gh.api(
            "DELETE", f"/repos/{owner}/{repo}/git/refs/claims/grid/{pid}")
        if st in (200, 204):
            released += 1
    if released:
        log(f"released {released} claim refs (not busy)")

    # ---- 4. cascade for new verified hits --------------------------------
    overlay = read_json(OVERLAY_PATH) or {
        "schema": "grid-cascade/1", "revision": 0, "updated_utc": now_utc(),
        "incorporated_hits": [], "next_seq": 0, "packages": []}
    known_hits = set(overlay.get("incorporated_hits") or [])
    new_hits = [h for h in verified_hits if h.get("url") not in known_hits]
    fresh_cascade = False
    if new_hits:
        seq = int(overlay.get("next_seq", 0))
        added = []
        for h in new_hits[:4]:  # cap per cycle (API budget)
            pkgs = build_cascade(spec, plan, h, seq)
            seq += len(pkgs)
            added.extend(pkgs)
            known_hits.add(h.get("url"))
        overlay["packages"] = list(overlay.get("packages") or []) + added
        overlay["next_seq"] = seq
        overlay["incorporated_hits"] = sorted(known_hits)[-64:]
        overlay["revision"] = int(overlay.get("revision", 0)) + 1
        overlay["updated_utc"] = now_utc()
        commit({OVERLAY_PATH: json.dumps(overlay, indent=1) + "\n"},
               f"cascade r{overlay['revision']}: +{len(added)} packages for "
               f"{len(new_hits)} hit(s)")
        fresh_cascade = True
        log(f"CASCADE: {len(added)} packages for {len(new_hits)} new hits")

    # ---- 5. coverage commit ----------------------------------------------
    all_pids = set(tier_of_pid)
    all_pids |= {p["pid"] for p in overlay.get("packages") or []}
    done_sorted = sorted(covered_done & all_pids, key=claim_sort_key)
    candidates_done = sum(base_candidates.get(pid, 0) for pid in done_sorted)
    partial_state = {pid: merge_partials(recs)
                     for pid, recs in partial_recs.items()
                     if pid not in covered_done}
    coverage = {
        "updated_utc": now_utc(),
        "done": done_sorted,
        "partial": partial_state,
    }
    commit({COVERAGE_PATH: json.dumps(coverage, indent=1) + "\n"},
           f"coverage: {len(done_sorted)}/{len(all_pids)} packages done")

    # ---- 6. dispatch decision (runs BEFORE the governor so the budget hint
    #         reflects the fleet size we are about to launch) ---------------
    def live_grid_runs() -> list[dict]:
        """in_progress runs + recently queued runs. A queued run that has
        been waiting > 30 min is a zombie (planted while repo Actions was
        disabled; GitHub attaches no jobs and refuses cancellation) and must
        not block dispatching."""
        out = []
        for r in gh.list_runs("grid.yml"):
            if r.get("status") == "in_progress":
                out.append(r)
                continue
            try:
                ca = datetime.strptime(r["created_at"], "%Y-%m-%dT%H:%M:%SZ") \
                    .replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                continue
            if r.get("status") == "queued" and now - ca < 30 * 60:
                out.append(r)
        return out

    active_runs = live_grid_runs()
    recent_run = False
    for r in gh.all_recent_runs("grid.yml", per_page=5):
        try:
            ca = datetime.strptime(r["created_at"], "%Y-%m-%dT%H:%M:%SZ") \
                .replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
        if now - ca < 120:
            recent_run = True
    pending = sorted(all_pids - covered_done, key=claim_sort_key)
    should_dispatch = (pending and not active_runs and not recent_run) or \
                      (pending and fresh_cascade and len(active_runs) < 2 and
                       not recent_run)
    dispatched = 0
    if should_dispatch:
        count = min(args.max_active_workers, 20, len(pending))
        ok = gh.dispatch("grid.yml", "main", {
            "worker_count": str(count),
            "minutes": str(args.worker_minutes),
            "mode": "full", "max_probes": "0",
        })
        dispatched = count if ok else 0
        log(f"dispatched grid.yml workers={count} minutes={args.worker_minutes} "
            f"pending={len(pending)}")
    else:
        log(f"no dispatch: active_runs={len(active_runs)} recent={recent_run} "
            f"pending={len(pending)} fresh_cascade={fresh_cascade}")

    # ---- 7. budget governor ------------------------------------------------
    # active_hint must reflect the fleet the WORKERS should assume: observed
    # fresh workers, or the fleet we just dispatched (workers read the hint at
    # their first claim, before any of them has heartbeated). Without this,
    # a lone stale hint of 1 lets all 20 workers each claim the full budget.
    budget = read_json("global_rate.json") or {}
    cur_budget = float(budget.get("budget_rps", 60000.0))
    active_n = len(fresh_workers)
    hint = max(active_n, dispatched, 1)
    if fleet_bad[1] >= 2000:
        frac = fleet_bad[0] / fleet_bad[1]
        if frac < 0.003:
            new_budget = min(cur_budget * 1.15, BUDGET_MAX)
        elif frac > 0.015:
            new_budget = max(cur_budget * 0.7, BUDGET_MIN)
        else:
            new_budget = cur_budget
        if abs(new_budget - cur_budget) > 1 or hint != int(
                budget.get("active_hint", 0)):
            commit({"global_rate.json": json.dumps({
                "budget_rps": round(new_budget, 1),
                "active_hint": hint,
                "updated_utc": now_utc(),
                "note": "grid v3 governor (5xx/err fraction driven)",
            }, indent=1) + "\n"}, f"budget {cur_budget:.0f} -> {new_budget:.0f} rps "
               f"(bad_frac={frac:.4f}) hint={hint}")
            cur_budget = new_budget
        log(f"governor: bad_frac={frac:.4f} budget={cur_budget:.0f} "
            f"active={active_n} hint={hint}")
    else:
        # keep active_hint fresh even without telemetry
        if hint != int(budget.get("active_hint", 0)):
            commit({"global_rate.json": json.dumps({
                "budget_rps": round(cur_budget, 1),
                "active_hint": hint,
                "updated_utc": now_utc(),
                "note": "grid v3 governor (no telemetry yet)",
            }, indent=1) + "\n"}, f"budget hint -> {hint}")

    # ---- 8. monitoring -------------------------------------------------------
    per_tier_done: dict[str, int] = collections.Counter()
    for pid in done_sorted:
        per_tier_done[tier_of_pid.get(pid, "cascade")] += \
            base_candidates.get(pid, 0)
    per_tier_total: dict[str, int] = collections.Counter()
    for pid, n in base_candidates.items():
        per_tier_total[tier_of_pid[pid]] += n
    fleet_rps = sum(float(sh.get("rps_ema", 0) or 0) for sh in fresh_workers)
    pending_candidates = sum(
        base_candidates.get(pid, 0) for pid in pending) + \
        sum(p.get("candidates", 0) for p in overlay.get("packages") or []
            if p["pid"] not in covered_done)
    status = {
        "updated_utc": now_utc(),
        "campaign": "halloween-2021",
        "budget_rps": cur_budget,
        "active_workers": active_n,
        "fleet_rps_ema": round(fleet_rps, 1),
        "probed_total_shards": probed_total,
        "packages": {"done": len(done_sorted), "pending": len(pending),
                     "total": len(all_pids)},
        "candidates": {"done": candidates_done,
                       "pending": pending_candidates,
                       "total": spec["candidates_emitted"]},
        "eta_hours_at_budget": round(pending_candidates /
                                     max(cur_budget, 1) / 3600.0, 2),
        "tier_progress": {t: {"done": per_tier_done.get(t, 0),
                              "total": per_tier_total.get(t, 0)}
                          for t in sorted(per_tier_total)},
        "verified_hits": [{"url": h.get("url"), "map_id": h.get("map_id"),
                           "ts_ms": h.get("ts_ms"),
                           "sha256": h.get("sha256"),
                           "bytes": h.get("bytes"),
                           "zip_entries": h.get("zip_entries")}
                          for h in verified_hits],
        "cascade": {"revision": overlay.get("revision", 0),
                    "packages": len(overlay.get("packages") or [])},
        "dispatched_now": dispatched,
    }
    commit({STATUS_PATH: json.dumps(status, indent=1) + "\n"},
           f"grid status: {len(done_sorted)}/{len(all_pids)} pkgs, "
           f"hits={len(verified_hits)}, fleet={active_n} workers "
           f"@{fleet_rps:.0f} rps")
    if prune:
        commit({p: None for p in prune},
               f"prune {len(prune)} stale shards (>48h)")

    print(json.dumps(status, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
