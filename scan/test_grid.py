#!/usr/bin/env python3
"""Offline tests for grid v3 (no network, no aiohttp needed for the pure parts).

Run:  python scan/test_grid.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import grid_plan as GP  # noqa: E402
from grid_worker import AIMD3, OrdinalStream  # noqa: E402
from grid_controller import (build_cascade, claim_sort_key,  # noqa: E402
                             cut_cascade, merge_partials, merge_ranges)

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f" FAIL  {name} {detail}")


# --------------------------------------------------------------------------
def mini_spec() -> dict:
    """2 tiers x 2 series x 3 hours, 1e6 ms per hour per series."""
    tiers = []
    base = 1632789600000  # 2021-09-28T00:00:00Z (hour-aligned)
    for prio, tid, path in (
            (1, "h1a_family_map_packs", "https://h/sandbox/games/maps/{map_id}.{ts_ms}.zip"),
            (2, "h1b_family_plugins", "https://h/sandbox/games/plugins/{map_id}.{ts_ms}.zip")):
        ranges = []
        for mid in ("g1046", "g1008"):
            for h in range(3):
                ranges.append({"map_id": mid, "base": base + h * 3_600_000,
                               "start_offset_ms": 0, "end_offset_ms": 1_000_000})
        tiers.append({"tier_id": tid, "priority": prio, "path_fmt": path,
                      "ranges": ranges})
    total = sum(max(0, r["end_offset_ms"] - r["start_offset_ms"])
                for t in tiers for r in t["ranges"])
    return {"url_path_fmt": "https://h/{map_id}.{ts_ms}.zip",
            "tiers": tiers, "candidates_emitted": total,
            "candidates_per_tier": {
                t["tier_id"]: sum(max(0, r["end_offset_ms"] - r["start_offset_ms"])
                                  for r in t["ranges"]) for t in tiers}}


def test_plan_generation() -> None:
    print("plan generation (synthetic spec):")
    spec = mini_spec()
    with tempfile.TemporaryDirectory() as td:
        sp = Path(td) / "spec.json"
        sp.write_text(json.dumps(spec))
        raw1 = sp.read_bytes()
        out1, out2 = Path(td) / "p1.json", Path(td) / "p2.json"
        GP.gen(sp, None, out1, package_target=1_500_000)
        GP.gen(sp, None, out2, package_target=1_500_000)
        check("deterministic bytes", out1.read_bytes() == out2.read_bytes())
        check("deterministic vs fresh spec read",
              json.loads(out1.read_bytes())["spec_sha256"] is not None)
        del raw1
        ok = GP.validate(sp, out1)
        check("validate passes", ok)
        plan = json.loads(out1.read_bytes())
        total = sum(p["candidates"] for p in plan["packages"])
        check("total conserved", total == spec["candidates_emitted"],
              f"{total} != {spec['candidates_emitted']}")

        # hour-major ordering inside tier 1: all series of hour h before h+1
        h1a = [p for p in plan["packages"] if p["tier"] == "h1a_family_map_packs"]
        seq = []
        for p in h1a:
            for it in p["items"]:
                seq.append((it["t0"], it["map_id"]))
        hours = [t // 3_600_000 for t, _ in seq]
        # hours must be non-decreasing
        check("hour-major order", all(a <= b for a, b in zip(hours, hours[1:])),
              f"hours={hours[:20]}")
        # same-hour items must carry distinct series until hour changes
        grouped_ok = True
        i = 0
        while i < len(seq):
            j = i
            while j < len(seq) and seq[j][0] // 3_600_000 == seq[i][0] // 3_600_000:
                j += 1
            mids = {m for _, m in seq[i:j]}
            if len(mids) != 2:  # both series present in each hour group
                grouped_ok = False
            i = j
        check("both series swept per hour", grouped_ok)


def test_real_plan() -> None:
    print("committed plan vs frozen spec:")
    sp = Path(__file__).resolve().parent / "spec_halloween_core.json"
    pp = Path(__file__).resolve().parent / "grid_plan.json"
    if not pp.exists():
        check("plan file exists", False, "grid_plan.json not committed yet")
        return
    check("validate real plan", GP.validate(sp, pp))
    plan = json.loads(pp.read_bytes())
    check("plan sha recorded", len(plan.get("spec_sha256", "")) == 64)


def test_seconds_tracker() -> None:
    print("CompletionTracker:")
    from grid_worker import CompletionTracker, DIRTY_CAP

    # clean prefix + recorded dirt does not block the cursor
    t = CompletionTracker(5000, 0)
    for o in range(0, 3000):
        t.complete(o, True)
    check("clean prefix", t.probed_through_local() == 3000)
    t.complete(3000, False, "503")          # recorded dirt -> accounted
    t.complete(3001, True)
    check("cursor passes recorded dirt", t.probed_through_local() == 3002)

    # unrecorded dirt (cap reached) stalls the cursor exactly there
    tail = 10
    t3 = CompletionTracker(1000 + DIRTY_CAP + tail, 0)
    for o in range(0, 1000):
        t3.complete(o, True)
    # DIRTY_CAP+1 dirty ordinals: the last one cannot be recorded
    for o in range(1000, 1000 + DIRTY_CAP + 1):
        t3.complete(o, False, "503")
    check("cap stalls cursor", t3.probed_through_local() == 1000 + DIRTY_CAP,
          f"ptr={t3.probed_through_local()}")

    # dirty_ranges compression
    d = {10: "503", 11: "503", 12: "503", 99: "TIMEOUT", 7: "503"}
    rs = CompletionTracker.dirty_ranges(d)
    check("dirty ranges", rs == [[7, 7], [10, 12], [99, 99]], f"{rs}")
    check("expand roundtrip",
          sorted(CompletionTracker.expand_ranges(rs)) == sorted(d))

    # retry debt below the cursor survives via dirty dict (worker path)
    t4 = CompletionTracker(100, 0)
    for o in range(100):
        t4.complete(o, True)
    t4.dirty[50] = "503"
    check("below-cursor debt kept",
          [50, 50] in CompletionTracker.dirty_ranges(t4.dirty))


def test_ordinal_stream() -> None:
    print("OrdinalStream:")
    pkg = {"pid": "b000000", "ordinal0": 100,
           "candidates": 6,
           "items": [
               {"path_fmt": "https://h/maps/{map_id}.{ts_ms}.zip",
                "map_id": "g1046", "t0": 1000, "t1": 1003, "ordinal0": 100},
               {"path_fmt": "https://h/maps/{map_id}.{ts_ms}.zip",
                "map_id": "g1008", "t0": 5000, "t1": 5003, "ordinal0": 103}]}
    s = OrdinalStream(pkg)
    got = [s.at(o) for o in range(100, 106)]
    want = [("https://h/maps/g1046.1000.zip", "g1046", 1000),
            ("https://h/maps/g1046.1001.zip", "g1046", 1001),
            ("https://h/maps/g1046.1002.zip", "g1046", 1002),
            ("https://h/maps/g1008.5000.zip", "g1008", 5000),
            ("https://h/maps/g1008.5001.zip", "g1008", 5001),
            ("https://h/maps/g1008.5002.zip", "g1008", 5002)]
    check("stream mapping", got == want, f"got={got}")
    check("blocks", s.blocks() == [3, 3])


def test_aimd() -> None:
    print("AIMD3:")
    a = AIMD3()
    check("start", a.rps == 600.0)
    for _ in range(20):
        a.window_tick(0.0, 0.0, 8000.0)
    check("clean ramp", a.rps == 600.0 + 200.0 * 20)
    for _ in range(40):
        a.window_tick(0.0, 0.0, 8000.0)
    check("hard cap", a.rps == 8000.0)
    # throttle-burst tolerance: 1-2% 503 -> hold, no collapse
    a2 = AIMD3()
    for _ in range(5):
        a2.window_tick(0.0, 0.0, 8000.0)   # ramp to 1600
    before = a2.rps
    a2.window_tick(0.015, 0.0, 8000.0)
    check("soft burst holds", a2.rps == before)
    a2.window_tick(0.035, 0.0, 8000.0)     # mild -> x0.8
    check("mild decay x0.8", abs(a2.rps - before * 0.8) < 1e-9)
    a2.window_tick(0.10, 0.0, 8000.0)      # severe -> x0.6 but ratchet floor
    check("severe decay with ratchet floor",
          a2.rps >= max(60.0, a2.best_clean * 0.4) - 1e-9)
    a3 = AIMD3()
    for _ in range(10):
        a3.window_tick(0.0, 0.0, 8000.0)   # ramp to 2600; proven rate = 2400
    for _ in range(10):
        a3.window_tick(0.20, 0.0, 8000.0)  # sustained severe window
    # best_clean snapshots the PROVEN (pre-increment) clean rate: 2400
    check("ratchet floor holds under sustained severe",
          abs(a3.rps - max(60.0, 2400.0 * 0.4)) < 1e-9,
          f"rps={a3.rps}")
    # inline streak
    b = AIMD3()
    for _ in range(20):
        b.record(True)
    check("streak decay+pause", b.rps == 600.0 * 0.6 and
          b.pause_until > 0)


def test_cascade() -> None:
    print("cascade builder:")
    spec = mini_spec()
    plan = {"packages": [
        {"pid": f"b{i:05d}", "tier": "h1a_family_map_packs", "items": [
            {"path_fmt": "https://h/sandbox/games/maps/{map_id}.{ts_ms}.zip",
             "map_id": mid, "t0": 0, "t1": 1, "ordinal0": 0}]
         } for i, mid in enumerate(("g1046", "g1008", "m1008_2"))] + [
        {"pid": "b000100", "tier": "h1b_family_plugins", "items": [
            {"path_fmt": "https://h/sandbox/games/plugins/{map_id}.{ts_ms}.zip",
             "map_id": mid, "t0": 0, "t1": 1, "ordinal0": 0}]
         } for mid in ("g1046", "BedWar")]}
    hit = {"map_id": "g1046", "ts_ms": 1_632_790_000_000,
           "url": "https://h/sandbox/games/maps/g1046.1632790000000.zip"}
    pkgs = build_cascade(spec, plan, hit, 0)
    c1 = [p for p in pkgs if p["pid"].startswith("c1")]
    c2 = [p for p in pkgs if p["pid"].startswith("c2")]
    check("c1 exists", len(c1) >= 1)
    items1 = [it for p in c1 for it in p["items"]]
    mids1 = {it["map_id"] for it in items1}
    check("c1 excludes hit itself", "g1046" not in mids1)
    check("c1 has map siblings", "g1008" in mids1)
    check("c1 has no +/-90s bundle dup",
          all(it["t1"] - it["t0"] != 180_000
              for it in items1 if it["map_id"].startswith("m")))
    # bundle tight window (exactly one entry, +/-2.5s)
    bundles = [it for it in items1 if it["map_id"] == "m1008_2"]
    check("bundle single entry", len(bundles) == 1)
    check("bundle window +/-2.5s",
          bundles[0]["t1"] - bundles[0]["t0"] == 5000)
    spec_mids = {r["map_id"] for t in spec["tiers"] for r in t["ranges"]}
    c2_mids = {it["map_id"] for p in c2 for it in p["items"]}
    check("c2 covers all spec series except hit",
          c2_mids == spec_mids - {"g1046"}, f"c2_mids={c2_mids}")
    check("c2 window +/-600s", all(it["t1"] - it["t0"] == 1_200_000
                                   for p in c2 for it in p["items"]))
    check("seq numbering", [p["pid"] for p in pkgs] ==
          [p["pid"] for p in sorted(pkgs, key=lambda p: (p["pid"].split("-")[0],
                                                         int(p["pid"].split("-")[1])))])


def test_claim_order() -> None:
    print("claim ordering:")
    pids = ["b000010", "c2-00003", "c1-00001", "b000002", "c1-00000"]
    got = sorted(pids, key=claim_sort_key)
    check("c1 < c2 < base", got == ["c1-00000", "c1-00001", "c2-00003",
                                    "b000002", "b000010"], f"got={got}")


def test_merge_partials() -> None:
    print("partial merge (dirty debt carry-over):")
    a = {"probed_through": 5_000_000, "dirty_ranges": [[2_000_000, 2_000_010]]}
    b = {"probed_through": 8_000_000, "dirty_ranges": [[6_000_000, 6_000_005]]}
    m = merge_partials([a, b])
    check("max through wins", m["through"] == 8_000_000)
    # the max claimant's own recorded debt (below its cursor) is kept...
    check("max claimant's debt kept", [6_000_000, 6_000_005] in m["dirty"])
    # ...while a superseded claimant's dirt below the prefix is dropped
    # (the max claimant completed those ordinals itself)
    check("superseded dirt dropped", [2_000_000, 2_000_010] not in m["dirty"])
    c = {"probed_through": 3_000_000, "dirty_ranges":
         [[7_900_000, 8_100_000]]}  # crosses the 8M prefix
    m2 = merge_partials([b, c])
    check("crossing dirt clipped to prefix",
          [8_000_000, 8_100_000] in m2["dirty"], f"{m2['dirty']}")
    check("merge_ranges coalesces",
          merge_ranges([[1, 3], [2, 5], [7, 7]]) == [[1, 5], [7, 7]])


def test_cut_cascade_cap() -> None:
    print("cascade packaging cap:")
    items = [{"map_id": f"g{i}", "path_fmt": "https://h/{map_id}.{ts_ms}.zip",
              "t0": 0, "t1": 7_000_000} for i in range(6)]  # 42M total
    pkgs = cut_cascade(items, "c2", 5)
    check("cap respected", all(p["candidates"] <= 18_000_000 for p in pkgs))
    check("total conserved", sum(p["candidates"] for p in pkgs) == 42_000_000)
    check("seq start honored", pkgs[0]["pid"] == "c2-00005")


if __name__ == "__main__":
    test_plan_generation()
    test_real_plan()
    test_seconds_tracker()
    test_ordinal_stream()
    test_aimd()
    test_cascade()
    test_claim_order()
    test_merge_partials()
    test_cut_cascade_cap()
    print(f"\n{PASS} passed, {FAIL} failed")
    raise SystemExit(1 if FAIL else 0)
