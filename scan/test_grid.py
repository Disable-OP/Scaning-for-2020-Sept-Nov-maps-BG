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
from grid_worker import AIMD3, OrdinalStream, SecondsTracker  # noqa: E402
from grid_controller import (build_cascade, claim_sort_key,  # noqa: E402
                             cut_cascade)

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
    print("SecondsTracker:")
    # 5 blocks of 1000
    t = SecondsTracker([1000] * 5, 0)
    for o in range(2500):
        t.complete(o, True)
    check("cursor mid-block", t.probed_through_local() == 2000)
    for o in range(2500, 5000):
        t.complete(o, True)
    check("cursor complete", t.probed_through_local() == 5000)

    # 5xx in second 1 blocks the cursor
    t = SecondsTracker([1000] * 4, 0)
    for o in range(0, 4000):
        t.complete(o, o != 1500)  # one 503 in block 1
    check("5xx blocks cursor", t.probed_through_local() == 1000,
          f"cursor={t.probed_through_local()}")

    # resume skips fully-clean prefix only
    t2 = SecondsTracker([1000] * 4, 1000)
    check("resume cursor", t2.probed_through_local() == 1000)
    for o in range(1000, 4000):
        t2.complete(o, True)
    check("resume completes", t2.probed_through_local() == 4000)

    # ragged tail block
    t = SecondsTracker([1000, 1000, 372], 0)
    for o in range(2372):
        t.complete(o, True)
    check("ragged tail", t.probed_through_local() == 2372)
    check("block_of ragged", t.block_of(2300) == 2)


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
    a.window_tick(0.05, 0.0, 8000.0)
    check("bad decay", abs(a.rps - 8000.0 * 0.6) < 1e-9)
    a.window_tick(0.0, 0.0, 3000.0)
    check("cap clamp", a.rps <= 3000.0)
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
    test_cut_cascade_cap()
    print(f"\n{PASS} passed, {FAIL} failed")
    raise SystemExit(1 if FAIL else 0)
