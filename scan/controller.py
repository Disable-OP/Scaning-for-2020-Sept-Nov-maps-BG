#!/usr/bin/env python3
"""Persistent-queue controller for the distributed MAPS scan.

State model (branch `scan-state`, all updates atomic git commits):
  queue.json            chunk ledger: PENDING -> RUNNING -> COMPLETED
                                     |            v
                                     +--> RETRY -> FAILED (job attempts)
  claims/{key}.json     written by workers at startup (run mapping)
  results/{key}.json    written by workers at end (source of truth)
  retry_batches/*.json  explicit candidate lists built from undetermined obs
  global_rate.json      global adaptive rate budget (this controller)
  monitoring/status.json+report   live metrics snapshot
  agg.json              incremental ingestion ledger

Two independent counters:
  job_attempts   per queue entry (crash/stale re-runs; cap 4)
  retry_level    for retry batches (candidate probe round: 2 or 3; the
                 initial spec probe is round 1). Bounded, disclosed.

Candidate statuses (never silently converted):
  CONFIRMED_404 | FOUND_200 | FOUND_INVALID | PROBED_UNDETERMINED_503 |
  TIMEOUT | NETERR | OTHER_ERROR | UNSEARCHED
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen_candidates as G  # noqa: E402
from gh import GH  # noqa: E402

STATE = "scan-state"
MAX_ACTIVE_WORKERS = int(os.environ.get("MAX_ACTIVE_WORKERS", "100"))
MAX_DISPATCH_PER_CYCLE = int(os.environ.get("MAX_DISPATCH_PER_CYCLE", "40"))
CHUNK_DEADLINE_MIN = 100
CLAIM_GRACE_MIN = 30           # dispatched-but-no-claim grace window
JOB_ATTEMPT_CAP = 4            # re-runs of one queue entry (crashes)
CANDIDATE_MAX_ROUNDS = 3       # 1 spec probe + up to 2 retry rounds
RETRY_DELAYS_MIN = {2: 15, 3: 60}
BUDGET_START = float(os.environ.get("BUDGET_START_RPS", "1500"))
BUDGET_MIN = float(os.environ.get("BUDGET_MIN_RPS", "150"))
BUDGET_MAX = float(os.environ.get("BUDGET_MAX_RPS", "8000"))
UNDETERMINED = {"503", "TIMEOUT", "NETERR", "OTHER"}
BATCH_SIZE = 20000


def now() -> float:
    return time.time()


def iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def log(msg: str) -> None:
    print(f"[controller {iso(now())}] {msg}", flush=True)


def load_json(gh: GH, path: str, default):
    try:
        d = gh.get_file_json(path, STATE)
        return d if d is not None else default
    except Exception:
        return default


def queue_blob_sha(gh: GH) -> str | None:
    """Current blob sha of queue.json on the state branch (via tree)."""
    st, ref, _h = gh.api("GET", f"/repos/{gh.owner}/{gh.repo}/git/ref/heads/{STATE}")
    if st != 200:
        return None
    st, tree, _h = gh.api("GET",
                          f"/repos/{gh.owner}/{gh.repo}/git/trees/{ref['object']['sha']}?recursive=0")
    if st != 200:
        return None
    for e in tree.get("tree", []):
        if e.get("path") == "queue.json":
            return e.get("sha")
    return None


def load_queue_blob(gh: GH, sha: str):
    st, blob, _h = gh.api("GET", f"/repos/{gh.owner}/{gh.repo}/git/blobs/{sha}")
    if st != 200:
        return None
    import base64
    return json.loads(base64.b64decode(blob["content"]))


def parse_utc(s: str) -> float:
    try:
        return time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone
    except Exception:
        return 0.0


def undetermined_of(res: dict) -> list[tuple[int, str]]:
    """[(pos, status)] of probed candidates that are undetermined."""
    default = res.get("default_status")
    if not default:
        return []
    out = []
    for pos in range(res.get("candidates_probed_unique", 0)):
        out.append((pos, default))  # placeholder, corrected below
    ex = dict(res.get("exceptions", []))
    undet = []
    for pos in range(res.get("candidates_probed_unique", 0)):
        st = ex.get(pos, default)
        if st in UNDETERMINED or st.startswith("OTHER_"):
            undet.append((pos, st))
    del out
    return undet


def main() -> int:
    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/")
    gh = GH(os.environ.get("GITHUB_TOKEN", ""), owner, repo)
    spec = G.load_spec("scan/spec_v1.json")
    num_chunks = G.num_chunks(spec)
    t_cycle0 = now()

    gh.ensure_branch(STATE, "main")

    # ------------------------------------------------ queue init / load ----
    q_sha0 = queue_blob_sha(gh)
    queue = load_queue_blob(gh, q_sha0) if q_sha0 else None
    if queue is None or not queue.get("chunks"):
        log("initializing queue")
        chunks = {}
        for cid in range(num_chunks):
            key = f"chunk-{cid:06d}"
            chunks[key] = {"status": "PENDING", "job_attempts": 0, "attempt_seq": 0,
                           "kind": "spec", "spec_sha256": spec["_sha256"]}
        queue = {"schema": 2, "spec_sha256": spec["_sha256"],
                 "num_chunks": num_chunks, "chunk_size": spec["chunk_size"],
                 "total_candidates": G.total_candidates(spec),
                 "chunks": chunks, "batches_seq": 0, "peak_active_workers": 0}
    chunks: dict = queue["chunks"]

    agg = load_json(gh, "agg.json", {"ingested": {}, "found_ledger": {},
                                     "verified": {}})

    # ------------------------------------------------ list state files -----
    st, ref, _h = gh.api("GET", f"/repos/{owner}/{repo}/git/ref/heads/{STATE}")
    head_sha = ref["object"]["sha"]
    st, tree, _h = gh.api("GET", f"/repos/{owner}/{repo}/git/trees/{head_sha}?recursive=1")
    paths = [e["path"] for e in tree.get("tree", [])
             if e["type"] == "blob" and e["path"].startswith(("results/", "claims/"))]
    result_keys = {p.split("/", 1)[1][:-len(".result.json")]
                   for p in paths if p.startswith("results/") and p.endswith(".result.json")}
    claim_keys = {p.split("/", 1)[1].rsplit(".", 1)[0]
                  for p in paths if p.startswith("claims/") and p.endswith(".json")}
    log(f"state tree: {len(result_keys)} results, {len(claim_keys)} claims")

    # --------------------------------- ingest new results (incremental) ----
    new_keys = sorted(result_keys - set(agg["ingested"]))
    ingested_now = []
    for key in new_keys:
        try:
            res = gh.get_file_json(f"results/{key}.result.json", STATE)
        except Exception as e:
            log(f"ingest read failed {key}: {e}")
            continue
        if not res:
            continue
        agg["ingested"][key] = {
            "outcome": res.get("outcome"), "statuses": res.get("statuses", {}),
            "probed": res.get("candidates_probed_unique", 0),
            "scheduled": res.get("candidates_scheduled", 0),
            "head_attempts": res.get("head_attempts_total", 0),
            "rps_mean": res.get("rate", {}).get("rps_mean"),
            "p50": res.get("latency_ms", {}).get("p50"),
            "p95": res.get("latency_ms", {}).get("p95"),
            "finished_utc": res.get("finished_utc"),
            "mode": res.get("mode"), "found": len(res.get("found", [])),
            "attempt": res.get("attempt"), "source": res.get("candidate_source"),
        }
        ingested_now.append((key, res))
        for f in res.get("found", []):
            if f.get("zip_ok") and f.get("zip_crc_ok"):
                agg["found_ledger"][f["url"]] = {
                    "map_id": f["map_id"], "ts_ms": f["ts_ms"],
                    "sha256": f.get("sha256"), "bytes": f.get("bytes"),
                    "observed_utc": f.get("observed_utc"),
                    "status": "FOUND_CANDIDATE", "source_result": key,
                }
    log(f"ingested {len(ingested_now)} new results: {new_keys[:6]}")

    # ------------------------------------------------- result validation ---
    for key, res in ingested_now:
        ent = chunks.get(key)
        if ent is None:
            continue
        outc = res.get("outcome")
        if outc in ("COMPLETED", "COMPLETED_WITH_UNDETERMINED"):
            ent["status"] = "COMPLETED"
            ent["completed_at"] = res.get("finished_utc")
            ent["statuses"] = res.get("statuses", {})
            ent["undetermined"] = len(undetermined_of(res))
        elif outc == "TEST":
            ent["status"] = "IGNORED_TEST"
        else:  # PARTIAL or unexpected: consume one job attempt, retry later
            ent["job_attempts"] = ent.get("job_attempts", 0) + 1
            if ent["job_attempts"] >= JOB_ATTEMPT_CAP:
                ent["status"] = "FAILED"
            else:
                ent["status"] = "RETRY"
                ent["partial_result"] = f"results/{key}.result.json"
                ent["not_before"] = now() + RETRY_DELAYS_MIN.get(
                    ent["job_attempts"] + 1, 60) * 60

    # ------------------------------------- stale RUNNING detection ---------
    runs = gh.list_runs("worker.yml", statuses=("queued", "in_progress"))
    active_run_ids = {r["id"] for r in runs}
    active_now = len(active_run_ids)
    queue["peak_active_workers"] = max(queue.get("peak_active_workers", 0), active_now)
    for key, ent in chunks.items():
        if ent.get("status") != "RUNNING":
            continue
        claim = load_json(gh, f"claims/{key}.json", {}) if key in claim_keys else {}
        dispatched_at = parse_utc(ent.get("dispatched_at", ""))
        if key in claim_keys:
            rid = claim.get("run_id")
            age_min = (now() - parse_utc(claim.get("started_utc", ""))) / 60.0
            dead = (rid not in active_run_ids) or age_min > CHUNK_DEADLINE_MIN
        else:
            age_min = (now() - dispatched_at) / 60.0 if dispatched_at else 1e9
            dead = age_min > CLAIM_GRACE_MIN
        if dead:
            ent["job_attempts"] = ent.get("job_attempts", 0) + 1
            if ent["job_attempts"] >= JOB_ATTEMPT_CAP:
                ent["status"] = "FAILED"
            else:
                ent["status"] = "RETRY"
                ent["not_before"] = now() + RETRY_DELAYS_MIN.get(
                    ent["job_attempts"] + 1, 60) * 60
            ent["stale_note"] = (f"claim_age_min={age_min:.0f} "
                                 f"run_active={claim.get('run_id') in active_run_ids}")
            log(f"stale RUNNING -> {ent['status']}: {key} ({ent['stale_note']})")

    # ------------------------- retry batches from undetermined candidates --
    # Built from NEWLY ingested full results (spec chunks and batches alike).
    # Only candidates whose latest observation is undetermined are retried,
    # bounded by CANDIDATE_MAX_ROUNDS. Confirmed 404 / found are never retried.
    seq = queue.get("batches_seq", 0)
    new_batch_files = {}
    for key, res in ingested_now:
        if res.get("mode") == "test" or res.get("outcome") != "COMPLETED_WITH_UNDETERMINED":
            continue
        if key.startswith("batch-"):
            parent_level = chunks.get(key, {}).get("retry_level", 2)
            level = parent_level + 1
            parent_kind = "retry_batch"
            batch_file = chunks.get(key, {}).get("batch_file")
            pos_map = None
            if batch_file:
                try:
                    bdata = gh.get_file_json(batch_file, STATE)
                    pos_map = {i: (c["map_id"], c["ts_ms"], c)
                               for i, c in enumerate(bdata["candidates"])}
                except Exception as e:
                    log(f"parent batch load failed {key}: {e}")
                    continue
        else:
            level = 2
            parent_kind = "spec"
            cid = int(key.split("-")[1])
            pos_map = {o: (m, ts, None)
                       for o, m, ts, _t in G.iter_chunk(spec, cid)}
        if level > CANDIDATE_MAX_ROUNDS:
            continue
        cands = []
        for pos, prior in undetermined_of(res):
            if pos_map and pos in pos_map:
                m, ts, orig = pos_map[pos]
                cands.append({"map_id": m, "ts_ms": ts,
                              "source_key": key, "source_ordinal": pos,
                              "prior_status": prior})
        if not cands:
            continue
        for i in range(0, len(cands), BATCH_SIZE):
            part = cands[i:i + BATCH_SIZE]
            bname = f"batch-{seq:04d}"
            seq += 1
            new_batch_files[f"retry_batches/{bname}.json"] = json.dumps({
                "batch_id": bname, "created_utc": iso(now()),
                "count": len(part), "retry_level": level,
                "parent_result": key, "candidates": part}, indent=1) + "\n"
            chunks[bname] = {"status": "PENDING", "job_attempts": 0, "attempt_seq": 0,
                             "kind": "retry_batch", "retry_level": level,
                             "batch_file": f"retry_batches/{bname}.json",
                             "size": len(part), "parent_result": key}
        # mark parent as fully accounted (its undetermined moved to a batch)
        if key in chunks and parent_kind == "retry_batch":
            chunks[key]["carried_to"] = f"batch seq {seq - 1}"
    queue["batches_seq"] = seq
    if new_batch_files:
        gh.commit_files(STATE, new_batch_files,
                        f"retry batches x{len(new_batch_files)} "
                        f"(undetermined-only, round<= {CANDIDATE_MAX_ROUNDS})")
        log(f"created {len(new_batch_files)} retry batches")

    # -------------------------------------------------- global rate law ----
    recent = [a for a in agg["ingested"].values()
              if a.get("finished_utc") and a.get("mode") != "test"
              and (now() - parse_utc(a["finished_utc"])) < 1500]
    nbad = sum(a["statuses"].get("503", 0) + a["statuses"].get("TIMEOUT", 0)
               + a["statuses"].get("NETERR", 0)
               + sum(v for k, v in a["statuses"].items() if k.startswith("OTHER"))
               for a in recent)
    ntot = sum(a.get("probed", 0) for a in recent)
    bad_frac = (nbad / ntot) if ntot else 0.0
    gr = load_json(gh, "global_rate.json", {"budget_rps": BUDGET_START})
    budget = float(gr.get("budget_rps", BUDGET_START))
    reason = "insufficient data" if not recent else "stable"
    if recent:
        if bad_frac > 0.08:
            budget = max(BUDGET_MIN, budget * 0.6); reason = f"bad={bad_frac:.3f}>8% cut x0.6"
        elif bad_frac > 0.03:
            budget = max(BUDGET_MIN, budget * 0.8); reason = f"bad={bad_frac:.3f}>3% cut x0.8"
        elif bad_frac < 0.01:
            budget = min(BUDGET_MAX, budget * 1.15); reason = f"bad={bad_frac:.3f}<1% raise x1.15"
    hist = gr.get("history", [])
    hist.append({"utc": iso(now()), "budget_rps": round(budget, 1),
                 "bad_frac": round(bad_frac, 4), "recent_probes": ntot,
                 "reason": reason, "active": active_now})
    gr_out = {"budget_rps": round(budget, 1), "active_hint": max(active_now, 1),
              "updated_utc": iso(now()), "reason": reason, "history": hist[-12:]}
    log(f"global rate: {gr_out['budget_rps']} rps ({reason})")

    # ------------------------------------------------------- dispatching ---
    commit = {"global_rate.json": json.dumps(gr_out, indent=1) + "\n"}
    dispatched = []
    if active_now < MAX_ACTIVE_WORKERS:
        def dispatchable(k, e):
            return (e.get("status") in ("PENDING", "RETRY")
                    and e.get("job_attempts", 0) < JOB_ATTEMPT_CAP
                    and now() >= e.get("not_before", 0))
        # priority: spec chunks first, then retry batches
        spec_keys = sorted(k for k, e in chunks.items()
                           if dispatchable(k, e) and e["kind"] == "spec")
        batch_keys = sorted(k for k, e in chunks.items()
                            if dispatchable(k, e) and e["kind"] == "retry_batch")
        for key in spec_keys + batch_keys:
            if active_now + len(dispatched) >= MAX_ACTIVE_WORKERS:
                break
            if len(dispatched) >= MAX_DISPATCH_PER_CYCLE:
                break
            ent = chunks[key]
            token = os.urandom(6).hex()
            ent["attempt_seq"] = ent.get("attempt_seq", 0) + 1
            ent.update({"status": "RUNNING", "dispatch_token": token,
                        "dispatched_at": iso(now()),
                        "attempt": ent["attempt_seq"]})
            if ent["kind"] == "retry_batch":
                inputs = {"chunk_id": "0", "retry_batch": ent["batch_file"],
                          "dispatch_token": token, "attempt": str(ent["attempt"]),
                          "mode": "full", "max_probes": "0"}
            else:
                inputs = {"chunk_id": str(int(key.split("-")[1])),
                          "retry_batch": "", "dispatch_token": token,
                          "attempt": str(ent["attempt"]), "mode": "full",
                          "max_probes": "0"}
            if gh.dispatch("worker.yml", "main", inputs):
                dispatched.append(key)
            else:
                ent["status"] = "PENDING"
                ent["dispatch_token"] = ""
                ent["attempt_seq"] -= 1
                log(f"dispatch API failed for {key}; left PENDING")
        if dispatched:
            log(f"dispatched {len(dispatched)} workers "
                f"(active {active_now} -> {active_now + len(dispatched)})")

    # ---------------------------------------------- found verification -----
    to_verify = [u for u, m in agg["found_ledger"].items()
                 if m.get("status") == "FOUND_CANDIDATE" and u not in agg["verified"]]
    for url in to_verify[:20]:
        out_json = "verify_out/v.json"
        r = subprocess.run([sys.executable, "scan/verify_map.py", "--urls", url,
                            "--save-dir", "verify_out/zips", "--out", out_json],
                           capture_output=True, text=True, timeout=300)
        try:
            v = json.loads(Path(out_json).read_text())["results"][0]
        except Exception:
            v = {"verified": False, "error": f"verifier crashed: {r.stderr[-200:]}"}
        agg["verified"][url] = {
            "verified": bool(v.get("verified")), "sha256": v.get("sha256"),
            "bytes": v.get("bytes"), "get_status": v.get("get_status"),
            "checked_utc": iso(now()), "error": v.get("error"),
        }
        agg["found_ledger"][url]["status"] = ("VERIFIED" if v.get("verified")
                                              else "REJECTED")
        log(f"verify {url}: {'VERIFIED' if v.get('verified') else 'REJECTED'}")

    # ------------------------------------------------------ queue commit ---
    counts = {}
    for e in chunks.values():
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    queue["counts"] = counts
    queue["updated_utc"] = iso(now())

    # ------------------------- concurrent-writer merge guard ---------------
    # If queue.json changed on the branch since we loaded it, rebase our
    # per-entry mutations onto the fresh copy instead of clobbering it.
    q_sha1 = queue_blob_sha(gh)
    if q_sha0 and q_sha1 and q_sha0 != q_sha1:
        log("queue.json changed underneath; rebasing mutations")
        fresh = load_queue_blob(gh, q_sha1) or queue
        import copy
        orig = copy.deepcopy(chunks)
        # find entries we touched this cycle
        touched = {k: e for k, e in chunks.items() if orig.get(k) != e}
        fchunks = fresh.setdefault("chunks", {})
        for k, e in touched.items():
            fchunks[k] = e
        fresh["batches_seq"] = max(fresh.get("batches_seq", 0),
                                   queue.get("batches_seq", 0))
        fresh["peak_active_workers"] = max(
            fresh.get("peak_active_workers", 0),
            queue.get("peak_active_workers", 0))
        fresh["counts"] = {}
        fc = {}
        for e in fchunks.values():
            fc[e["status"]] = fc.get(e["status"], 0) + 1
        fresh["counts"] = fc
        fresh["updated_utc"] = iso(now())
        queue = fresh
        counts = fc
    commit["queue.json"] = json.dumps(queue, indent=1) + "\n"
    commit["agg.json"] = json.dumps(agg, indent=1) + "\n"

    # ------------------------------------------------------- monitoring ----
    uniq = {"confirmed_404": 0, "found_200": 0, "found_invalid": 0,
            "undetermined": 0, "other": 0}
    for a in agg["ingested"].values():
        s = a.get("statuses", {})
        uniq["found_200"] += s.get("200", 0)
        uniq["found_invalid"] += s.get("FOUND_INVALID", 0)
        uniq["confirmed_404"] += s.get("404", 0)
        uniq["undetermined"] += (s.get("503", 0) + s.get("TIMEOUT", 0)
                                 + s.get("NETERR", 0))
        uniq["other"] += sum(v for k, v in s.items() if k.startswith("OTHER"))
    status = {
        "updated_utc": iso(now()),
        "workflows": {"active_workers": active_now,
                      "peak_active_workers": queue.get("peak_active_workers", 0),
                      "max_allowed": MAX_ACTIVE_WORKERS,
                      "dispatched_this_cycle": len(dispatched)},
        "chunks": {k.lower(): v for k, v in counts.items()} | {
            "total": len(chunks)},
        "candidates": {"scheduled_total": queue.get("total_candidates", 0),
                       "unique_confirmed_404": uniq["confirmed_404"],
                       "unique_found_200": uniq["found_200"],
                       "unique_found_invalid": uniq["found_invalid"],
                       "unique_undetermined": uniq["undetermined"],
                       "unique_other": uniq["other"],
                       "remaining_unsearched": max(
                           0, queue.get("total_candidates", 0) - sum(uniq.values()))},
        "retries": {"retry_batches": sum(1 for e in chunks.values()
                                         if e.get("kind") == "retry_batch"),
                    "retry_level_cap": CANDIDATE_MAX_ROUNDS,
                    "failed_chunks": counts.get("FAILED", 0)},
        "rate": {"global_budget_rps": gr_out["budget_rps"],
                 "measured_rps_recent": round(
                     sum(a.get("rps_mean") or 0 for a in recent) / max(1, len(recent)),
                     1) if recent else 0.0,
                 "bad_frac_recent": round(bad_frac, 4),
                 "reason": reason,
                 "recent_window_results": len(recent)},
        "latency_ms_recent": {
            "p50": round(sum(a.get("p50") or 0 for a in recent) / max(1, len(recent)), 1),
            "p95": round(sum(a.get("p95") or 0 for a in recent) / max(1, len(recent)), 1)},
        "found": {"candidates": len(agg["found_ledger"]),
                  "verified": sum(1 for v in agg["verified"].values() if v.get("verified")),
                  "rejected": sum(1 for v in agg["verified"].values()
                                  if not v.get("verified"))},
        "cycle_seconds": round(now() - t_cycle0, 1),
        "github_api_calls": gh.calls,
    }
    commit["monitoring/status.json"] = json.dumps(status, indent=1) + "\n"
    report = render_report(status)
    commit["monitoring/report.md"] = report
    gh.commit_files(STATE, commit, f"controller cycle: {counts} budget={gr_out['budget_rps']}")

    try:
        gh.commit_files("main", {"results/monitoring/report.md": report,
                                 "results/monitoring/status.json":
                                     json.dumps(status, indent=1) + "\n"},
                        f"monitoring snapshot {status['updated_utc']}")
    except Exception as e:
        log(f"main monitoring commit failed: {e}")

    # ------------------------------------------------------- completion ----
    open_work = counts.get("PENDING", 0) + counts.get("RETRY", 0) + counts.get("RUNNING", 0)
    if open_work == 0 and len(agg["ingested"]) > 0:
        log("queue drained; running final aggregation")
        r = subprocess.run([sys.executable, "scan/aggregate.py"],
                           capture_output=True, text=True, timeout=1800)
        print(r.stdout[-4000:], r.stderr[-2000:], flush=True)
        gh.commit_files(STATE, {"scan_complete.json": json.dumps(
            {"completed_utc": iso(now()), "status": status}, indent=1) + "\n"},
            "scan complete: queue drained + evidence aggregated")
    print("STATUS " + json.dumps(status), flush=True)
    return 0


def render_report(s: dict) -> str:
    c, w, r = s["chunks"], s["workflows"], s["rate"]
    lines = [
        "# Distributed MAPS scan — live status",
        f"_Cycle completed {s['updated_utc']} (cycle {s['cycle_seconds']}s, "
        f"{s['github_api_calls']} API calls)_",
        "",
        "## Workers",
        f"- Active workers: **{w['active_workers']}** "
        f"(peak {w['peak_active_workers']}, cap {w['max_allowed']})",
        f"- Dispatched this cycle: {w['dispatched_this_cycle']}",
        "",
        "## Chunks",
        f"- Completed **{c.get('completed', 0)}** / running **{c.get('running', 0)}** / "
        f"pending **{c.get('pending', 0)}** / retry **{c.get('retry', 0)}** / "
        f"failed **{c.get('failed', 0)}** (total {c['total']})",
        "",
        "## Candidates (unique)",
        f"- Scheduled total: {s['candidates']['scheduled_total']}",
        f"- Confirmed 404: {s['candidates']['unique_confirmed_404']}",
        f"- FOUND 200: **{s['candidates']['unique_found_200']}** "
        f"(independently verified maps: **{s['found']['verified']}**)",
        f"- Undetermined 503/timeout (never counted as misses): "
        f"{s['candidates']['unique_undetermined']}",
        f"- Remaining unsearched: {s['candidates']['remaining_unsearched']}",
        "",
        "## Rate & latency (measured)",
        f"- Global budget: {r['global_budget_rps']} rps — {r['reason']}",
        f"- Measured mean worker rps (recent window): {r['measured_rps_recent']} "
        f"over {r['recent_window_results']} results",
        f"- Rolling latency p50/p95: {s['latency_ms_recent']['p50']} / "
        f"{s['latency_ms_recent']['p95']} ms",
        f"- Recent bad-status fraction (503+timeout+other): {r['bad_frac_recent']:.4f}",
        "",
        "## Retry",
        f"- Retry batches: {s['retries']['retry_batches']} "
        f"(candidate probe-round cap {s['retries']['retry_level_cap']}, "
        f"failed chunks {s['retries']['failed_chunks']})",
        "",
        "Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; "
        "a map exists only after direct ZIP + CRC + SHA-256 verification.",
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
