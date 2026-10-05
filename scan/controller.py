#!/usr/bin/env python3
"""Deep-scan controller (v5): unattended, unbounded-chunk persistent queue.

Continues the drained v4 campaign (scan/spec_v1.json, 18,000,252 confirmed
404s, coverage 1.0). The v5 universe is an open-ended ladder of deterministic
epochs (scan/deep_config.json + scan/deep_universe.py). This controller:

  1. materializes queue entries from the frontier (lazy sliding window) -
     there is NO cap on the number of chunks over the campaign lifetime;
  2. commits each epoch spec to the state branch before any of its chunks is
     dispatched (workers re-fetch and verify its sha256);
  3. ingests worker results exactly once (entry-driven; completed entries are
     pruned into cumulative tallies to keep queue.json bounded);
  4. builds retry batches ONLY from undetermined observations (round cap 3);
  5. enforces the global AIMD rate law and dispatches up to MAX_ACTIVE_WORKERS;
  6. verifies every FOUND_200 independently (direct GET + ZIP + CRC + SHA-256);
  7. on frontier exhaustion + queue drain: runs final aggregation, commits
     scan_complete_v5.json and publishes a GitHub Release - unattended.

Honesty invariants (inherited from v4, unchanged):
  - 503/timeout stay UNDETERMINED and are never converted to 404;
  - duplicate probes are counted separately, never as unique coverage;
  - a map exists only after independent direct ZIP+CRC+SHA-256 verification;
  - confirmed v1 candidates are never re-probed (exact interval exclusion);
  - the STOP file on the state branch halts all dispatching immediately.

State model (branch `scan-state`, atomic git commits):
  queue.json            schema 3: frontier + materialized window ledger
                        PENDING -> RUNNING -> COMPLETED -> (pruned, tallied)
                                     |-> RETRY -> (requeue | ABANDONED)
  epochs/epoch_N.json   deterministic deep-universe epoch specs
  claims/{key}.json     worker startup records (pruned after ingest)
  results/{key}.json    worker results (source of truth; PARTIAL is resumable)
  retry_batches/*.json  explicit undetermined candidate lists
  global_rate.json      global adaptive rate budget
  monitoring/*          live status (tallies are exact, not estimates)
  agg.json              rolling 3h ingestion ledger for the rate law
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import deep_universe as D  # noqa: E402
import gen_candidates as G  # noqa: E402
from gh import GH  # noqa: E402

STATE = "scan-state"
MAX_ACTIVE_WORKERS = int(os.environ.get("MAX_ACTIVE_WORKERS", "100"))
MAX_DISPATCH_PER_CYCLE = int(os.environ.get("MAX_DISPATCH_PER_CYCLE", "60"))
QUEUE_WINDOW = int(os.environ.get("DEEP_QUEUE_WINDOW", "600"))
CHUNK_DEADLINE_MIN = 100
CLAIM_GRACE_MIN = 8            # dispatched-but-no-claim grace window
JOB_ATTEMPT_CAP = 4
CHUNK_REQUEUE_CAP = 3       # requeues after FAILED before ABANDONED
CANDIDATE_MAX_ROUNDS = 3
RETRY_DELAYS_MIN = {2: 15, 3: 60}
STALE_RETRY_DELAY_MIN = 2   # no-claim stale requeue: short delay (run never started)
BUDGET_START = float(os.environ.get("BUDGET_START_RPS", "1500"))
BUDGET_MIN = float(os.environ.get("BUDGET_MIN_RPS", "150"))
BUDGET_MAX = float(os.environ.get("BUDGET_MAX_RPS", "16000"))
UNDETERMINED = {"503", "TIMEOUT", "NETERR", "OTHER"}
BATCH_SIZE = 20000
AGG_TTL_S = 3 * 3600
MONITOR_MAIN_EVERY = 6
RELEASE_TAG = "deep-scan-v5-final"

TALLY_KEYS = ("found_200", "found_invalid", "confirmed_404",
              "undetermined", "other")


def now() -> float:
    return time.time()


def iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def log(msg: str) -> None:
    print(f"[controller {iso(now())}] {msg}", flush=True)


def load_json(gh: GH, path: str, branch: str, default):
    try:
        d = gh.get_file_json(path, branch)
        return d if d is not None else default
    except Exception:
        return default


def parse_utc(s: str) -> float:
    try:
        return time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone
    except Exception:
        return 0.0


def parse_chunk_key(key: str):
    """'e12-chunk-000456' -> (12, 456); else None."""
    if not key.startswith("e") or "-chunk-" not in key:
        return None
    try:
        e, c = key[1:].split("-chunk-")
        return int(e), int(c)
    except ValueError:
        return None


def statuses_of(res: dict, chunk_size: int) -> dict[int, str]:
    """pos_LOCAL -> observed status (default + exceptions), epoch-aware."""
    default = res.get("default_status")
    if not default:
        return {}
    src = res.get("candidate_source") or ""
    off = (res["chunk_id"] * chunk_size
           if (src == "spec" or src.startswith("epoch:"))
           and res.get("chunk_id") is not None else 0)
    n = res.get("candidates_probed_unique", 0)
    ex = {int(p): s for p, s in res.get("exceptions", [])}
    return {pos: ex.get(pos + off, default) for pos in range(n)}


def undetermined_of(res: dict, chunk_size: int) -> list[tuple[int, str]]:
    out = []
    for pos, st in statuses_of(res, chunk_size).items():
        if st in UNDETERMINED or st.startswith("OTHER_"):
            out.append((pos, st))
    return out


def queue_blob_sha(gh: GH) -> str | None:
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
    return json.loads(base64.b64decode(blob["content"]))


def empty_tally() -> dict:
    return {k: 0 for k in TALLY_KEYS} | {"probed": 0, "chunks_done": 0,
                                         "head_attempts": 0}


def main() -> int:
    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/")
    gh = GH(os.environ.get("GITHUB_TOKEN", ""), owner, repo)
    t_cycle0 = now()

    cfg = json.loads(Path("scan/deep_config.json").read_text())
    spec_v1 = G.load_spec("scan/spec_v1.json")
    cfg_sha = D.epoch_sha256(cfg)  # canonical config digest for provenance

    gh.ensure_branch(STATE, "main")

    # -------------------------------------------------- guards (unattended) -
    done = load_json(gh, "scan_complete_v5.json", STATE, None)
    if done:
        log("campaign already complete; nothing to do")
        return 0
    if gh.get_file("STOP", STATE) is not None:
        log("STOP file present on state branch; halting all dispatching")
        return 0

    # ---------------------------------------------------- queue init / load -
    q_sha0 = queue_blob_sha(gh)
    queue = load_queue_blob(gh, q_sha0) if q_sha0 else None
    if queue is None or queue.get("schema") != 3:
        log("initializing v5 deep-scan queue (frontier at epoch 1)")
        queue = {"schema": 3, "campaign": cfg["campaign_id"],
                 "config_sha256": cfg_sha,
                 "chunk_size": cfg["chunk_size"],
                 "frontier": {"next_epoch": 1, "next_chunk": 0,
                              "exhausted": False, "epochs": {},
                              "tallies": {}, "candidates_scheduled": 0,
                              "batches_seq": 0, "monitor_seq": 0},
                 "chunks": {}, "peak_active_workers": 0, "counts": {},
                 "updated_utc": iso(now())}
    frontier = queue["frontier"]
    chunks: dict = queue["chunks"]

    agg = load_json(gh, "agg.json", STATE,
                    {"ingested": {}, "found_ledger": {}, "verified": {}})

    # ------------------------------------------ frontier materialization ----
    # Grow the materialized window until QUEUE_WINDOW entries are open. Epoch
    # specs are committed atomically with the first entries that use them.
    epoch_commit_files: dict = {}
    n_new_entries = 0
    while not frontier.get("exhausted") and open_count(chunks) < QUEUE_WINDOW:
        n = frontier["next_epoch"]
        einfo = frontier["epochs"].get(str(n))
        if einfo is None:
            espec, _cov = D.build_epoch(cfg, spec_v1, n)
            if espec is None:
                frontier["exhausted"] = True
                frontier["exhausted_utc"] = iso(now())
                log("frontier exhausted: epoch ladder complete")
                break
            epath = f"epochs/epoch_{n}.json"
            tmp = Path(f".epoch_{n}.tmp")
            sha = D.write_epoch_file(espec, tmp)  # sha256 of the FILE bytes
            raw = tmp.read_bytes()
            tmp.unlink(missing_ok=True)
            total = espec["candidates_emitted"]
            cs = espec["chunk_size"]
            einfo = {"file": epath, "sha256": sha,
                     "total_candidates": total,
                     "num_chunks": (total + cs - 1) // cs,
                     "kind": espec.get("kind"),
                     "basis": espec.get("evidence_basis")}
            frontier["epochs"][str(n)] = einfo
            epoch_commit_files[epath] = raw
            log(f"epoch {n} built: {total:,} candidates, "
                f"{einfo['num_chunks']} chunks, sha {sha[:16]}")
        idx = frontier["next_chunk"]
        if idx >= einfo["num_chunks"]:
            frontier["next_epoch"] = n + 1
            frontier["next_chunk"] = 0
            continue
        key = f"e{n}-chunk-{idx:06d}"
        cs = cfg["chunk_size"]
        total = einfo["total_candidates"]
        size = min((idx + 1) * cs, total) - idx * cs
        chunks[key] = {"status": "PENDING", "kind": "epoch", "epoch": n,
                       "spec_sha256": einfo["sha256"], "size": size,
                       "job_attempts": 0, "attempt_seq": 0, "requeues": 0}
        frontier["next_chunk"] = idx + 1
        frontier["candidates_scheduled"] += chunks[key]["size"]
        n_new_entries += 1
    if n_new_entries:
        log(f"materialized {n_new_entries} chunks (window open={open_count(chunks)})")

    # cycle commit payload (built incrementally from here on)
    commit: dict = {"global_rate.json": json.dumps(
        load_json(gh, "global_rate.json", STATE, {"budget_rps": BUDGET_START}),
        indent=1) + "\n"}
    commit.update(epoch_commit_files)

    # ---------------------------------------------------- list state files --
    st, ref, _h = gh.api("GET", f"/repos/{owner}/{repo}/git/ref/heads/{STATE}")
    head_sha = ref["object"]["sha"]
    st, tree, _h = gh.api("GET", f"/repos/{owner}/{repo}/git/trees/{head_sha}?recursive=1")
    paths = [e["path"] for e in tree.get("tree", [])
             if e["type"] == "blob" and e["path"].startswith(("results/", "claims/"))]
    result_keys = {p.split("/", 1)[1][:-len(".result.json")]
                   for p in paths if p.startswith("results/") and p.endswith(".result.json")}
    claim_keys = {p.split("/", 1)[1].rsplit(".", 1)[0]
                  for p in paths if p.startswith("claims/") and p.endswith(".json")}
    log(f"state tree: {len(result_keys)} results, {len(claim_keys)} claims, "
        f"open chunks {open_count(chunks)}")

    # --------------------------------- ingest new results (exactly once) ----
    # Entry-driven: only keys that still own a queue entry are ingested;
    # completed entries are pruned into tallies in the same cycle, so a
    # result can never be ingested twice and queue.json stays bounded.
    new_keys = sorted(k for k in result_keys if k in chunks)
    ingested_now = []
    pruned_keys = []
    retry_builds = []  # (parent_key, res) needing retry batches
    for key in new_keys:
        ent = chunks[key]
        try:
            res = gh.get_file_json(f"results/{key}.result.json", STATE)
        except Exception as e:
            log(f"ingest read failed {key}: {e}")
            continue
        if not res:
            continue
        outc = res.get("outcome")
        if outc == "TEST":
            ent["status"] = "IGNORED_TEST"
            pruned_keys.append(key)
            continue
        if outc in ("COMPLETED", "COMPLETED_WITH_UNDETERMINED"):
            if ent.get("kind") == "retry_batch":
                # rounds accounting only; status counts reconcile the PARENT
                # bucket so a candidate is never double-counted across rounds
                tal0 = frontier["tallies"].setdefault("0", empty_tally())
                tal0["probed"] += res.get("candidates_probed_unique", 0)
                tal0["head_attempts"] += res.get("head_attempts_total", 0)
                tal0["chunks_done"] += 1
                _reconcile_batch_tallies(gh, frontier, ent, res,
                                         cfg["chunk_size"])
            else:
                tal = frontier["tallies"].setdefault(
                    str(res.get("epoch", 0)), empty_tally())
                st_map = res.get("statuses", {})
                for k, v in st_map.items():
                    if k == "200":
                        tal["found_200"] += v
                    elif k == "FOUND_INVALID":
                        tal["found_invalid"] += v
                    elif k == "404":
                        tal["confirmed_404"] += v
                    elif k in ("503", "TIMEOUT", "NETERR"):
                        tal["undetermined"] += v
                    else:
                        tal["other"] += v
                tal["probed"] += res.get("candidates_probed_unique", 0)
                tal["head_attempts"] += res.get("head_attempts_total", 0)
                tal["chunks_done"] += 1
            ent["status"] = "COMPLETED"
            ent["completed_at"] = res.get("finished_utc")
            pruned_keys.append(key)
            ingested_now.append((key, res))
            if outc == "COMPLETED_WITH_UNDETERMINED":
                retry_builds.append((key, res))
        else:  # PARTIAL or unexpected: consume one job attempt, resumable
            ent["job_attempts"] = ent.get("job_attempts", 0) + 1
            if ent["job_attempts"] >= JOB_ATTEMPT_CAP:
                _requeue_or_abandon(chunks, key, ent, "repeated partial")
            else:
                ent["status"] = "RETRY"
                ent["partial_result"] = f"results/{key}.result.json"
                ent["not_before"] = now() + RETRY_DELAYS_MIN.get(
                    ent["job_attempts"] + 1, 60) * 60
            ingested_now.append((key, res))
    log(f"ingested {len(ingested_now)} results, pruned {len(pruned_keys)}")

    for key in pruned_keys:
        chunks.pop(key, None)

    # ------------------------------------------------ stale RUNNING sweep ---
    runs = gh.list_runs("worker.yml", statuses=("queued", "in_progress"))
    active_run_ids = {r["id"] for r in runs}
    active_now = len(active_run_ids)
    queue["peak_active_workers"] = max(queue.get("peak_active_workers", 0),
                                       active_now)
    for key, ent in list(chunks.items()):
        if ent.get("status") != "RUNNING":
            continue
        claim = load_json(gh, f"claims/{key}.json", STATE, {}) \
            if key in claim_keys else {}
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
                _requeue_or_abandon(chunks, key, ent, "stale running")
            else:
                ent["status"] = "RETRY"
                ent["not_before"] = now() + STALE_RETRY_DELAY_MIN * 60
            ent["stale_note"] = (f"claim_age_min={age_min:.0f} "
                                 f"run_active={claim.get('run_id') in active_run_ids}")
            log(f"stale RUNNING -> {ent['status']}: {key}")

    # ---------------------- retry batches from undetermined observations ---
    seq = frontier.get("batches_seq", 0)
    new_batch_files: dict = {}
    epoch_spec_cache: dict = {}
    for key, res in retry_builds:
        if res.get("mode") == "test":
            continue
        if key.startswith("batch-"):
            parent = chunks.get(key) or {}
            level = parent.get("retry_level", 2) + 1
            batch_file = parent.get("batch_file")
            pos_map = None
            if batch_file:
                bdata = load_json(gh, batch_file, STATE, None)
                if bdata:
                    pos_map = {i: (c["map_id"], c["ts_ms"])
                               for i, c in enumerate(bdata["candidates"])}
        else:
            level = 2
            pn = parse_chunk_key(key)
            if not pn:
                continue
            en, cid = pn
            espec = _epoch_spec(gh, en, epoch_spec_cache)
            if espec is None:
                log(f"epoch spec {en} missing for retry build {key}")
                continue
            pos_map = {o - cid * cfg["chunk_size"]: (m, ts)
                       for o, m, ts, _t in G.iter_chunk(espec, cid)}
        if level > CANDIDATE_MAX_ROUNDS:
            continue
        cands = []
        for pos, prior in undetermined_of(res, cfg["chunk_size"]):
            if pos_map and pos in pos_map:
                m, ts = pos_map[pos]
                cands.append({"map_id": m, "ts_ms": ts,
                              "source_key": key, "source_ordinal": pos,
                              "prior_status": prior})
        if not cands:
            log(f"retry build {key}: no undetermined candidates matched "
                f"(level {level}, pos_map={len(pos_map) if pos_map else 0})")
            continue
        for i in range(0, len(cands), BATCH_SIZE):
            part = cands[i:i + BATCH_SIZE]
            bname = f"batch-{seq:04d}"
            seq += 1
            new_batch_files[f"retry_batches/{bname}.json"] = json.dumps({
                "batch_id": bname, "created_utc": iso(now()),
                "count": len(part), "retry_level": level,
                "parent_result": key, "candidates": part}, indent=1) + "\n"
            chunks[bname] = {"status": "PENDING", "kind": "retry_batch",
                             "retry_level": level,
                             "batch_file": f"retry_batches/{bname}.json",
                             "size": len(part), "parent_result": key,
                             "job_attempts": 0, "attempt_seq": 0, "requeues": 0}
    frontier["batches_seq"] = seq
    if new_batch_files:
        commit.update(new_batch_files)
        log(f"created {len(new_batch_files)} retry batches "
            f"(rounds<={CANDIDATE_MAX_ROUNDS})")

    # -------------------------------------------------- global rate law -----
    recent = [a for a in agg["ingested"].values()
              if a.get("finished_utc") and a.get("mode") != "test"
              and (now() - parse_utc(a["finished_utc"])) < 1500]
    nbad = sum(a["statuses"].get("503", 0) + a["statuses"].get("TIMEOUT", 0)
               + a["statuses"].get("NETERR", 0)
               + sum(v for k, v in a["statuses"].items() if k.startswith("OTHER"))
               for a in recent)
    ntot = sum(a.get("probed", 0) for a in recent)
    bad_frac = (nbad / ntot) if ntot else 0.0
    gr = load_json(gh, "global_rate.json", STATE, {"budget_rps": BUDGET_START})
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

    # ------------------------------------------------------- dispatching ----
    # Run-listing can lag behind dispatches (eventual consistency); treat
    # recently-dispatched RUNNING entries as presumed active so a listing lag
    # can never cause an over-dispatch storm.
    presumed_active = active_now
    recent_running = sum(
        1 for e in chunks.values()
        if e.get("status") == "RUNNING"
        and now() - parse_utc(e.get("dispatched_at", "")) < 180)
    if recent_running > presumed_active:
        presumed_active = recent_running
        log(f"dispatch capacity: runs listed {active_now}, "
            f"recent dispatches {recent_running} -> using {presumed_active}")
    dispatched = []
    if presumed_active < MAX_ACTIVE_WORKERS:
        def dispatchable(k, e):
            return (e.get("status") in ("PENDING", "RETRY")
                    and e.get("job_attempts", 0) < JOB_ATTEMPT_CAP
                    and now() >= e.get("not_before", 0))
        spec_keys = sorted(k for k, e in chunks.items()
                           if dispatchable(k, e) and e["kind"] == "epoch")
        batch_keys = sorted(k for k, e in chunks.items()
                            if dispatchable(k, e) and e["kind"] == "retry_batch")
        for key in spec_keys + batch_keys:
            if presumed_active + len(dispatched) >= MAX_ACTIVE_WORKERS:
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
                inputs = {"chunk_id": "0", "epoch": "0",
                          "retry_batch": ent["batch_file"],
                          "dispatch_token": token, "attempt": str(ent["attempt"]),
                          "mode": "full", "max_probes": "0",
                          "resume_from_result": ""}
            else:
                inputs = {"chunk_id": str(int(key.split("-chunk-")[1])),
                          "epoch": str(ent["epoch"]),
                          "retry_batch": "", "dispatch_token": token,
                          "attempt": str(ent["attempt"]), "mode": "full",
                          "max_probes": "0",
                          "resume_from_result": ent.get("partial_result", "")}
            if gh.dispatch("worker.yml", "main", inputs):
                dispatched.append(key)
            else:
                ent["status"] = "PENDING"
                ent["dispatch_token"] = ""
                ent["attempt_seq"] -= 1
                log(f"dispatch API failed for {key}; left PENDING")
        if dispatched:
            log(f"dispatched {len(dispatched)} workers "
                f"(active {presumed_active} -> {presumed_active + len(dispatched)})")
    for key in pruned_keys:
        if key in claim_keys:
            commit[f"claims/{key}.json"] = None  # tombstone -> delete
            # NOTE: results/{key}.result.json is NEVER deleted - it is the
            # source of truth for the final canonical accounting.

    # ---------------------------------------------- found verification ------
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

    # ------------------------------------------------------ queue commit ----
    counts = {}
    for e in chunks.values():
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    queue["counts"] = counts
    queue["updated_utc"] = iso(now())

    q_sha1 = queue_blob_sha(gh)
    if q_sha0 and q_sha1 and q_sha0 != q_sha1:
        log("queue.json changed underneath; rebasing mutations")
        fresh = load_queue_blob(gh, q_sha1) or queue
        import copy
        orig = copy.deepcopy(chunks)
        touched = {k: e for k, e in chunks.items() if orig.get(k) != e}
        fch = fresh.setdefault("chunks", {})
        for k in pruned_keys:
            fch.pop(k, None)
        for k, e in touched.items():
            fch[k] = e
        fr = fresh.setdefault("frontier", {})
        fr["batches_seq"] = max(fr.get("batches_seq", 0),
                                frontier.get("batches_seq", 0))
        fr["candidates_scheduled"] = max(fr.get("candidates_scheduled", 0),
                                         frontier.get("candidates_scheduled", 0))
        fresh["peak_active_workers"] = max(
            fresh.get("peak_active_workers", 0),
            queue.get("peak_active_workers", 0))
        fc = {}
        for e in fch.values():
            fc[e["status"]] = fc.get(e["status"], 0) + 1
        fresh["counts"] = fc
        fresh["updated_utc"] = iso(now())
        queue = fresh
        counts = fc
    frontier["monitor_seq"] = frontier.get("monitor_seq", 0) + 1
    commit["queue.json"] = json.dumps(queue, indent=1) + "\n"

    # prune agg ledger (rolling window for the rate law only)
    cutoff = now() - AGG_TTL_S
    agg["ingested"] = {k: v for k, v in agg["ingested"].items()
                       if parse_utc(v.get("finished_utc", "")) > cutoff}
    for key, res in ingested_now:
        agg["ingested"][key] = {
            "outcome": res.get("outcome"), "statuses": res.get("statuses", {}),
            "probed": res.get("candidates_probed_unique", 0),
            "finished_utc": res.get("finished_utc"), "mode": res.get("mode"),
            "rps_mean": res.get("rate", {}).get("rps_mean"),
            "p50": res.get("latency_ms", {}).get("p50"),
            "p95": res.get("latency_ms", {}).get("p95"),
        }
        for f in res.get("found", []):
            if f.get("zip_ok") and f.get("zip_crc_ok"):
                agg["found_ledger"][f["url"]] = {
                    "map_id": f["map_id"], "ts_ms": f["ts_ms"],
                    "sha256": f.get("sha256"), "bytes": f.get("bytes"),
                    "observed_utc": f.get("observed_utc"),
                    "status": "FOUND_CANDIDATE", "source_result": key,
                }
    commit["agg.json"] = json.dumps(agg, indent=1) + "\n"

    # ------------------------------------------------------- monitoring -----
    status = build_status(queue, frontier, counts, presumed_active, dispatched,
                          gr_out, bad_frac, recent, agg, gh, t_cycle0)
    commit["monitoring/status.json"] = json.dumps(status, indent=1) + "\n"
    report = render_report(status)
    commit["monitoring/report.md"] = report
    gh.commit_files(STATE, commit,
                    f"controller cycle: {counts} budget={gr_out['budget_rps']} "
                    f"epochs={sum(1 for _ in frontier.get('epochs', {}))}")

    if frontier["monitor_seq"] % MONITOR_MAIN_EVERY == 0:
        try:
            gh.commit_files("main", {
                "results/monitoring/report.md": report,
                "results/monitoring/status.json":
                    json.dumps(status, indent=1) + "\n"},
                f"monitoring snapshot {status['updated_utc']}")
        except Exception as e:
            log(f"main monitoring commit failed: {e}")

    # ------------------------------------------------------- completion -----
    open_work = counts.get("PENDING", 0) + counts.get("RETRY", 0) \
        + counts.get("RUNNING", 0)
    abandoned = counts.get("ABANDONED", 0)
    if frontier.get("exhausted") and open_work == 0 and len(agg["ingested"]) >= 0:
        log("frontier exhausted and queue drained; running final aggregation")
        r = subprocess.run([sys.executable, "scan/aggregate.py"],
                           capture_output=True, text=True, timeout=3000)
        print(r.stdout[-4000:], r.stderr[-2000:], flush=True)
        summary = _last_json(r.stdout)
        _publish_release(gh, status, summary, abandoned)
        gh.commit_files(STATE, {"scan_complete_v5.json": json.dumps(
            {"completed_utc": iso(now()), "status": status,
             "summary": summary}, indent=1) + "\n"},
            "deep scan v5 complete: ladder exhausted + queue drained + evidence aggregated")
    print("STATUS " + json.dumps(status), flush=True)
    return 0


def _reconcile_batch_tallies(gh: GH, frontier: dict, ent: dict, res: dict,
                             cs: int) -> None:
    """Move resolved retry candidates from 'undetermined' to their final
    status in the root chunk's epoch tally bucket (live canonical view)."""
    b = load_json(gh, ent.get("batch_file", ""), STATE, None)
    if not b:
        return
    parent = ent.get("parent_result") or ""
    seen = set()
    while parent.startswith("batch-") and parent not in seen:
        seen.add(parent)
        pb = load_json(gh, f"retry_batches/{parent}.json", STATE, None)
        if not pb:
            return
        parent = pb.get("parent_result") or ""
    pn = parse_chunk_key(parent)
    if not pn:
        return
    bucket = frontier["tallies"].setdefault(str(pn[0]), empty_tally())
    cands = b.get("candidates", [])
    for pos, s in statuses_of(res, cs).items():
        if pos >= len(cands):
            continue
        if s == "404":
            bucket["undetermined"] = max(0, bucket["undetermined"] - 1)
            bucket["confirmed_404"] += 1
        elif s == "200":
            bucket["undetermined"] = max(0, bucket["undetermined"] - 1)
            bucket["found_200"] += 1
        elif s == "FOUND_INVALID":
            bucket["undetermined"] = max(0, bucket["undetermined"] - 1)
            bucket["found_invalid"] += 1
        elif s.startswith("OTHER"):
            bucket["undetermined"] = max(0, bucket["undetermined"] - 1)
            bucket["other"] += 1
        # still-undetermined observations stay in the bucket


def open_count(chunks: dict) -> int:
    return sum(1 for e in chunks.values()
               if e.get("status") in ("PENDING", "RUNNING", "RETRY"))


def _requeue_or_abandon(chunks: dict, key: str, ent: dict, why: str) -> None:
    ent["requeues"] = ent.get("requeues", 0) + 1
    if ent["requeues"] > CHUNK_REQUEUE_CAP:
        ent["status"] = "ABANDONED"
        ent["abandon_reason"] = why
        log(f"ABANDONED after {ent['requeues']} requeues: {key} ({why})")
    else:
        ent["status"] = "PENDING"
        ent["not_before"] = now() + 120 * 60
        log(f"requeue {key} #{ent['requeues']} ({why})")


def _epoch_spec(gh: GH, n: int, cache: dict):
    if n not in cache:
        cache[n] = load_json(gh, f"epochs/epoch_{n}.json", STATE, None)
    return cache[n]


def _last_json(text: str):
    """Extract the last pretty-printed JSON object from aggregate stdout."""
    try:
        start = text.rindex("{\n  \"")
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(text[start:i + 1])
    except Exception:
        return None
    return None


def _publish_release(gh: GH, status: dict, summary, abandoned: int) -> None:
    body = render_report(status) + "\n## Final accounting\n\n```json\n" \
        + json.dumps(summary or {}, indent=1)[:8000] + "\n```\n"
    if abandoned:
        body += f"\nDISCLOSURE: {abandoned} chunk(s) ABANDONED after repeated " \
                "failures; their candidates remain unsearched and are listed " \
                "in the final accounting.\n"
    st, rel, _h = gh.api("POST", f"/repos/{gh.owner}/{gh.repo}/releases",
                         body={"tag_name": RELEASE_TAG,
                               "target_commitish": "main",
                               "name": "Deep Scan v5 - final evidence",
                               "body": body})
    if st == 201:
        log(f"release published: {rel.get('html_url')}")
    else:
        log(f"release publish failed ({st}); final files are committed to main")


def build_status(queue, frontier, counts, active_now, dispatched, gr_out,
                 bad_frac, recent, agg, gh, t_cycle0) -> dict:
    tal = frontier.get("tallies", {})
    tot = {k: sum(t.get(k, 0) for t in tal.values()) for k in TALLY_KEYS}
    tot["probed"] = sum(t.get("probed", 0) for t in tal.values())
    epochs_committed = {k: {"candidates": v.get("total_candidates"),
                            "chunks": v.get("num_chunks"),
                            "sha256": v.get("sha256", "")[:16]}
                        for k, v in frontier.get("epochs", {}).items()}
    return {
        "updated_utc": iso(now()),
        "campaign": queue.get("campaign"),
        "workflows": {"active_workers": active_now,
                      "peak_active_workers": queue.get("peak_active_workers", 0),
                      "max_allowed": MAX_ACTIVE_WORKERS,
                      "dispatched_this_cycle": len(dispatched)},
        "frontier": {"next_epoch": frontier.get("next_epoch"),
                     "next_chunk": frontier.get("next_chunk"),
                     "exhausted": frontier.get("exhausted", False),
                     "epochs_committed": epochs_committed,
                     "candidates_scheduled": frontier.get("candidates_scheduled", 0),
                     "tallies_by_epoch": tal,
                     "tallies_total": tot},
        "chunks": {k.lower(): v for k, v in counts.items()} | {
            "total": len(queue.get("chunks", {}))},
        "candidates_live": {
            "scheduled_total": frontier.get("candidates_scheduled", 0),
            "unique_confirmed_404": tot["confirmed_404"],
            "unique_found_200": tot["found_200"],
            "unique_found_invalid": tot["found_invalid"],
            "unique_undetermined": tot["undetermined"],
            "unique_other": tot["other"],
        },
        "found": {"candidates": len(agg.get("found_ledger", {})),
                  "verified": sum(1 for v in agg.get("verified", {}).values()
                                  if v.get("verified")),
                  "rejected": sum(1 for v in agg.get("verified", {}).values()
                                  if not v.get("verified"))},
        "retries": {"batches_seq": frontier.get("batches_seq", 0),
                    "retry_level_cap": CANDIDATE_MAX_ROUNDS,
                    "failed_chunks": counts.get("FAILED", 0),
                    "abandoned_chunks": counts.get("ABANDONED", 0)},
        "rate": {"global_budget_rps": gr_out["budget_rps"],
                 "measured_rps_recent": round(
                     sum(a.get("rps_mean") or 0 for a in recent)
                     / max(1, len(recent)), 1) if recent else 0.0,
                 "bad_frac_recent": round(bad_frac, 4),
                 "reason": gr_out["reason"],
                 "recent_window_results": len(recent)},
        "latency_ms_recent": {
            "p50": round(sum(a.get("p50") or 0 for a in recent)
                         / max(1, len(recent)), 1),
            "p95": round(sum(a.get("p95") or 0 for a in recent)
                         / max(1, len(recent)), 1)},
        "cycle_seconds": round(now() - t_cycle0, 1),
        "github_api_calls": gh.calls,
    }


def render_report(s: dict) -> str:
    c, w, r = s["chunks"], s["workflows"], s["rate"]
    f = s["frontier"]
    t = f["tallies_total"]
    lines = [
        "# Deep MAPS scan v5 - live status (unattended)",
        f"_Cycle completed {s['updated_utc']} (cycle {s['cycle_seconds']}s, "
        f"{s['github_api_calls']} API calls)_",
        "",
        "## Frontier (unbounded deep universe)",
        f"- Next epoch/chunk: **{f['next_epoch']} / {f['next_chunk']}** - "
        f"ladder exhausted: **{f['exhausted']}**",
        f"- Epochs committed: {len(f['epochs_committed'])} - "
        f"candidates scheduled so far: {f['candidates_scheduled']:,}",
        "",
        "## Workers",
        f"- Active workers: **{w['active_workers']}** "
        f"(peak {w['peak_active_workers']}, cap {w['max_allowed']})",
        f"- Dispatched this cycle: {w['dispatched_this_cycle']}",
        "",
        "## Materialized chunks (sliding window)",
        f"- Running **{c.get('running', 0)}** / pending **{c.get('pending', 0)}** / "
        f"retry **{c.get('retry', 0)}** / abandoned **{c.get('abandoned', 0)}** "
        f"(total in window {c['total']})",
        f"- Completed (pruned to tallies): **{t.get('chunks_done', 0):,}**",
        "",
        "## Candidates (unique, exact tallies)",
        f"- Confirmed 404: {t.get('confirmed_404', 0):,}",
        f"- FOUND 200: **{t.get('found_200', 0):,}** "
        f"(independently verified maps: **{s['found']['verified']}**)",
        f"- Undetermined 503/timeout (never counted as misses): "
        f"{t.get('undetermined', 0):,}",
        f"- Other errors: {t.get('other', 0):,}",
        "",
        "## Rate & latency (measured)",
        f"- Global budget: {r['global_budget_rps']} rps - {r['reason']}",
        f"- Measured mean worker rps (recent window): {r['measured_rps_recent']} "
        f"over {r['recent_window_results']} results",
        f"- Rolling latency p50/p95: {s['latency_ms_recent']['p50']} / "
        f"{s['latency_ms_recent']['p95']} ms",
        f"- Recent bad-status fraction (503+timeout+other): {r['bad_frac_recent']:.4f}",
        "",
        "Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; "
        "a map exists only after direct ZIP + CRC + SHA-256 verification; "
        "all v1-confirmed candidates are excluded, never re-probed.",
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
