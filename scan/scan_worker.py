#!/usr/bin/env python3
"""Adaptive-rate scan worker for one deterministic chunk (GitHub Actions job).

Honesty invariants implemented here:
- Candidate list comes ONLY from the frozen spec (or a committed retry-batch file).
- 503/timeout are recorded as undetermined and never converted to 404.
- Every HTTP 200 is treated as a candidate only; bytes are downloaded directly
  from the origin and verified (ZIP structure + CRC + SHA-256) before being
  reported as FOUND_200; the controller re-verifies independently.
- Partial runs commit PARTIAL results; a chunk is never marked complete
  unless every scheduled candidate in it was probed.
- Duplicate probes (across attempts/resumes) are counted in attempts_total
  and de-duplicated in unique-candidate accounting by the aggregator.

Rate control: per-worker AIMD (start conservative, increase on stability,
multiplicative decrease + bounded exponential backoff on 503/timeout), inside
a global budget published by the controller (budget/active_hint per worker).
No proxies, no IP rotation, no bypass of any access control.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import os
import signal
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen_candidates as G  # noqa: E402
from zipcheck import verify_zip_bytes  # noqa: E402

HOST = "staticgs.sandboxol.com"
URL_FMT = "https://{host}/sandbox/games/maps/{{map_id}}.{{ts_ms}}.zip".format(host=HOST)
UA = "blockman-go-archival-scan/5.0 (maps-path deep-scan research; adaptive rate; no bypass)"

MIN_RPS = 12.0
START_RPS = 25.0
MAX_RPS = 250.0
INCR_RPS = 5.0            # additive increase per stable window
WINDOW_S = 10.0
BACKOFF_BASE_S = 2.0
BACKOFF_MAX_S = 60.0
INLINE_RETRY_PAUSE_S = 1.5
HEAD_TIMEOUT_S = 12.0
GET_TIMEOUT_S = 120.0
MAX_ZIP_BYTES = 80 * 1024 * 1024
COMMIT_ZIP_MAX_BYTES = 15 * 1024 * 1024
MAX_RUNTIME_S = 38 * 60
BUDGET_REFRESH_S = 60.0

ST404, ST200, ST503 = "404", "200", "503"
STTIMEOUT, STNETERR, STOTHER = "TIMEOUT", "NETERR", "OTHER"
STINV = "FOUND_INVALID"


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class AIMD:
    """Per-worker adaptive rate: additive increase, multiplicative decrease,
    bounded exponential backoff pauses after repeated bad outcomes."""

    def __init__(self):
        self.rps = START_RPS
        self.backoff_n = 0
        self.pause_until = 0.0
        self.win_bad = 0
        self.win_all = 0

    def record(self, bad: bool) -> None:
        self.win_all += 1
        if bad:
            self.win_bad += 1
            if self.win_bad >= 12 and self.win_all >= 25:
                self._hit()

    def _hit(self) -> None:
        self.rps = max(MIN_RPS, self.rps * 0.5)
        pause = min(BACKOFF_BASE_S * (2 ** self.backoff_n), BACKOFF_MAX_S)
        self.pause_until = max(self.pause_until, time.monotonic() + pause)
        self.backoff_n += 1
        self.win_bad = 0
        self.win_all = 0

    def window_tick(self, bad_frac: float, err_frac: float, cap: float) -> None:
        self.rps = min(self.rps, max(cap, MIN_RPS))
        if bad_frac <= 0.005 and err_frac <= 0.01:
            self.backoff_n = max(0, self.backoff_n - 1)
            self.rps = min(self.rps + INCR_RPS, cap, MAX_RPS)
        elif bad_frac > 0.02 or err_frac > 0.03:
            self.rps = max(MIN_RPS, self.rps * 0.5)
            self.backoff_n += 1
        # else hold

    def clamp(self, cap: float) -> None:
        self.rps = min(max(self.rps, MIN_RPS), max(cap, MIN_RPS), MAX_RPS)


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = max(0, min(len(sorted_vals) - 1, int(round((p / 100.0) * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def build_candidates(spec: dict, args) -> tuple[list[tuple[int, str, int]], str]:
    """Return ([(pos, map_id, ts_ms)], candidate_source). pos = position inside
    this work item (for spec chunks pos == global stream ordinal)."""
    if args.retry_batch:
        batch = G.load_retry_batch(args.retry_batch)
        cap = args.max_probes or len(batch)
        return ([(i, c["map_id"], int(c["ts_ms"])) for i, c in enumerate(batch[:cap])],
                f"retry_batch:{args.retry_batch}")
    out = [(o, m, ts) for o, m, ts, _t in G.iter_chunk(spec, args.chunk_id)]
    if args.max_probes:
        out = out[:args.max_probes]
    return out, (f"epoch:{args.epoch}" if args.epoch > 0 else "spec")


async def download_and_verify(session, url: str, map_id: str, ts_ms: int,
                              found_dir: Path) -> dict:
    rec = {"map_id": map_id, "ts_ms": ts_ms, "url": url,
           "observed_utc": now_utc(), "get_status": None}
    try:
        timeout = aiohttp.ClientTimeout(total=GET_TIMEOUT_S, sock_read=60)
        async with session.get(url, timeout=timeout) as r:
            rec["get_status"] = r.status
            rec["content_type"] = r.headers.get("Content-Type", "")
            data = await r.read() if r.status == 200 else b""
    except Exception as e:
        rec.update(verify_zip_bytes(b""))
        rec["error"] = f"GET error: {e}"
        return rec
    if rec.get("get_status") != 200:
        rec.update(verify_zip_bytes(b""))
        rec["error"] = f"GET status {rec['get_status']} after HEAD 200"
        return rec
    if len(data) > MAX_ZIP_BYTES:
        rec.update(verify_zip_bytes(b""))
        rec["error"] = "payload exceeds MAX_ZIP_BYTES"
        return rec
    rec.update(verify_zip_bytes(data))
    if rec["zip_ok"] and rec["zip_crc_ok"]:
        found_dir.mkdir(parents=True, exist_ok=True)
        (found_dir / f"{map_id}.{ts_ms}.zip").write_bytes(data)
    return rec


async def probe_one(session, map_id: str, ts_ms: int, aimd: AIMD,
                    head_attempts: list, latencies: collections.deque,
                    stop: asyncio.Event, found_dir: Path):
    """One candidate: HEAD (+one inline retry for transient), GET+verify on 200.
    Returns (status, found_record_or_None). Never converts 503 to 404."""
    url = URL_FMT.format(map_id=map_id, ts_ms=ts_ms)
    pause = aimd.pause_until - time.monotonic()
    if pause > 0:
        await asyncio.sleep(pause)
    code, err_kind = None, None
    for attempt in (0, 1):
        if stop.is_set():
            return STTIMEOUT, None
        head_attempts[0] += 1
        t0 = time.monotonic()
        try:
            async with session.head(url) as resp:
                latencies.append((time.monotonic() - t0) * 1000.0)
                code = resp.status
                if code == 200:
                    aimd.record(False)
                    rec = await download_and_verify(session, url, map_id, ts_ms,
                                                    found_dir)
                    if rec.get("zip_ok") and rec.get("zip_crc_ok"):
                        return ST200, rec
                    return STINV, rec
                if code == 404:
                    aimd.record(False)
                    return ST404, None
                aimd.record(True)
                err_kind = None
        except asyncio.TimeoutError:
            latencies.append((time.monotonic() - t0) * 1000.0)
            aimd.record(True)
            err_kind = "timeout"
            code = None
        except aiohttp.ClientError:
            aimd.record(True)
            err_kind = "neterr"
            code = None
        if attempt == 0:
            await asyncio.sleep(INLINE_RETRY_PAUSE_S)
    if code is None:
        return (STTIMEOUT if err_kind == "timeout" else STNETERR), None
    if code == 503:
        return ST503, None
    return (f"OTHER_{code}" if code != 429 else "OTHER_429"), None


async def run(args) -> int:
    from gh import GH
    owner, repo = os.environ.get("GITHUB_REPOSITORY", "local/local").split("/")
    gh = GH(os.environ.get("GITHUB_TOKEN", ""), owner, repo)
    branch = args.state_branch

    # ---- spec resolution: v1 spec (epoch 0) or committed deep epoch spec ----
    if args.epoch > 0:
        if args.spec:
            spec_path = args.spec  # local-testing override
        else:
            raw = gh.get_file(f"epochs/epoch_{args.epoch}.json", branch)
            if raw is None:
                print(f"FATAL: epochs/epoch_{args.epoch}.json not found on {branch}",
                      flush=True)
                return 5
            if isinstance(raw, dict):
                raw = json.dumps(raw)  # defensive: already-parsed body
            if isinstance(raw, bytes):
                spec_path = ".epoch_spec.json"
                Path(spec_path).write_bytes(raw)
            else:
                spec_path = ".epoch_spec.json"
                Path(spec_path).write_text(str(raw))
    else:
        spec_path = args.spec or "scan/spec_v1.json"
    spec = G.load_spec(spec_path)
    if args.spec_sha and spec["_sha256"] != args.spec_sha:
        print(f"FATAL: spec digest mismatch {spec['_sha256']} != {args.spec_sha}",
              flush=True)
        return 5

    batch_stem = Path(args.retry_batch).stem if args.retry_batch else ""
    chunk_key = (batch_stem if args.retry_batch else
                 (f"e{args.epoch}-chunk-{args.chunk_id:06d}" if args.epoch > 0
                  else f"chunk-{args.chunk_id:06d}"))
    key = (("test-" if args.mode == "test" else "") + chunk_key)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    found_dir = out_dir / "found"

    # -------- ownership validation (reject duplicate ownership) ------------
    # A re-run is allowed ONLY to resume from an earlier PARTIAL result of the
    # same queue entry (attempt <= current); a full/other result rejects.
    prior_result = None
    if args.skip_ownership:
        print("LOCAL-TEST MODE: ownership checks skipped", flush=True)
    else:
        entry = {}
        for attempt_read in range(3):
            queue = gh.get_file_json("queue.json", branch) or {"chunks": {}}
            entry = (queue.get("chunks") or {}).get(key, {})
            if entry.get("status") == "RUNNING" and \
               entry.get("dispatch_token") == args.dispatch_token:
                break
            if entry.get("status") == "PENDING" and attempt_read < 2:
                # possible controller commit lag (dispatch -> queue commit);
                # re-read briefly. A RUNNING entry with a foreign token or an
                # existing full result file is still rejected immediately below.
                time.sleep(15)
                continue
            break
        if entry.get("status") != "RUNNING" or entry.get("dispatch_token") != args.dispatch_token:
            print(f"OWNERSHIP-REJECTED: {key} status={entry.get('status')} "
                  f"token_match={entry.get('dispatch_token') == args.dispatch_token}",
                  flush=True)
            return 3
        prior_result = gh.get_file_json(f"results/{key}.result.json", branch)
        if prior_result is not None and (
                prior_result.get("outcome") != "PARTIAL"
                or int(prior_result.get("attempt", 0)) > int(args.attempt)):
            print(f"OWNERSHIP-REJECTED: {key} has result file "
                  f"outcome={prior_result.get('outcome')} "
                  f"attempt={prior_result.get('attempt')}", flush=True)
            return 3
        if entry.get("spec_sha256") not in (None, spec["_sha256"]):
            print("FATAL: queue spec digest mismatch", flush=True)
            return 5

    if args.retry_batch and not Path(args.retry_batch).exists():
        # batch files live on the state branch when running in Actions
        rb = gh.get_file_json(args.retry_batch, branch)
        if rb is None:
            print(f"FATAL: retry batch {args.retry_batch} not found on {branch}",
                  flush=True)
            return 5
        Path(args.retry_batch).parent.mkdir(parents=True, exist_ok=True)
        Path(args.retry_batch).write_text(json.dumps(rb))

    candidates, cand_source = build_candidates(spec, args)
    scheduled_full = len(candidates)  # full scheduled count (before resume skip)
    # -------- resume support: skip ordinals already confirmed in PARTIAL ----
    # Auto-resume from the entry's own PARTIAL result (or explicit path).
    resume_note = None
    carried_head_attempts = 0
    carried: dict[int, tuple[str, dict | None]] = {}
    prior = prior_result
    if prior is None and args.resume_from_result:
        try:
            prior = gh.get_file_json(args.resume_from_result, branch)
        except Exception:
            prior = None
    if prior is not None:
        try:
            if prior.get("outcome") == "PARTIAL" and prior.get("default_status"):
                pdef = prior["default_status"]
                src = prior.get("candidate_source") or ""
                off = (prior.get("chunk_id") or 0) * spec["chunk_size"] \
                    if (src == "spec" or src.startswith("epoch:")) else 0
                prior_status = {}
                for pos in range(prior.get("candidates_scheduled", 0)):
                    prior_status[pos] = pdef
                for pos, st in prior.get("exceptions", []):
                    prior_status[int(pos) - off] = st
                prior_found_by_url = {f["url"]: f for f in prior.get("found", [])}
                skip = {pos for pos, st in prior_status.items()
                        if st in (ST404, ST200)}
                skip_g = {pos + off for pos in skip}  # candidates carry GLOBAL ordinals
                before = len(candidates)
                kept = []
                for (p, m, t) in candidates:
                    u = URL_FMT.format(map_id=m, ts_ms=t)
                    if p in skip_g or u in prior_found_by_url:
                        st = prior_status.get(p - off)
                        rec = prior_found_by_url.get(u)
                        if u in prior_found_by_url:
                            st = ST200 if (rec or {}).get("zip_ok") else STINV
                        carried[p] = (st, rec)
                    else:
                        kept.append((p, m, t))
                candidates = kept
                carried_head_attempts = int(prior.get("head_attempts_total", 0))
                resume_note = (f"resume from attempt {prior.get('attempt')}: "
                               f"skipped {before - len(candidates)} "
                               f"confirmed/found, carrying them into accounting")
            elif prior_result is not None:
                print(f"OWNERSHIP-REJECTED: {key} prior result not resumable",
                      flush=True)
                return 3
        except Exception as e:
            resume_note = f"resume load failed ({e}); full re-probe"

    # -------- claim (startup record + run mapping) -------------------------
    if not args.skip_ownership:
        gh.commit_files(branch, {
            f"claims/{key}.json": json.dumps({
                "key": key,
                "run_id": int(os.environ.get("GITHUB_RUN_ID", 0)),
                "run_url": "https://github.com/{o}/{r}/actions/runs/{rid}".format(
                    o=owner, r=repo, rid=os.environ.get("GITHUB_RUN_ID", 0)),
                "dispatch_token": args.dispatch_token, "attempt": args.attempt,
                "mode": args.mode, "spec_sha256": spec["_sha256"],
                "candidates": len(candidates), "started_utc": now_utc(),
                "resume_note": resume_note,
            }, indent=1) + "\n",
        }, f"claim {key} (attempt {args.attempt})")

    # -------- probe loop ----------------------------------------------------
    latencies: collections.deque = collections.deque(maxlen=4096)
    windows = []
    aimd = AIMD()
    stop = asyncio.Event()
    started = time.monotonic()
    started_utc = now_utc()
    budget = {"budget_rps": 1500.0, "active_hint": 20}
    last_budget_fetch = 0.0
    last_window = time.monotonic()
    win_counts = collections.Counter()
    next_send = time.monotonic()
    head_attempts = [0]

    def local_cap() -> float:
        return max(MIN_RPS, min(MAX_RPS, budget["budget_rps"] /
                                max(1, int(budget.get("active_hint", 20)))))

    conn = aiohttp.TCPConnector(limit=320, ttl_dns_cache=600,
                                enable_cleanup_closed=False)
    timeout = aiohttp.ClientTimeout(total=HEAD_TIMEOUT_S, connect=8)
    results: dict[int, tuple[str, dict | None]] = {}
    tasks: set[asyncio.Task] = set()
    cand_iter = iter(candidates)
    exhausted = False

    async def handle(pos: int, m: str, ts: int) -> None:
        nonlocal next_send
        if stop.is_set():
            return  # never probed: absent from results (honest partial)
        wait = next_send - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        jitter = 0.9 + 0.2 * ((pos * 2654435761) % 1000) / 1000.0
        next_send = max(next_send, time.monotonic()) + jitter / max(aimd.rps, 1.0)
        st, rec = await probe_one(session, m, ts, aimd, head_attempts,
                                  latencies, stop, found_dir)
        results[pos] = (st, rec)
        win_counts[st] += 1
        win_counts["_all"] += 1

    def sig(signum, frm):
        stop.set()
    signal.signal(signal.SIGTERM, sig)
    signal.signal(signal.SIGINT, sig)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout,
                                     headers={"User-Agent": UA},
                                     trust_env=False) as session:
        def spawn(target: int) -> None:
            nonlocal exhausted
            if exhausted or stop.is_set():
                return
            while len(tasks) < target:
                try:
                    pos, m, ts = next(cand_iter)
                except StopIteration:
                    exhausted = True
                    return
                t = asyncio.create_task(handle(pos, m, ts))
                tasks.add(t)
                t.add_done_callback(tasks.discard)

        spawn(256)
        while tasks or not exhausted:
            if not stop.is_set() and time.monotonic() - started > MAX_RUNTIME_S:
                print("max runtime reached; graceful partial stop", flush=True)
                stop.set()
            await asyncio.sleep(1.0)
            spawn(256)
            now = time.monotonic()
            if now - last_budget_fetch > BUDGET_REFRESH_S and not stop.is_set():
                try:
                    b = gh.get_file_json("global_rate.json", branch)
                    if isinstance(b, dict) and "budget_rps" in b:
                        budget = b
                except Exception:
                    pass
                last_budget_fetch = now
                aimd.clamp(local_cap())
            if now - last_window >= WINDOW_S:
                dur = now - last_window
                n = max(1, win_counts["_all"])
                bad = (win_counts[ST503] + sum(v for k, v in win_counts.items()
                                               if k.startswith("OTHER"))) / n
                err = (win_counts[STTIMEOUT] + win_counts[STNETERR]) / n
                aimd.window_tick(bad, err, local_cap())
                lat_sorted = sorted(latencies)
                windows.append([round(dur, 1), round(win_counts["_all"] / dur, 2),
                                win_counts[ST200], win_counts[ST404],
                                win_counts[ST503], win_counts[STTIMEOUT],
                                win_counts[STNETERR],
                                round(percentile(lat_sorted, 50), 1),
                                round(percentile(lat_sorted, 95), 1)])
                win_counts = collections.Counter()
                last_window = now

    # -------- aggregate result ----------------------------------------------
    dur = time.monotonic() - started
    for p, (st, rec) in carried.items():
        if p not in results:
            results[p] = (st, rec)  # confirmed/found positions from the prior partial
    probed = len(results)
    scheduled = scheduled_full
    status_counter = collections.Counter(st for st, _ in results.values())
    default_status = (status_counter.most_common(1)[0][0] if probed else None)
    exceptions = []
    found = []
    for pos in sorted(results):
        st, rec = results[pos]
        if st != default_status:
            exceptions.append([pos, st])
        if rec is not None:
            found.append(rec)
    partial = probed < scheduled
    outcome = ("TEST" if args.mode == "test" else
               "PARTIAL" if partial else
               "COMPLETED_WITH_UNDETERMINED" if any(
                   st not in (ST404, ST200) for st in status_counter) else
               "COMPLETED")
    lat_sorted = sorted(latencies)
    result = {
        "schema": 1, "key": key,
        "chunk_id": None if args.retry_batch else args.chunk_id,
        "epoch": args.epoch,
        "attempt": args.attempt, "mode": args.mode,
        "spec_sha256": spec["_sha256"],
        "run_id": int(os.environ.get("GITHUB_RUN_ID", 0)),
        "candidate_source": cand_source,
        "started_utc": started_utc, "finished_utc": now_utc(),
        "duration_s": round(dur, 1),
        "candidates_scheduled": scheduled,
        "candidates_probed_unique": probed,
        "head_attempts_total": head_attempts[0] + carried_head_attempts,
        "statuses": dict(status_counter),
        "default_status": default_status,
        "exceptions": exceptions,
        "found": found,
        "rate": {"rps_mean": round(probed / max(dur, 1e-6), 2),
                 "rps_final": round(aimd.rps, 2),
                 "local_cap": round(local_cap(), 2),
                 "budget_rps": budget["budget_rps"],
                 "active_hint": budget.get("active_hint")},
        "latency_ms": {"p50": round(percentile(lat_sorted, 50), 1),
                       "p95": round(percentile(lat_sorted, 95), 1)},
        "windows": windows[:600],
        "outcome": outcome,
        "notes": [n for n in [resume_note] if n],
    }

    # -------- persist result (atomic commit; artifact fallback) -------------
    local = out_dir / f"{key}.result.json"
    local.write_text(json.dumps(result, indent=1) + "\n")
    files = {f"results/{key}.result.json": local.read_bytes()}
    for f in found_dir.glob("*.zip"):
        if f.stat().st_size <= COMMIT_ZIP_MAX_BYTES:
            files[f"found_zips/{f.name}"] = f.read_bytes()
    commit_ok = False
    if args.skip_ownership:
        commit_ok = True  # local test: result stays on disk only
        print("LOCAL-TEST MODE: result committed to disk only", flush=True)
    else:
        try:
            gh.commit_files(branch, files,
                            f"result {key} attempt {args.attempt}: "
                            f"{result['statuses']} outcome={outcome}")
            commit_ok = True
        except Exception as e:
            print(f"RESULT-COMMIT-FAILED ({e}); artifact fallback only", flush=True)
    print(f"DONE {key} outcome={outcome} probed={probed}/{scheduled} "
          f"statuses={dict(status_counter)} commit_ok={commit_ok} "
          f"rps_mean={result['rate']['rps_mean']} "
          f"p50={result['latency_ms']['p50']}ms p95={result['latency_ms']['p95']}ms",
          flush=True)
    return 0 if commit_ok else 4


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spec", default="",
                    help="spec path override (default: spec_v1 for epoch 0, "
                         "committed epochs/epoch_N.json for epoch > 0)")
    ap.add_argument("--epoch", type=int, default=0,
                    help="deep-scan epoch number (0 = legacy v1 spec)")
    ap.add_argument("--spec-sha", default="",
                    help="expected spec digest; verified against queue entry")
    ap.add_argument("--chunk-id", type=int, default=0)
    ap.add_argument("--retry-batch", default="")
    ap.add_argument("--dispatch-token", default="")
    ap.add_argument("--attempt", type=int, default=1)
    ap.add_argument("--mode", choices=("full", "test"), default="full")
    ap.add_argument("--max-probes", type=int, default=0,
                    help="cap on candidates probed (platform smoke tests)")
    ap.add_argument("--resume-from-result", default="")
    ap.add_argument("--out-dir", default="worker_out")
    ap.add_argument("--state-branch", default="scan-state")
    ap.add_argument("--skip-ownership", action="store_true",
                    help="LOCAL TESTING ONLY: run without queue ownership checks")
    args = ap.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
