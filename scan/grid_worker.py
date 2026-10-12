#!/usr/bin/env python3
"""Grid v3 worker: persistent claim-and-sweep harvester (GitHub Actions job).

Design contract (grid-architecture v3):

- One job = one runner = up to `--minutes` of continuous scanning. The worker
  claims packages from the frozen plan (scan/grid_plan.json on the code
  branch), sweeps them ms-dense (every candidate timestamp ms 0..999 within
  every second - no 1-second sampling, per operator requirement), commits
  results to its own shard file, and chains to the next package. This removes
  the v5/v6 per-chunk workflow dispatch overhead (~35s setup per 25s scan).

- Claims: grid_state/claims/{pid}.json on the state branch (read -> commit ->
  re-read; last-writer-wins with re-read confirmation). Coverage truth lives
  in worker shards; duplicate scans waste work but never corrupt accounting.

- Rate control: per-worker AIMD tuned for the grid (start 600 rps, +200 per
  clean 10s window, cap min(budget/active_hint, 8000)), adaptive consumer
  pool (concurrency ~= rps * RTT * 1.35, clamped [64, 1800]), inline retry on
  transient failures. 5xx/timeout are NEVER recorded as 404 - the package
  cursor only advances over seconds where every probe returned 200/404.

- Hits: HTTP 200 -> direct GET from origin + ZIP structure/CRC/SHA-256
  verification (zipcheck). Verified hits are committed immediately to
  grid_state/found/ (metadata + zip when <= 15MB) and the worker shard, and
  trigger the controller. The first hit injects an INLINE family expansion
  (same-path-prefix sibling series +/-90s; map bundles +/-2.5s) at absolute
  priority: the feeder serves expansion ordinals before the package stream
  through the same single pacer (no double-rate windows).

- GitHub API discipline: the built-in GITHUB_TOKEN is limited to ~1000 REST
  calls/hour/repository fleet-wide. Workers keep calls O(packages):
  budget/STOP refresh every 20 min, shard commits on claim/heartbeat/hit/
  completion, coverage read on a slow cadence.

- No proxies, no IP rotation, no bypass of any access control: HEAD/GET only
  against the public CDN with honest adaptive backoff.
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import collections
import json
import os
import signal
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent))
from zipcheck import verify_zip_bytes  # noqa: E402

HOST = "staticgs.sandboxol.com"
UA = "blockman-go-archival-grid/3.0 (ms-dense research sweep; adaptive rate; no bypass)"

# AIMD v3 (grid-tuned; v5 constants capped a worker at 250 rps)
MIN_RPS = 60.0
START_RPS = 600.0
MAX_RPS = 8000.0
INCR_RPS = 200.0
DECAY = 0.6
WINDOW_S = 10.0
STREAK_PAUSE_S = 1.5

HEAD_TIMEOUT_S = 15.0
GET_TIMEOUT_S = 120.0
MAX_ZIP_BYTES = 80 * 1024 * 1024
COMMIT_ZIP_MAX_BYTES = 15 * 1024 * 1024
BUDGET_REFRESH_S = 1200.0        # GITHUB_TOKEN fleet budget discipline
HEARTBEAT_S = 600.0
COVERAGE_REFRESH_S = 1800.0
SHARD_DIR = "grid_state/shards"
CLAIM_DIR = "grid_state/claims"
FOUND_DIR = "grid_state/found"
OVERLAY_PATH = "grid_state/grid_plan_cascade.json"
EXCEPTIONS_CAP = 200_000
CONC_MIN, CONC_MAX = 64, 1800

ST404, ST200 = "404", "200"
ST503, STTIMEOUT, STNETERR = "503", "TIMEOUT", "NETERR"


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ==========================================================================
# pure logic (unit-tested offline in test_grid.py)
# ==========================================================================
class AIMD3:
    """Grid-tuned AIMD: fast clean ramp, multiplicative decay on badness."""

    def __init__(self):
        self.rps = START_RPS
        self.pause_until = 0.0
        self.bad_streak = 0

    def record(self, bad: bool) -> None:
        if bad:
            self.bad_streak += 1
            if self.bad_streak >= 20:
                self.rps = max(MIN_RPS, self.rps * DECAY)
                self.pause_until = max(self.pause_until,
                                       time.monotonic() + STREAK_PAUSE_S)
                self.bad_streak = 0
        else:
            if self.bad_streak:
                self.bad_streak -= 1

    def window_tick(self, bad_frac: float, err_frac: float, cap: float) -> float:
        cap = max(cap, MIN_RPS)
        if bad_frac <= 0.005 and err_frac <= 0.01:
            self.rps = min(self.rps + INCR_RPS, cap, MAX_RPS)
        elif bad_frac > 0.02 or err_frac > 0.03:
            self.rps = max(MIN_RPS, self.rps * DECAY)
        self.rps = min(max(self.rps, MIN_RPS), cap, MAX_RPS)
        return self.rps


class SecondsTracker:
    """Cursor accounting for ms-dense packages.

    A package is an ordered list of blocks (normally 1000-ms seconds). The
    cursor (probed_through) may only advance across blocks where EVERY
    candidate probe completed with a clean status (200/404). 5xx/timeout
    blocks stay behind the cursor and are honestly re-probed on resume.
    """

    def __init__(self, blocks: list[int], start_local: int = 0):
        self.blocks = blocks
        self.starts = [0]
        for b in blocks:
            self.starts.append(self.starts[-1] + b)
        # Eager pending population: a block may only be advanced (counted
        # into the cursor) after EVERY one of its probes completed clean.
        # Lazy population here was a real bug: the advance walk would see
        # unprobed blocks as "no pending entry" and skip them.
        self.pending: dict[int, list[int]] = {i: [b, 0]
                                              for i, b in enumerate(blocks)}
        self.ptr = 0
        skipped = 0
        while self.ptr < len(blocks) and skipped + blocks[self.ptr] <= start_local:
            skipped += blocks[self.ptr]
            del self.pending[self.ptr]
            self.ptr += 1
        self.cursor_local = skipped

    def block_of(self, local_ord: int) -> int:
        return min(bisect.bisect_right(self.starts, local_ord) - 1,
                   len(self.blocks) - 1)

    def complete(self, local_ord: int, ok: bool) -> None:
        i = self.block_of(local_ord)
        st = self.pending.get(i)
        if st is None:
            return  # block already clean-completed and advanced past
        st[0] -= 1
        if not ok:
            st[1] += 1
        if st[0] <= 0 and st[1] == 0:
            del self.pending[i]
            self._advance()
        # Dirty blocks (bad > 0) deliberately stay in `pending` forever:
        # they act as barriers so the cursor never crosses a contaminated
        # second; resume re-probes them from the cursor onward.

    def _advance(self) -> None:
        while self.ptr < len(self.blocks) and self.ptr not in self.pending:
            self.cursor_local += self.blocks[self.ptr]
            self.ptr += 1

    def probed_through_local(self) -> int:
        return self.cursor_local


class OrdinalStream:
    """Deterministic ordinal -> (url, map_id, ts_ms) mapping for a package."""

    def __init__(self, pkg: dict):
        self.items = pkg["items"]
        self.starts = [it["ordinal0"] for it in self.items]
        self.end = pkg["ordinal0"] + pkg["candidates"]

    def at(self, ordinal: int) -> tuple[str, str, int]:
        i = bisect.bisect_right(self.starts, ordinal) - 1
        it = self.items[i]
        ts = it["t0"] + (ordinal - it["ordinal0"])
        url = it["path_fmt"].format(map_id=it["map_id"], ts_ms=ts)
        return url, it["map_id"], ts

    def blocks(self) -> list[int]:
        out = []
        for it in self.items:
            n = it["t1"] - it["t0"]
            while n > 0:
                take = min(1000, n)
                out.append(take)
                n -= take
        return out


# ==========================================================================
# worker runtime
# ==========================================================================
class Worker:
    def __init__(self, args):
        self.args = args
        self.branch = args.state_branch
        from gh import GH
        owner, repo = os.environ.get("GITHUB_REPOSITORY", "local/local").split("/")
        self.gh = GH(os.environ.get("GITHUB_TOKEN", ""), owner, repo)
        self.worker_key = (f"w{args.index:02d}-"
                           f"{os.environ.get('GITHUB_RUN_ID', 'local')}")
        self.plan = json.loads(Path(args.plan).read_bytes())
        self.overlay_pkgs: list[dict] = []
        self.overlay_rev = 0
        self.coverage: dict = {}
        self.budget = {"budget_rps": 60000.0, "active_hint": 20}
        self.found_dir = Path(args.out_dir) / "found"
        self.stop = asyncio.Event()
        self.probed_total = 0
        self.hits_all: list[dict] = []
        self.windows: list[list] = []
        self.aimd = AIMD3()
        self.latencies: collections.deque = collections.deque(maxlen=4096)
        self.current_pid = ""
        self.target_conc = 512
        self.deadline = time.monotonic() + args.minutes * 60
        self.last_budget_read = 0.0
        self.last_coverage_read = 0.0
        self.last_heartbeat = 0.0
        self.package_statuses: dict[str, dict] = {}
        self.exp_windows: collections.deque = collections.deque()
        self._hit_commit_pending = False
        self._controller_triggered = False
        self._family_cache: dict[str, str] | None = None

    # ---------------- github state helpers ----------------
    def commit(self, files: dict, msg: str) -> bool:
        try:
            self.gh.commit_files(self.branch, files, msg)
            return True
        except Exception as e:
            print(f"COMMIT-FAILED: {e}", flush=True)
            return False

    def read_json_state(self, path: str):
        try:
            return self.gh.get_file_json(path, self.branch)
        except Exception:
            return None

    def refresh_overlay(self) -> None:
        d = self.read_json_state(OVERLAY_PATH)
        if isinstance(d, dict) and int(d.get("revision", 0)) != self.overlay_rev:
            self.overlay_rev = int(d.get("revision", 0))
            self.overlay_pkgs = list(d.get("packages", []))
            print(f"overlay r{self.overlay_rev}: "
                  f"{len(self.overlay_pkgs)} cascade packages", flush=True)

    def refresh_coverage(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_coverage_read < COVERAGE_REFRESH_S:
            return
        d = self.read_json_state("grid_state/coverage.json")
        if isinstance(d, dict):
            self.coverage = d
        self.last_coverage_read = now

    def refresh_budget_and_stop(self) -> None:
        now = time.monotonic()
        if now - self.last_budget_read < BUDGET_REFRESH_S:
            return
        self.last_budget_read = now
        b = self.read_json_state("global_rate.json")
        if isinstance(b, dict) and "budget_rps" in b:
            self.budget = b
        try:
            if self.gh.get_file("STOP", self.branch) is not None:
                print("STOP file present on state branch; standing down",
                      flush=True)
                self.stop.set()
        except Exception:
            pass

    def cap_rps(self) -> float:
        hint = max(1, int(self.budget.get("active_hint", 20)))
        return min(MAX_RPS, float(self.budget.get("budget_rps", 60000)) / hint)

    # ---------------- shard / claims ----------------
    def shard_path(self) -> str:
        return f"{SHARD_DIR}/{self.worker_key}.json"

    def commit_shard(self, msg: str) -> None:
        body = {
            "worker": self.worker_key, "index": self.args.index,
            "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
            "heartbeat_utc": now_utc(), "updated_utc": now_utc(),
            "current_pid": self.current_pid,
            "probed_total": self.probed_total,
            "rps_ema": round(self.aimd.rps, 1),
            "windows_tail": self.windows[-12:],
            "packages": self.package_statuses,
            "hits": self.hits_all,
        }
        self.commit({self.shard_path(): json.dumps(body, indent=1) + "\n"}, msg)

    def claim(self, pid: str, probed_through: int, status: str) -> bool:
        ok = self.commit({
            f"{CLAIM_DIR}/{pid}.json": json.dumps({
                "pid": pid, "worker": self.worker_key,
                "status": status, "probed_through": probed_through,
                "heartbeat_utc": now_utc(),
            }, indent=1) + "\n",
        }, f"grid claim {pid} [{status}] by {self.worker_key}")
        if not ok:
            return False
        got = self.read_json_state(f"{CLAIM_DIR}/{pid}.json")
        if isinstance(got, dict) and got.get("worker") != self.worker_key:
            return False
        return True

    def claim_stale(self, claim: dict) -> bool:
        ts = str(claim.get("heartbeat_utc", ""))
        try:
            t = time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone
            return time.time() - t > 25 * 60
        except Exception:
            return True

    # ---------------- hit handling ----------------
    async def download_and_verify(self, session, url: str, map_id: str,
                                  ts_ms: int) -> dict:
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
            self.found_dir.mkdir(parents=True, exist_ok=True)
            (self.found_dir / f"{map_id}.{ts_ms}.zip").write_bytes(data)
        return rec

    def family_series(self) -> dict[str, str]:
        if self._family_cache is not None:
            return self._family_cache
        out: dict[str, str] = {}
        for p in self.plan["packages"]:
            if not p["tier"].startswith("h1"):
                continue
            for it in p["items"]:
                out.setdefault(it["map_id"], it["path_fmt"])
        self._family_cache = out
        return out

    def expansion_windows(self, hit_map_id: str, hit_ts: int) -> list[dict]:
        """Sibling + bundle expansion windows around a confirmed hit.

        Same-path-prefix family series: ts +/- 90s (clone packs upload within
        +/-2min - corpus evidence). Other tier-1 map bundles: +/-2.5s (slots
        share timestamps within seconds; component ms repeats +/-15ms)."""
        fam = self.family_series()
        hit_fmt = fam.get(hit_map_id, "")
        out: list[dict] = []
        for mid, fmt in sorted(fam.items()):
            if mid == hit_map_id:
                continue
            if "/maps/" in fmt and "/maps/" in hit_fmt:
                out.append({"map_id": mid, "path_fmt": fmt,
                            "t0": hit_ts - 90_000, "t1": hit_ts + 90_000})
            elif "/plugins/" in fmt and "/plugins/" in hit_fmt:
                out.append({"map_id": mid, "path_fmt": fmt,
                            "t0": hit_ts - 90_000, "t1": hit_ts + 90_000})
            elif "/maps/" in fmt and "/maps/" in hit_fmt and mid.startswith("m"):
                out.append({"map_id": mid, "path_fmt": fmt,
                            "t0": hit_ts - 2_500, "t1": hit_ts + 2_500})
        return out

    def inject_expansion(self, hit_map_id: str, hit_ts: int, tier: str) -> int:
        wins = self.expansion_windows(hit_map_id, hit_ts)
        total = sum(w["t1"] - w["t0"] for w in wins)
        for w in wins:
            w["cur"] = w["t0"]
            w["tier"] = f"expansion-of-{hit_map_id}"
        self.exp_windows.extend(wins)
        print(f"INLINE-EXPANSION armed: {len(wins)} windows, {total:,} probes "
              f"around ts={hit_ts} ({hit_map_id}, tier={tier})", flush=True)
        return total

    def commit_hit_now(self, rec: dict) -> None:
        """Single-flight immediate hit persistence (called via task)."""
        name = f"{rec['map_id']}.{rec['ts_ms']}"
        files = {f"{FOUND_DIR}/{name}.json":
                 json.dumps(rec, indent=1) + "\n"}
        z = self.found_dir / f"{name}.zip"
        if z.exists() and z.stat().st_size <= COMMIT_ZIP_MAX_BYTES:
            files[f"grid_state/found_zips/{name}.zip"] = z.read_bytes()
        self.commit(files, f"GRID HIT: {rec['url']} sha256={rec.get('sha256','')[:12]}")
        self.commit_shard(f"hit {name}")

    # ---------------- package sweep ----------------
    async def sweep_package(self, session, pkg: dict, resume_global: int) -> dict:
        stream = OrdinalStream(pkg)
        blocks = stream.blocks()
        resume_local = max(0, resume_global - pkg["ordinal0"])
        tracker = SecondsTracker(blocks, resume_local)
        statuses: collections.Counter = collections.Counter()
        exp_statuses: collections.Counter = collections.Counter()
        exceptions: list[list] = []
        found: list[dict] = []
        q: asyncio.Queue = asyncio.Queue(maxsize=4200)
        feeder_done = asyncio.Event()
        win_counts: collections.Counter = collections.Counter()
        started = time.monotonic()

        async def probe(url: str, map_id: str, ts: int, ordinal, tier: str):
            """HEAD (+1 inline retry); GET+verify on 200. Cursor-clean only
            for 200/404."""
            pause = self.aimd.pause_until - time.monotonic()
            if pause > 0:
                await asyncio.sleep(pause)
            code, err_kind = None, None
            for attempt in (0, 1):
                if self.stop.is_set():
                    return
                try:
                    t0m = time.monotonic()
                    async with session.head(url) as resp:
                        self.latencies.append((time.monotonic() - t0m) * 1000)
                        code = resp.status
                        if code == 200:
                            self.aimd.record(False)
                            rec = await self.download_and_verify(
                                session, url, map_id, ts)
                            rec["package"] = pkg["pid"] if ordinal is not None \
                                else "expansion"
                            rec["tier"] = tier
                            found.append(rec)
                            self.hits_all.append(rec)
                            if rec.get("zip_ok") and rec.get("zip_crc_ok"):
                                if not self._hit_commit_pending:
                                    self._hit_commit_pending = True
                                    asyncio.create_task(
                                        self._async_commit_hit(rec))
                            if not self._controller_triggered:
                                self._controller_triggered = True
                                self.inject_expansion(map_id, ts, tier)
                                try:
                                    self.gh.dispatch(
                                        "controller.yml", "main",
                                        {"trigger": "grid-hit",
                                         "max_workers": "20"})
                                except Exception:
                                    pass
                            break
                        if code == 404:
                            self.aimd.record(False)
                            break
                        self.aimd.record(True)
                except asyncio.TimeoutError:
                    self.latencies.append((time.monotonic() - t0m) * 1000)
                    self.aimd.record(True)
                    err_kind = "timeout"
                    code = None
                except (aiohttp.ClientError, OSError):
                    self.aimd.record(True)
                    err_kind = "neterr"
                    code = None
                if attempt == 0 and not self.stop.is_set():
                    await asyncio.sleep(1.5)
            if code == 200:
                st = ST200
            elif code == 404:
                st = ST404
            elif code is None:
                st = STTIMEOUT if err_kind == "timeout" else STNETERR
            else:
                st = ST503 if code == 503 else f"OTHER_{code}"
            if ordinal is not None:
                statuses[st] += 1
                if st not in (ST200, ST404) and len(exceptions) < EXCEPTIONS_CAP:
                    exceptions.append([ordinal, st])
                tracker.complete(ordinal - pkg["ordinal0"], st in (ST200, ST404))
            else:
                exp_statuses[st] += 1
            win_counts[st] += 1
            win_counts["_all"] += 1

        async def consume() -> None:
            while not (feeder_done.is_set() and q.empty()):
                if self.stop.is_set() and q.empty():
                    return
                try:
                    datum = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                kind, map_id, fmt, x, tier = datum
                try:
                    if kind == "pkg":
                        url, mid, ts = stream.at(x)
                        await probe(url, mid, ts, x, tier)
                    else:
                        url = fmt.format(map_id=map_id, ts_ms=x)
                        await probe(url, map_id, x, None, tier)
                except Exception as e:  # never lose the consumer slot
                    print(f"probe error: {e!r}", flush=True)

        async def feed() -> None:
            try:
                ordinal = pkg["ordinal0"] + resume_local
                nxt = time.monotonic()
                while not self.stop.is_set():
                    if self.exp_windows:
                        w = self.exp_windows[0]
                        if w["cur"] >= w["t1"]:
                            self.exp_windows.popleft()
                            continue
                        datum = ("exp", w["map_id"], w["path_fmt"], w["cur"],
                                 w["tier"])
                        w["cur"] += 1
                    elif ordinal < stream.end:
                        datum = ("pkg", None, None, ordinal, pkg["tier"])
                        ordinal += 1
                    else:
                        return
                    wait = nxt - time.monotonic()
                    if wait > 0:
                        await asyncio.sleep(wait)
                    while q.full() and not self.stop.is_set():
                        await asyncio.sleep(0.01)
                    q.put_nowait(datum)
                    nxt = max(nxt, time.monotonic()) + \
                        1.0 / max(self.aimd.rps, 1.0)
            finally:
                feeder_done.set()

        async def supervise() -> None:
            """Window accounting + adaptive pool + heartbeat + deadline."""
            last_window = time.monotonic()
            consumers: list[asyncio.Task] = []
            while not (feeder_done.is_set() and q.empty() and
                       all(c.done() for c in consumers)):
                await asyncio.sleep(1.0)
                now = time.monotonic()
                if now > self.deadline:
                    print("run deadline reached; draining", flush=True)
                    self.stop.set()
                if now - last_window >= WINDOW_S:
                    dur = now - last_window
                    n = max(1, win_counts["_all"])
                    bad = (win_counts[ST503] +
                           sum(v for k, v in win_counts.items()
                               if k.startswith("OTHER"))) / n
                    err = (win_counts[STTIMEOUT] + win_counts[STNETERR]) / n
                    self.aimd.window_tick(bad, err, self.cap_rps())
                    lat = sorted(self.latencies)
                    p50 = (lat[len(lat) // 2] / 1000.0) if lat else 0.12
                    self.target_conc = int(min(CONC_MAX, max(
                        CONC_MIN, self.aimd.rps * max(p50 * 1.35, 0.10))))
                    self.windows.append([
                        round(dur, 1), round(win_counts["_all"] / dur, 2),
                        win_counts[ST200], win_counts[ST404], win_counts[ST503],
                        win_counts[STTIMEOUT], win_counts[STNETERR]])
                    win_counts.clear()
                    last_window = now
                consumers = [c for c in consumers if not c.done()]
                while len(consumers) < self.target_conc and \
                        not feeder_done.is_set() and not self.stop.is_set():
                    consumers.append(asyncio.create_task(consume()))
                if now - self.last_heartbeat >= HEARTBEAT_S:
                    self.last_heartbeat = now
                    cur = pkg["ordinal0"] + tracker.probed_through_local()
                    self.commit_shard(f"heartbeat {pkg['pid']} "
                                      f"cursor={cur:,} rps={self.aimd.rps:.0f}")

        feeder = asyncio.create_task(feed())
        supervisor = asyncio.create_task(supervise())
        await feeder
        # supervisor returns only when feeder is done, the queue is drained
        # and every consumer has exited (feeder_done + q.empty gate)
        await supervisor
        probed = sum(statuses.values())
        probed_through = pkg["ordinal0"] + tracker.probed_through_local()
        dur = time.monotonic() - started
        rec = {
            "status": "done" if probed_through >= stream.end else "partial",
            "candidates": pkg["candidates"],
            "probed_unique": probed,
            "probed_through": probed_through,
            "statuses": dict(statuses),
            "expansion_statuses": dict(exp_statuses),
            "found": found,
            "exceptions_n": len(exceptions),
            "rate_rps": round(probed / max(dur, 1e-6), 1),
        }
        if exceptions:
            rec["exceptions_sample"] = exceptions[:2000]
        self.probed_total += probed
        return rec

    async def _async_commit_hit(self, rec: dict) -> None:
        try:
            await asyncio.to_thread(self.commit_hit_now, rec)
        except Exception as e:
            print(f"hit commit error: {e!r}", flush=True)
        finally:
            self._hit_commit_pending = False

    # ---------------- main loop ----------------
    def ordered_entries(self) -> list[dict]:
        out = [{"pkg": p, "overlay": True} for p in self.overlay_pkgs]
        out += [{"pkg": p, "overlay": False} for p in self.plan["packages"]]
        return out

    def is_done(self, pid: str) -> bool:
        return pid in (self.coverage.get("done") or [])

    def partial_through(self, pid: str) -> int:
        return int((self.coverage.get("partial") or {}).get(pid, 0))

    async def run(self) -> int:
        started = time.monotonic()
        self.refresh_coverage(force=True)
        self.refresh_overlay()
        self.commit_shard("worker start")

        conn = aiohttp.TCPConnector(limit=2400, limit_per_host=2400,
                                    ttl_dns_cache=600, keepalive_timeout=55,
                                    enable_cleanup_closed=False)
        timeout = aiohttp.ClientTimeout(total=HEAD_TIMEOUT_S, connect=8)
        claimed_this_run = 0
        async with aiohttp.ClientSession(
                connector=conn, timeout=timeout,
                headers={"User-Agent": UA}, trust_env=False) as session:
            signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
            signal.signal(signal.SIGINT, lambda *_: self.stop.set())

            for entry in self.ordered_entries():
                if self.stop.is_set() or time.monotonic() > self.deadline:
                    break
                pkg = entry["pkg"]
                pid = pkg["pid"]
                if self.is_done(pid):
                    continue
                self.refresh_coverage()
                self.refresh_budget_and_stop()
                if self.stop.is_set() or self.is_done(pid):
                    continue
                existing = self.read_json_state(f"{CLAIM_DIR}/{pid}.json")
                resume = self.partial_through(pid)
                if isinstance(existing, dict) and \
                        existing.get("worker") != self.worker_key and \
                        not self.claim_stale(existing):
                    continue
                if not self.claim(pid, resume,
                                  "resuming" if resume else "claimed"):
                    continue
                self.current_pid = pid
                self.commit_shard(f"claimed {pid}")
                claimed_this_run += 1
                print(f"SCAN {pid} tier={pkg['tier']} "
                      f"candidates={pkg['candidates']:,} resume={resume:,}",
                      flush=True)
                rec = await self.sweep_package(session, pkg, resume)
                self.package_statuses[pid] = rec
                if any(h.get("zip_ok") and h.get("zip_crc_ok")
                       for h in rec["found"]):
                    print("*** VERIFIED HITS THIS PACKAGE: "
                          f"{[h['url'] for h in rec['found'] if h.get('zip_ok')]}",
                          flush=True)
                self.commit_shard(
                    f"result {pid}: {rec['status']} "
                    f"probed={rec['probed_unique']:,} "
                    f"statuses={rec['statuses']}")
                self.current_pid = ""
                if claimed_this_run % 3 == 0:
                    self.refresh_overlay()

            self.commit_shard("worker exit")

        dur = time.monotonic() - started
        print(f"WORKER-DONE {self.worker_key} claimed={claimed_this_run} "
              f"probed={self.probed_total:,} dur={dur/60:.1f}min "
              f"hits={len(self.hits_all)}", flush=True)
        try:
            self.gh.dispatch("controller.yml", "main",
                             {"trigger": "worker-done", "max_workers": "20"})
        except Exception:
            pass
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default="scan/grid_plan.json")
    ap.add_argument("--index", type=int, default=0)
    ap.add_argument("--minutes", type=int, default=285)
    ap.add_argument("--state-branch", default="scan-state")
    ap.add_argument("--out-dir", default="worker_out")
    ap.add_argument("--max-probes", type=int, default=0,
                    help="smoke-test cap (probes on the first package)")
    args = ap.parse_args()
    if args.max_probes:
        w = Worker(args)
        pkg = dict(w.plan["packages"][0])
        pkg["items"] = [dict(pkg["items"][0])]
        pkg["items"][0]["t1"] = pkg["items"][0]["t0"] + args.max_probes
        pkg["candidates"] = args.max_probes

        async def smoke() -> int:
            conn = aiohttp.TCPConnector(limit=256)
            timeout = aiohttp.ClientTimeout(total=HEAD_TIMEOUT_S, connect=8)
            async with aiohttp.ClientSession(
                    connector=conn, timeout=timeout,
                    headers={"User-Agent": UA}, trust_env=False) as session:
                rec = await w.sweep_package(session, pkg, 0)
            print("SMOKE-RESULT:", json.dumps(
                {k: v for k, v in rec.items() if k != "exceptions_sample"},
                indent=1))
            return 0
        return asyncio.run(smoke())
    return asyncio.run(Worker(args).run())


if __name__ == "__main__":
    raise SystemExit(main())
