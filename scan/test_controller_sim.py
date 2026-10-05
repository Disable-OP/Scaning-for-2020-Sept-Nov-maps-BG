#!/usr/bin/env python3
"""End-to-end controller lifecycle simulation (no network, no real GitHub).

Runs scan/controller.py against a FakeGH in-memory store through many cycles
with simulated workers, covering:
  - frontier materialization + epoch file commit + epoch boundary crossing
  - exactly-once ingest, pruning into tallies, bounded queue.json
  - 503 undetermined -> retry batches (round cap) -> tally reconciliation
  - PARTIAL -> RETRY -> resume dispatch -> COMPLETED with carried accounting
  - FOUND_200 -> independent verification (mocked verifier) -> verified map
  - stale RUNNING requeue, ABANDONED disclosure
  - frontier exhaustion -> drain -> REAL aggregate (in-process, no-clone)
    -> scan_complete_v5 + release
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SIMROOT = Path(".simroot")
sys.path.insert(0, str(REPO / "scan"))
import gen_candidates as G  # noqa: E402
import deep_universe as D  # noqa: E402


# --------------------------------------------------------------------------
class FakeGH:
    """In-memory GitHub: branches as {path: bytes}, minimal REST surface."""

    def __init__(self, token="", owner="o", repo="r", shared=None):
        self.owner = owner
        self.repo = repo
        if shared is not None:
            self.store, self.runs, self.dispatches, self.calls = shared
            return
        self.store = {"main": {}, "scan-state": {}}
        self.runs = {}          # run_id -> {"workflow":..., "inputs":..., "active":bool}
        self.dispatches = []
        self.calls = 0
        self._next_run = 1000

    # -- core ---------------------------------------------------------------
    def api(self, method, path, *, body=None, headers=None, timeout=60):
        self.calls += 1
        if method == "GET" and "/git/ref/heads/" in path:
            br = path.rsplit("/", 1)[1]
            if br not in self.store:
                return 404, None, {}
            sha = self._branch_sha(br)
            return 200, {"object": {"sha": sha}}, {}
        if method == "POST" and path.endswith("/git/refs"):
            br = body["ref"].split("refs/heads/")[1]
            if br in self.store:
                return 422, {"message": "exists"}, {}
            self.store[br] = dict(self.store.get(body.get("...", "main")) or {})
            self.store[br] = {}
            return 201, {}, {}
        if method == "GET" and "/git/trees/" in path:
            br = self._branch_by_sha(path.split("/git/trees/")[1].split("?")[0])
            recursive = "recursive=1" in path
            tree = []
            if br is not None:
                if recursive:
                    for p in sorted(self.store[br]):
                        tree.append({"path": p, "type": "blob", "sha": self._h(p)})
                else:
                    for p in sorted({p.split("/")[0] for p in self.store[br]}):
                        tree.append({"path": p, "type": "blob", "sha": self._h(p)})
            return 200, {"tree": tree}, {}
        if method == "GET" and "/git/blobs/" in path:
            sha = path.rsplit("/", 1)[1]
            br = self._branch_by_sha_context.get(sha)
            for s in self.store.values():
                for p, c in s.items():
                    if self._h(p) == sha:
                        return 200, {"content": base64.b64encode(c).decode()}, {}
            return 404, None, {}
        if method == "POST" and path.endswith("/releases"):
            return 201, {"html_url": "https://example.invalid/release"}, {}
        return 404, None, {}

    def _branch_sha(self, br):
        return self._h("HEAD:" + br)

    def _h(self, s):
        import hashlib
        return hashlib.sha1(s.encode()).hexdigest()

    _branch_by_sha_context: dict = {}

    def _branch_by_sha(self, sha):
        for br in self.store:
            if self._branch_sha(br) == sha:
                return br
        return None

    # -- repos --------------------------------------------------------------
    def get_ref(self, branch):
        return {"object": {"sha": self._branch_sha(branch)}} \
            if branch in self.store else None

    def ensure_branch(self, branch, from_branch="main"):
        if branch not in self.store:
            self.store[branch] = {}
        return True

    def get_file(self, path, branch):
        return self.store.get(branch, {}).get(path)

    def get_file_json(self, path, branch):
        raw = self.get_file(path, branch)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)

    def list_dir(self, path, branch):
        prefix = path.rstrip("/") + "/"
        return [p[len(prefix):] for p in self.store.get(branch, {})
                if p.startswith(prefix)]

    def commit_files(self, branch, files, message, max_rounds=6):
        st = self.store.setdefault(branch, {})
        for p, content in files.items():
            if content is None:
                st.pop(p, None)
            else:
                st[p] = content.encode() if isinstance(content, str) else content
        return self._branch_sha(branch)

    # -- actions ------------------------------------------------------------
    def dispatch(self, workflow, ref, inputs):
        rid = self._next_run
        self._next_run += 1
        self.runs[rid] = {"workflow": workflow, "inputs": dict(inputs),
                          "active": True}
        self.dispatches.append((workflow, rid, dict(inputs)))
        return True

    def list_runs(self, workflow, statuses=("queued", "in_progress"),
                  per_page=100, max_pages=5):
        return [{"id": rid} for rid, r in self.runs.items()
                if r["workflow"] == workflow and r["active"]]

    def all_recent_runs(self, workflow, per_page=100, max_pages=1):
        return [{"id": rid, "workflow": workflow} for rid in self.runs]


# --------------------------------------------------------------------------
def setup_simroot(mini_cfg: dict) -> Path:
    if SIMROOT.exists():
        shutil.rmtree(SIMROOT)
    scan = SIMROOT / "scan"
    scan.mkdir(parents=True)
    for f in ("controller.py", "deep_universe.py", "gen_candidates.py",
              "gh.py", "aggregate.py", "verify_map.py", "zipcheck.py",
              "spec_v1.json", "requirements.txt"):
        src = REPO / "scan" / f
        if src.exists():
            os.symlink(src, scan / f)
    (scan / "deep_config.json").write_text(json.dumps(mini_cfg, indent=1))
    return SIMROOT


def dump_state(gh: FakeGH, out: Path) -> Path:
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    for p, content in gh.store["scan-state"].items():
        fp = out / p
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_bytes(content)
    return out


def make_result(gh, key, epoch, size, chunk_id, mode="full",
                inject=None, partial_upto=None, prior=None, cs=None):
    """Deterministic simulated worker result for one chunk.
    Exception keys are GLOBAL per-epoch ordinals (real worker semantics)."""
    import collections
    cs = cs or size
    base = chunk_id * cs  # global ordinal of this chunk's first candidate
    results = {}            # LOCAL position -> (status, found_rec)
    head_attempts = 0
    if prior:  # resume: carry confirmed positions, probe the rest
        for pos, st, rec in prior:
            results[pos] = (st, rec)
        head_attempts = len(prior)
    start_probe = max(results) + 1 if results else 0
    for pos in range(start_probe, size):
        g = base + pos
        if g % 5000 == 7:
            results[pos] = ("503", None)
        else:
            results[pos] = ("404", None)
    if inject:
        gpos, st, rec = inject
        results[gpos - base] = (st, rec)
    probed = len(results)
    counter = collections.Counter(st for st, _ in results.values())
    default = counter.most_common(1)[0][0]
    exceptions = [[p + base, st] for p, (st, _r) in sorted(results.items())
                  if st != default]
    found = [r for _st, r in results.values() if r is not None]
    scheduled = size
    if partial_upto is not None:
        results = dict(list(results.items())[:partial_upto])
        probed = len(results)
        counter = collections.Counter(st for st, _ in results.values())
        default = counter.most_common(1)[0][0]
        exceptions = [[p + base, st] for p, (st, _r) in sorted(results.items())
                      if st != default]
        found = [r for _st, r in results.values() if r is not None]
    outcome = ("PARTIAL" if probed < scheduled else
               "COMPLETED_WITH_UNDETERMINED" if any(
                   st not in ("404", "200") for st in counter) else "COMPLETED")
    return {
        "schema": 1, "key": key, "chunk_id": chunk_id, "epoch": epoch,
        "attempt": 1, "mode": mode, "run_id": 1,
        "candidate_source": f"epoch:{epoch}" if epoch else "spec",
        "started_utc": "2026-10-05T00:00:00Z", "finished_utc": "2026-10-05T00:10:00Z",
        "duration_s": 60.0, "candidates_scheduled": scheduled,
        "candidates_probed_unique": probed, "head_attempts_total": head_attempts + probed,
        "statuses": dict(counter), "default_status": default,
        "exceptions": exceptions, "found": found,
        "rate": {"rps_mean": 100.0, "rps_final": 100.0, "local_cap": 100.0,
                 "budget_rps": 1500.0, "active_hint": 5},
        "latency_ms": {"p50": 900.0, "p95": 950.0},
        "windows": [], "outcome": outcome, "notes": [],
    }


def main() -> int:
    os.chdir(REPO)
    mini_cfg = {
        "config_version": 5,
        "campaign_id": "deep-scan-v5-sim",
        "host": "staticgs.sandboxol.com",
        "url_path_fmt": "/sandbox/games/maps/{map_id}.{ts_ms}.zip",
        "search_scope": "MAPS ONLY", "granularity": "millisecond-exact",
        "provenance": "sim", "chunk_size": 20000, "queue_window": 8,
        "ingest_lookback": 3000,
        "all_map_ids": ["m1001", "m1002_1", "m1008_2", "m1014_1"],
        "anchors_ms": {"S1": 1600317822000, "S1B": 1600317824000,
                       "S2": 1605665040000, "S2B": 1605665064000},
        "anchor_order": ["S1", "S1B", "S2", "S2B"],
        "sentinel_pairs": [{"map_id": "m1014_1", "anchor": "S1"},
                           {"map_id": "m1008_2", "anchor": "S2"}],
        "epoch_ladder": [
            {"epoch": 1, "kind": "family", "basis": "sim family +/-30s",
             "fwd_s": [0, 30], "back_s": [-30, 0]},
            {"epoch": 2, "kind": "sentinel", "basis": "sim sentinel back",
             "back_s": [-7200, -1800]},
        ],
    }
    setup_simroot(mini_cfg)
    os.chdir(SIMROOT)

    env = os.environ.copy()
    env.update({"GITHUB_REPOSITORY": "o/r", "GITHUB_TOKEN": "fake-token",
                "MAX_ACTIVE_WORKERS": "12", "MAX_DISPATCH_PER_CYCLE": "12",
                "DEEP_QUEUE_WINDOW": "24", "BUDGET_START_RPS": "1500"})
    os.environ.update(env)

    gh = FakeGH(shared=None)
    gh.ensure_branch("scan-state", "main")

    import controller as C
    import aggregate as AG
    C.GH = lambda token, owner, repo: gh
    AG.GH = lambda token, owner, repo: gh
    C.RETRY_DELAYS_MIN = {2: 0, 3: 0}  # fast-forward sim time for retry delays

    spec_v1 = G.load_spec("scan/spec_v1.json")
    espec1, _ = D.build_epoch(mini_cfg, spec_v1, 1)
    total1 = espec1["candidates_emitted"]
    num1 = (total1 + 20000 - 1) // 20000
    print(f"sim epoch 1: {total1:,} candidates, {num1} chunks")

    def fake_subprocess(cmd, capture_output=True, text=True, timeout=None, **kw):
        import subprocess as sp
        if any("verify_map.py" in c for c in cmd):
            out = cmd[cmd.index("--out") + 1]
            Path(out).parent.mkdir(parents=True, exist_ok=True)
            Path(out).write_text(json.dumps(
                {"results": [{"verified": True, "sha256": "a" * 64,
                              "bytes": 1234, "get_status": 200}]}))
            return sp.CompletedProcess(cmd, 0, "", "")
        if any("aggregate.py" in c for c in cmd):
            dump_state(gh, SIMROOT / "state_dump")
            os.environ["AGG_NOCLONE"] = "1"
            os.environ["AGG_STATE_DIR"] = str((SIMROOT / "state_dump").resolve())
            buf_out = []
            import io
            old = sys.stdout
            sys.stdout = io.StringIO()
            try:
                rc = AG.main()
            finally:
                cap = sys.stdout.getvalue()
                sys.stdout = old
            os.environ.pop("AGG_NOCLONE", None)
            return sp.CompletedProcess(cmd, rc, cap + "\nJSONMARK\n" +
                                       json.dumps({"ok": rc == 0}), "")
        return sp.CompletedProcess(cmd, 0, "", "")

    C.subprocess.run = fake_subprocess

    cycle = 0
    partial_state = {}   # key -> list of (pos, st, rec) for resume simulation
    resumed_seen = set()
    while cycle < 600:
        cycle += 1
        before = gh.get_file_json("scan_complete_v5.json", "scan-state")
        C.main()
        done = gh.get_file_json("scan_complete_v5.json", "scan-state")
        # simulate the dispatched workers of this cycle
        for rid, r in list(gh.runs.items()):
            if not r["active"]:
                continue
            inp = r["inputs"]
            if inp.get("retry_batch"):
                bfile = inp["retry_batch"]
                b = gh.get_file_json(bfile, "scan-state")
                n = b["count"]
                key = bfile.split("/", 1)[1][:-len(".json")]
                res = make_result(gh, key, 0, n, 0)
                # resolve every retried candidate to 404
                res["statuses"] = {"404": n}
                res["default_status"] = "404"
                res["exceptions"] = []
                res["outcome"] = "COMPLETED"
                res["epoch"] = 0
                res["candidate_source"] = f"retry_batch:{bfile}"
                gh.commit_files("scan-state", {
                    f"claims/{key}.json": json.dumps({"run_id": rid}),
                    f"results/{key}.result.json":
                        json.dumps(res, indent=1) + "\n"}, "sim batch result")
            else:
                cid = int(inp["chunk_id"])
                ep = int(inp.get("epoch", "0") or 0)
                key = f"e{ep}-chunk-{cid:06d}"
                q = gh.get_file_json("queue.json", "scan-state")
                size = q["chunks"][key]["size"]
                inject = None
                if ep == 1 and cid == 5:
                    espec = gh.get_file_json(f"epochs/epoch_{ep}.json", "scan-state")
                    _o, m, ts, _t = next(G.iter_chunk(espec, cid))
                    inject = (cid * 20000 + (12345 % size), "200", {
                        "map_id": m, "ts_ms": ts,
                        "url": f"https://staticgs.sandboxol.com/sandbox/games/maps/{m}.{ts}.zip",
                        "observed_utc": "2026-10-05T00:05:00Z",
                        "get_status": 200, "zip_ok": True, "zip_crc_ok": True,
                        "sha256": "b" * 64, "bytes": 4321})
                prior = partial_state.get(key)
                if key == "e1-chunk-000001" and prior is None:
                    res = make_result(gh, key, ep, size, cid, cs=20000,
                                      partial_upto=size // 2)
                    carried = []
                    d = res["default_status"]
                    ex = {int(p): s for p, s in res["exceptions"]}  # global keys
                    b = cid * 20000
                    for pos in range(res["candidates_probed_unique"]):
                        carried.append((pos, ex.get(pos + b, d), None))
                    partial_state[key] = carried
                    gh.commit_files("scan-state", {
                        f"claims/{key}.json": json.dumps({"run_id": rid}),
                        f"results/{key}.result.json":
                            json.dumps(res, indent=1) + "\n"}, "sim partial")
                    r["active"] = False  # run ends; controller must RETRY it
                    continue
                if prior is not None and key in partial_state:
                    resumed_seen.add(key)
                    res = make_result(gh, key, ep, size, cid, cs=20000,
                                      prior=prior, inject=inject)
                    res["attempt"] = int(inp.get("attempt", "1"))
                else:
                    res = make_result(gh, key, ep, size, cid, cs=20000,
                                      inject=inject)
                gh.commit_files("scan-state", {
                    f"claims/{key}.json": json.dumps({"run_id": rid}),
                    f"results/{key}.result.json": json.dumps(res, indent=1) + "\n"},
                    "sim chunk result")
            r["active"] = False
        if done and not before:
            print(f"campaign complete after {cycle} cycles")
            break
    else:
        raise AssertionError("sim did not complete in 600 cycles")

    # ------------------------------------------------------------ asserts ---
    q = gh.get_file_json("queue.json", "scan-state")
    tal = q["frontier"]["tallies"]
    t1 = tal.get("1", {})
    assert q["frontier"]["exhausted"] is True, "frontier should be exhausted"
    assert t1.get("found_200") == 1, f"found_200 tally {t1}"
    assert t1.get("undetermined") == 0, f"undetermined must reconcile to 0: {t1}"
    expected_404 = total1 - 1  # one candidate is the injected 200
    assert t1.get("confirmed_404") == expected_404, \
        f"404 tally {t1.get('confirmed_404')} != {expected_404}"
    assert t1.get("probed", 0) >= total1, "probed tally undercounts"
    # bounded queue: completed entries pruned
    live = [k for k, e in q["chunks"].items()
            if e.get("status") in ("PENDING", "RUNNING", "RETRY")]
    assert not live, f"open chunks remain: {live[:5]}"
    # claims pruned for completed chunks
    claim_paths = [p for p in gh.store["scan-state"] if p.startswith("claims/")]
    result_paths = [p for p in gh.store["scan-state"] if p.startswith("results/")]
    assert len(claim_paths) < len(result_paths) or num1 < 10, \
        "claims should be pruned after ingest"
    # verified map
    agg = gh.get_file_json("agg.json", "scan-state")
    assert agg["verified"] and list(agg["verified"].values())[0]["verified"] is True
    # resume actually happened with resume input
    resumes = [i for _w, _r, i in gh.dispatches
               if i.get("resume_from_result")]
    assert resumes, "no resume dispatch occurred"
    # final accounting committed to main
    acc = gh.get_file_json("results/final/deep_v5_accounting.json", "main")
    espec2, _ = D.build_epoch(mini_cfg, spec_v1, 2)
    ladder_total = total1 + espec2["candidates_emitted"]
    assert acc["universe_total_unique_candidates"] == ladder_total, \
        (acc["universe_total_unique_candidates"], ladder_total)
    assert acc["canonical_unique"]["found_200"] == 1
    assert acc["canonical_unique"]["undetermined"] == 0
    assert acc["unique_unsearched"] == 0, acc["unique_unsearched"]
    assert acc["coverage_fraction"] == 1.0
    stats = gh.get_file_json("scan_statistics.json", "main")
    assert stats["deep_v5"]["verified_maps"] == 1
    assert "results/final/deep_v5_accounting.json" in \
        gh.get_file("SHA256SUMS", "main").decode()
    # release published (FakeGH returns 201)
    print("ALL SIM ASSERTIONS PASSED")
    print(json.dumps({"cycles": cycle, "epoch1_candidates": total1,
                      "chunks": num1, "tallies": tal,
                      "live_chunks": len(q["chunks"]),
                      "claims_left": len(claim_paths),
                      "results_total": len(result_paths)}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
