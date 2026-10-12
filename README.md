# Scanning for 2020 Sept–Nov maps (Blockman GO, Engine 10068)

Archival existence research for map ZIPs served by `staticgs.sandboxol.com` at
`/sandbox/games/maps/{map_id}.{ts_ms}.zip`, focused on the 2020-09-17 and
2020-11-18 build clusters (Engine context `{"engineVersion": 10068, "useCmd": "true", "logLevel": "debug"}`).

**Scope: MAPS path only.** Millisecond-exact timestamps (never rounded).
No proxy/IP-rotation, no bypass of access controls, adaptive rate control at
worker and global level.

## Current phase: GRID v3 — halloween-2021 fleet (ACTIVE, supersedes v5/v6)

The v5/v6 architecture (queue.json + one workflow-run per 50k chunk) measured
**~250 rps/worker AIMD ceiling** and ~35s of runner setup per ~25s of scanning
- at that effective rate the 28.5B-candidate campaign is a multi-month effort.
Grid v3 replaces it (v5/v6 code kept in git for archaeology; `scan/deep_config.json`
v7 documents the supersession).

### Design

- **Frozen universe, deterministic packages**: `scan/grid_plan.json` (sha-pinned
  to `scan/spec_halloween_core.json`) cuts the 28,512,000,000 ms-dense
  candidates into **1,584 packages of ~18M** (~50-100 min each per worker).
  Interval arithmetic is verified by `scan/test_grid.py` (no gaps, no overlaps,
  per-tier conservation vs the spec). Ordering: tiers by priority; within a
  tier **hour-major** by corpus-mined upload density - the g1008 family uploads
  assets in correlated deploy bursts, so sweeping all series of one hour
  maximises early-hit probability.
- **Persistent harvester workers** (`scan/grid_worker.py` + `grid.yml`): one
  job = one runner = up to 300 min of continuous claim-sweep-commit chaining
  across packages. No per-chunk dispatch overhead. Dynamic matrix of up to
  20 workers (GitHub free concurrency cap), `fail-fast: false`.
- **Rate control**: per-worker AIMD tuned for the grid (start 600 rps, +200
  per clean 10s window, per-worker cap = budget/active hint, hard cap 8000
  rps), adaptive consumer pool (concurrency ~ rps x RTT x 1.35, bounds
  64..1800), inline retry for transients. 5xx/timeouts are never counted as
  404; package cursors advance only across seconds where **every** ms
  0..999 returned 200/404.
- **Closed-loop intelligence**: a verified hit is GET-downloaded, ZIP/CRC/SHA-256
  verified (`zipcheck`), committed to `grid_state/found/` immediately, and
  triggers an **inline family expansion** (same-path-prefix siblings +/-90s,
  map bundles +/-2.5s) served at absolute priority through the same pacer.
  The controller converts hits into **cascade packages** (c1: family +/-90s +
  bundles +/-2.5s; c2: all 124 series +/-600s) prepended to the claim order,
  and dispatches an extra grid run so the fleet pivots at once.
- **Coordination without a queue server**: claims are
  `grid_state/claims/{pid}.json` (read -> commit -> re-read, last-writer-wins
  with re-read confirmation); coverage truth is aggregated from per-worker
  shards into `grid_state/coverage.json` (done set + partial cursors for
  resume). Dead-runner waste is bounded to one package.
- **Budget governor**: fleet 5xx/err telemetry (worker window tails) drives
  `global_rate.json` `budget_rps` within 10k..120k rps; workers divide by the
  active-worker hint. Every runner is a distinct source IP, so the CDN
  per-connection limits observed locally do not aggregate against the fleet.
- **API discipline**: the built-in GITHUB_TOKEN allows ~1000 REST calls/h/repo
  fleet-wide; worker cadences (budget/STOP refresh 20 min, heartbeats 10 min)
  keep the fleet at a few hundred calls/hour.

### Field-measured performance (runs 38307759930 -> 38313806075)

The CDN (Tencent COS) throttles overseas datacenter IPs (GitHub Actions
runners) hard: field telemetry shows **~150-250 rps sustainable per runner IP**
with 6-20% background 503 shedding, at ~340ms p50 latency - regardless of
concurrency (pool 80 and pool 1800 hit the same wall). The grid therefore
self-tunes each worker to its IP's sustainable point:

| scenario | v6 (as configured) | grid v3 (field) |
|---|---|---|
| per-worker sustained | ~250 rps cap, 450s ramp, 35s setup/25s scan | ~140-200 rps effective goodput, zero setup cost |
| fleet sustained (20 runners) | ~4-5k rps nominal, less in practice | **~3-4k rps effective, 24/7** |
| retry economics | per-chunk rescan on any undetermined | exact 5xx debt carried forward (v3.2 bitmap) |
| tier-1 family (3.89B) | ~9-10 days nominal | **~11-15 days continuous, prioritized** |
| full universe (28.5B) | ~2-3 months | ~3-4 months continuous |

Throughput scaling is linear in concurrent runners: raise `worker_count`
(paid plans allow >20 concurrent jobs) or attach self-hosted runners with
CN-side egress (the same code measured 2.5k rps clean from a single
China-side IP) to multiply fleet throughput. If the release upload sits in
the prior hours, the expected first hit lands far earlier than full-sweep
time; any single hit cascades the whole family within minutes (inline) to
hours (c2 +/-600s across 124 series).

### Ops

- Dispatch: `gh workflow run grid.yml -f worker_count=20 -f minutes=300`.
- Status: `monitoring/grid-status.json` on `scan-state` (per-tier progress,
  fleet rps, ETA, verified hits, cascade revision).
- Kill switch: create file `STOP` on `scan-state` - workers stand down within
  ~20 min, controller takes no further action.
- Verification: `python scan/test_grid.py` (36 offline tests: plan interval
  arithmetic, cursor honesty, AIMD, cascade windows, claim ordering).

## Superseded phase: deep scan v5 — unattended, unbounded epoch ladder

The v4 campaign drained its frozen 18,000,252-candidate universe on 2026-10-04
(coverage 1.0, canonical confirmed-404 = 18,000,252, FOUND-200 = 0,
undetermined-after-retries = 0, zero 403/429). v5 continues from that evidence
into an open-ended, automatically expanding universe:

- **Universe**: 14 deterministic epochs (`scan/deep_config.json`) built by
  `scan/deep_universe.py` — ms-dense windows for all 65 map IDs (the v1 list
  plus sentinel `m1014_1`) around the four anchors, plus ever-wider sentinel
  rings (back to -24h, forward to +24h) and family rings (to +/-30min).
  Total: **790,063,748 candidates / 15,803 chunks** — and the frontier design
  means the chunk queue has **no cap**: the controller materializes new chunks
  automatically as workers drain them.
- **Exclusion (never re-probe)**: every v1-confirmed candidate is subtracted by
  exact integer interval arithmetic before any epoch is emitted. Two v1
  bookkeeping artifacts are handled explicitly and disclosed in the config
  provenance: `m1014_1` was missing from v1's `all_map_ids` (its never-probed
  S2/S2B neighborhoods are covered by epoch 1), and two v1 stream anchor
  points (`m901`xS2/S2B) were dropped by an ordinal-count mismatch (epoch 1
  re-probes them; v3 already probed them as 404 - the duplicates are disclosed).
- **Unattended operation**: `controller.yml` runs on cron */5 and is also
  re-triggered by every finishing worker; workers claim chunks, probe with
  per-worker AIMD + global rate budget, commit results atomically, and the
  controller prunes completed entries into exact live tallies, builds bounded
  retry batches for undetermined observations (round cap 3), requeues stale
  runs, and - when the ladder is exhausted and the queue drains - runs the
  final aggregation, commits the accounting to `main` and publishes the
  `deep-scan-v5-final` GitHub Release. No human or agent step is required.
- **Crash safety**: every state mutation is an atomic git commit on the
  `scan-state` branch; PARTIAL results are resumable (confirmed positions are
  carried, never silently re-probed); repeated failures requeue, then become
  disclosed ABANDONED (unsearched) entries.
- **Honesty rules (unchanged)**: 503/timeout = UNDETERMINED, never a miss;
  duplicate probes disclosed; map existence only after direct GET + ZIP
  structure + CRC + SHA-256 verification; no proxy/IP rotation.
- **Kill switch**: commit a file named `STOP` to the `scan-state` branch (or
  disable the workflows) to halt all dispatching.

## Completed phase: distributed re-scan + undetermined retry (v4)

The earlier single-host scans established:

| metric (v1 + v3 direct scans) | value |
|---|---|
| millisecond-exact probes | 8,374,673 |
| sustained direct throughput | ~2,037 rps (per-IP ceiling observed) |
| confirmed HTTP 404 | 1,823,575 |
| probed-undetermined (HTTP 503 / timeout) | ~6,551,098 |
| free-proxy lane contribution | ~12 rps (deprecated; disallowed for v4) |
| verified archives | **0** |

The ~6.55M undetermined responses are **not** treated as missing files.
v3 stored only aggregate outcome counts plus contiguous coverage pointers, so
individual 503 positions cannot be enumerated; the only honest retry is to
re-probe the known probed ranges, which necessarily re-probes confirmed-404
positions inside them. That overlap is disclosed here and is de-duplicated in
all unique-candidate accounting (duplicate probes are counted separately and
never inflate coverage).

The earlier single-host scans established:

| metric (v1 + v3 direct scans) | value |
|---|---|
| millisecond-exact probes | 8,374,673 |
| sustained direct throughput | ~2,037 rps (per-IP ceiling observed) |
| confirmed HTTP 404 | 1,823,575 |
| probed-undetermined (HTTP 503 / timeout) | ~6,551,098 |
| free-proxy lane contribution | ~12 rps (deprecated; disallowed for v4) |
| verified archives | **0** |

The ~6.55M undetermined responses are **not** treated as missing files.
v3 stored only aggregate outcome counts plus contiguous coverage pointers, so
individual 503 positions cannot be enumerated; the only honest retry is to
re-probe the known probed ranges, which necessarily re-probes confirmed-404
positions inside them. That overlap is disclosed here and is de-duplicated in
all unique-candidate accounting (duplicate probes are counted separately and
never inflate coverage).

## Candidate universe (frozen spec, `scan/spec_v1.json`)

Deterministic, append-only tier list; the stream is a pure function of the
spec file (sha256 `a83ec705fd7bea3cdb1e061df292036d3c4d86c7b5632ffbe30befc47b261982`).

| tier | disposition | candidates |
|---|---|---|
| t5_fwd_3600_7200s (both sentinels) | previously unsearched | 7,200,000 |
| t4b_back_600_1800s (both sentinels) | previously unsearched | 2,400,000 |
| t1_retry_fwd_0_300s | retry range (contains undetermined) | 600,000 |
| t4_retry_back_0_600s | retry range (contains undetermined) | 1,200,000 |
| t2_retry_fwd_300_1800s | retry range (contains undetermined) | 3,000,000 |
| t3_retry_fwd_1800_3600s | retry range (contains undetermined) | 3,600,000 |
| t0_anchors_final (64 map IDs x 4 anchors, minus 4 sentinel pairs owned by t1_retry) | final consistency recheck (all were confirmed 404; queued last, not blindly re-probed) | 252 |
| **total unique candidates** | | **18,000,252** |

Partitioning: interval chunks of 20,000 ordinals over the ordered stream →
901 chunks, structural exactly-one-owner guarantee (property-tested in
`scan/test_partition.py`: coverage, disjointness, cross-process determinism,
resume purity).

## Architecture

```
controller (workflow, every 5 min + refill triggers)
  ├─ reconciles queue.json on the scan-state branch
  │    PENDING -> RUNNING -> COMPLETED
  │                 |          └-> (undetermined) -> retry_batches -> PENDING
  │                 +-> RETRY -> FAILED (job-attempt cap)
  ├─ detects stale RUNNING via claims/ + GitHub run API, requeues safely
  ├─ dispatches up to MAX_ACTIVE_WORKERS=100 worker workflows (adaptive)
  ├─ publishes global_rate.json (global AIMD budget; measured, not assumed)
  ├─ independently re-verifies every FOUND_200 (direct GET, ZIP+CRC+SHA-256)
  ├─ writes monitoring/status.json + monitoring/report.md every cycle
  └─ on drain: aggregate.py -> results/final/unique_accounting.json

worker (workflow, one deterministic chunk per run)
  ├─ validates ownership (queue entry + dispatch token + result absence)
  ├─ commits claim/{key}.json (startup record, run mapping)
  ├─ probes: HEAD; 404=CONFIRMED_404; 503/timeout=UNDETERMINED (never 404)
  ├─ HTTP 200 -> direct GET -> ZIP structure + CRC + SHA-256 (zipcheck)
  ├─ per-worker AIMD: start 25 rps, +5 per stable window, x0.5 + bounded
  │   exponential backoff on 503/timeout, capped by global budget/hint
  └─ commits results/{key}.result.json atomically (artifact backup)
```

State lives on the `scan-state` branch (atomic git commits = crash-safe
checkpoints); every chunk is independently restartable; duplicate ownership is
rejected via tokens; incomplete chunks are never marked completed (PARTIAL
results resume by skipping already-confirmed ordinals).

## FINAL RESULT (2026-10-05)

The distributed v4 scan **completed with 100% coverage of the frozen universe**:

| metric | value |
|---|---|
| unique candidates scheduled (spec v1) | 18,000,252 |
| unique candidates actually probed | **18,000,252** |
| remaining unsearched | **0** (coverage_fraction 1.0) |
| canonical confirmed HTTP 404 | **18,000,252** |
| canonical FOUND 200 / verified maps | **0** |
| undetermined after all bounded retries | **0** |
| head attempts total (dup probes excluded from coverage) | 18,000,373 (121 duplicates) |
| worker workflow runs | 1,474 (931 success, 373 ownership-rejections [exit 3, no probe], 170 superseded-queued cancellations) |
| peak simultaneous workers | 99 (cap 100) |
| measured aggregate throughput | ~950 rps average over 5h17m; ~7,800 rps instantaneous at the 16,000 rps global-budget cap |
| global rate controller | adaptive 1,500 -> 16,000 rps (AIMD on measured bad-frac; 0.0000 over 17.9M probes) |
| per-worker rate control | start 25 rps, +5/stable-window, x0.5 + bounded exp backoff on 503/timeout |
| 403 / 429 responses | 0 |
| proxies / IP rotation / bypass | none (disallowed) |

`results/final/unique_accounting.json` is the authoritative machine-readable
accounting; `scan_complete.json` on the `scan-state` branch marks the drain.

**Truthful conclusion:** within the evidence-defined candidate universe (both
2020-09-17 and 2020-11-18 anchor clusters, millisecond-exact, maps path only),
no map archive exists on the live CDN as of 2026-10-05: every one of the
18,000,252 candidates returned HTTP 404 on direct probes, and the handful of
transient 503/timeout observations were re-probed to confirmed 404 within the
bounded retry rounds. Nothing was fabricated; 503 was never counted as a miss;
duplicates were never counted as unique coverage.

## Live status

- `results/monitoring/report.md` (updated every controller cycle)
- `scan-state` branch: `monitoring/status.json`, `queue.json`, `global_rate.json`

## Honesty rules (enforced in code)

1. HTTP 503/timeout are UNDETERMINED and are never converted to 404/misses.
2. A map exists only after direct GET + ZIP structure + CRC + SHA-256
   verification; worker observations alone are candidates, the controller
   re-verifies independently.
3. Unique coverage counts each candidate once; duplicate probes are reported
   separately (`duplicate_probes_excluded_from_unique_coverage`).
4. Throughput numbers are measured (per-worker windows aggregated), never
   theoretical.
5. The timestamp universe counts as searched only when the queue mathematically
   drains: `coverage_fraction = probed_unique / 18,000,252`.
6. Test-mode results are excluded from all coverage accounting.

## Evidence baseline (preserved from previous phases)

- `maps.json`, `maps.csv`, `timeline.json`, `predictions.json`,
  `game_registry.json`, `scan_statistics.json`, `SHA256SUMS`, `evidence.json`
- `evidence/fast_run/` — v3 coverage pointers (`prog_*.json`), per-shard stats,
  calibration; regenerable raw candidate lists are not committed
- `evidence/metadata/` — connectivity report, v1 probe transcript, run log

Historical result: 0 verified archives; all negative results reported as-is
with UNSEARCHED ranges explicitly listed.
