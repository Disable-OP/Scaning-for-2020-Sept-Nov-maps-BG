# Scanning for 2020 Sept–Nov maps (Blockman GO, Engine 10068)

Archival existence research for map ZIPs served by `staticgs.sandboxol.com` at
`/sandbox/games/maps/{map_id}.{ts_ms}.zip`, focused on the 2020-09-17 and
2020-11-18 build clusters (Engine context `{"engineVersion": 10068, "useCmd": "true", "logLevel": "debug"}`).

**Scope: MAPS path only.** Millisecond-exact timestamps (never rounded).
No proxy/IP-rotation, no bypass of access controls, adaptive rate control at
worker and global level.

## Current phase: distributed re-scan + undetermined retry (v4)

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
