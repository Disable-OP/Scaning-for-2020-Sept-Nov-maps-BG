# Blockman GO Engine-10068 Map Archive Scan (v1 + v3 fast)

Generated: 2026-10-04T16:23:59Z

## Scope (user-pinned constraints)
- Host: https://staticgs.sandboxol.com
- Path probed: `/sandbox/games/maps/{ID}.{TS}.zip` ONLY (plugins path excluded per user).
- Granularity: **millisecond-dense only**. No second/minute grids were used, because
  map timestamps are millisecond-exact and coarser grids find nothing (per user).
- Timestamps: computed programmatically from UTC definitions and round-trip
  verified with Python `datetime` (S1=1600317822000, S1b=1600317824000, S2=1605665040000, S2b=1605665064000).
- Engine context: `{"engineVersion": 10068, "useCmd": "true", "logLevel": "debug"}`.

## v3 fast scan method
Tiered sentinel sweep (one sentinel per anchor cluster, all ms-dense):
- t0: exact anchor ms values x all 64 map IDs
- t1/t2/t3: forward windows (0-300s, 300-1800s, 1800-3600s) around S1 and S2
- t4: backward 600s window (clock-skew insurance)
- t5 (budget-gated): forward 3600-7200s extension
- fan-out rounds around every verified hit for all remaining map IDs
Concurrency: 8 shard processes x 400 async workers = 3200 in-flight HEAD
probes with adaptive throttling on 429/403/5xx.

## Results
- Probes sent (v3): 8374673 incl. t0 (256 exact-anchor probes)
- Confirmed 404: 1,823,575; probed-undetermined (HTTP 503/timeout under load): 6,551,098
- Proxy lane: 119 validated free proxies contributed 13,514 discovery probes (~12 rps; CDN refuses most datacenter IPs); hits would be re-verified directly before acceptance
- VERIFIED hits (HTTP 200 + zipfile-validated): 0
- See `fast_scan_results_v3.json` for per-stage status counts and exact
  probed millisecond windows; anything outside those windows is UNSEARCHED.
- v1 scan (185,701 HEAD probes, anchor +-2-3s ms-dense rings, both paths)
  is preserved unchanged in this directory's v1 files.

## Files
- `fast_scan_results_v3.json` - v3 scan results (authoritative for v3)
- `scan_statistics.json` - v1 stats + `fast_v3` section
- `evidence.json` - v1 observations + v3 hit/negative observations
- `maps.json`, `maps.csv`, `timeline.json`, `predictions.json`, `game_registry.json` - v1 artifacts (maps.* updated when v3 verified hits exist)
- `metadata/` - connectivity, probe transcript (v1), run logs
- `fast_run/` - transient v3 run state (excluded from package manifest)
- `SHA256SUMS` - checksums for all packaged files
