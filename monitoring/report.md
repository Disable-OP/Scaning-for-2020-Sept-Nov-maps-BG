# Deep MAPS scan v5 - live status (unattended)
_Cycle completed 2026-10-05T20:33:08Z (cycle 17.2s, 24 API calls)_

## Frontier (unbounded deep universe)
- Next epoch/chunk: **3 / 251** - ladder exhausted: **False**
- Epochs committed: 3 - candidates scheduled so far: 32,693,748

## Workers
- Active workers: **92** (peak 100, cap 100)
- Dispatched this cycle: 8

## Materialized chunks (sliding window)
- Running **38** / pending **209** / retry **352** / abandoned **0** (total in window 599)
- Completed (pruned to tallies): **0**

## Candidates (unique, exact tallies)
- Confirmed 404: 2,750,000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Other errors: 0

## Rate & latency (measured)
- Global budget: 1500.0 rps - insufficient data
- Measured mean worker rps (recent window): 0.0 over 0 results
- Rolling latency p50/p95: 0.0 / 0.0 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification; all v1-confirmed candidates are excluded, never re-probed.
