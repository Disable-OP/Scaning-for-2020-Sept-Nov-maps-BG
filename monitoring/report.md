# Deep MAPS scan v5 - live status (unattended)
_Cycle completed 2026-10-05T17:35:00Z (cycle 96.4s, 75 API calls)_

## Frontier (unbounded deep universe)
- Next epoch/chunk: **3 / 197** - ladder exhausted: **False**
- Epochs committed: 3 - candidates scheduled so far: 29,993,748

## Workers
- Active workers: **2** (peak 3, cap 100)
- Dispatched this cycle: 60

## Materialized chunks (sliding window)
- Running **546** / pending **54** / retry **0** / abandoned **0** (total in window 600)
- Completed (pruned to tallies): **0**

## Candidates (unique, exact tallies)
- Confirmed 404: 0
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Other errors: 0

## Rate & latency (measured)
- Global budget: 1500.0 rps - insufficient data
- Measured mean worker rps (recent window): 0.0 over 0 results
- Rolling latency p50/p95: 0.0 / 0.0 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification; all v1-confirmed candidates are excluded, never re-probed.
