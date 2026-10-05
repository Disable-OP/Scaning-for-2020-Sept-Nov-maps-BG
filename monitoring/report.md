# Deep MAPS scan v5 - live status (unattended)
_Cycle completed 2026-10-05T19:11:05Z (cycle 15.7s, 33 API calls)_

## Frontier (unbounded deep universe)
- Next epoch/chunk: **3 / 230** - ladder exhausted: **False**
- Epochs committed: 3 - candidates scheduled so far: 31,643,748

## Workers
- Active workers: **100** (peak 100, cap 100)
- Dispatched this cycle: 0

## Materialized chunks (sliding window)
- Running **74** / pending **76** / retry **450** / abandoned **0** (total in window 600)
- Completed (pruned to tallies): **0**

## Candidates (unique, exact tallies)
- Confirmed 404: 1,650,000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Other errors: 0

## Rate & latency (measured)
- Global budget: 1725.0 rps - bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 70.8 over 33 results
- Rolling latency p50/p95: 345.7 / 447.3 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification; all v1-confirmed candidates are excluded, never re-probed.
