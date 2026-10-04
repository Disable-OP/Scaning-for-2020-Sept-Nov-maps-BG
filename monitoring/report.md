# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:08:26Z (cycle 21.9s, 40 API calls)_

## Workers
- Active workers: **94** (peak 99, cap 100)
- Dispatched this cycle: 6

## Chunks
- Completed **71** / running **98** / pending **681** / retry **51** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 1420000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 16580252

## Rate & latency (measured)
- Global budget: 2623.5 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 51.4 over 51 results
- Rolling latency p50/p95: 340.9 / 443.5 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
