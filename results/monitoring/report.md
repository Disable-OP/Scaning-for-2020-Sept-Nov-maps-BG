# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:22:22Z (cycle 14.6s, 37 API calls)_

## Workers
- Active workers: **97** (peak 99, cap 100)
- Dispatched this cycle: 3

## Chunks
- Completed **99** / running **94** / pending **656** / retry **52** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 1980000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 16020252

## Rate & latency (measured)
- Global budget: 6068.2 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 30.2 over 41 results
- Rolling latency p50/p95: 342.7 / 452.5 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
