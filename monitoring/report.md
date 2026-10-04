# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:03:18Z (cycle 18.1s, 34 API calls)_

## Workers
- Active workers: **98** (peak 98, cap 100)
- Dispatched this cycle: 2

## Chunks
- Completed **64** / running **149** / pending **688** / retry **0** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 1280000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 16720252

## Rate & latency (measured)
- Global budget: 1983.7 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 63.9 over 64 results
- Rolling latency p50/p95: 339.0 / 452.3 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
