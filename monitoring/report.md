# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:16:19Z (cycle 17.9s, 36 API calls)_

## Workers
- Active workers: **98** (peak 99, cap 100)
- Dispatched this cycle: 2

## Chunks
- Completed **83** / running **93** / pending **670** / retry **55** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 1660000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 16340252

## Rate & latency (measured)
- Global budget: 4588.4 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 38.9 over 40 results
- Rolling latency p50/p95: 345.3 / 450.1 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
