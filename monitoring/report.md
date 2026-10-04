# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T20:12:20Z (cycle 24.6s, 45 API calls)_

## Workers
- Active workers: **92** (peak 99, cap 100)
- Dispatched this cycle: 8

## Chunks
- Completed **290** / running **100** / pending **511** / retry **0** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 5800000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 12200252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 79.7 over 98 results
- Rolling latency p50/p95: 338.6 / 437.0 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
