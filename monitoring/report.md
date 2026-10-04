# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:29:12Z (cycle 24.0s, 32 API calls)_

## Workers
- Active workers: **90** (peak 99, cap 100)
- Dispatched this cycle: 10

## Chunks
- Completed **122** / running **88** / pending **656** / retry **35** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 2440000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 15560252

## Rate & latency (measured)
- Global budget: 8000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 41.1 over 58 results
- Rolling latency p50/p95: 340.9 / 448.3 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
