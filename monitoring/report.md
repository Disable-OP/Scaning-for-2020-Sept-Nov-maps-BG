# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:19:46Z (cycle 38.5s, 54 API calls)_

## Workers
- Active workers: **86** (peak 99, cap 100)
- Dispatched this cycle: 14

## Chunks
- Completed **96** / running **94** / pending **656** / retry **55** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 1920000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 16080252

## Rate & latency (measured)
- Global budget: 5276.7 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 29.3 over 38 results
- Rolling latency p50/p95: 341.9 / 454.0 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
