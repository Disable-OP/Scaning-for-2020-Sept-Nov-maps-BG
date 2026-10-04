# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T18:08:27Z (cycle 3.4s, 7 API calls)_

## Workers
- Active workers: **0** (peak 0, cap 0)
- Dispatched this cycle: 0

## Chunks
- Completed **0** / running **0** / pending **901** / retry **0** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 0
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 18000252

## Rate & latency (measured)
- Global budget: 1500.0 rps — insufficient data
- Measured mean worker rps (recent window): 0.0 over 0 results
- Rolling latency p50/p95: 0.0 / 0.0 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
