# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T23:46:13Z (cycle 5.7s, 13 API calls)_

## Workers
- Active workers: **0** (peak 99, cap 100)
- Dispatched this cycle: 0

## Chunks
- Completed **905** / running **0** / pending **0** / retry **0** / failed **0** (total 905)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 18000253
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 3
- Remaining unsearched: 0

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 35.3 over 7 results
- Rolling latency p50/p95: 875.8 / 926.4 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 4 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
