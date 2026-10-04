# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T22:28:12Z (cycle 14.3s, 33 API calls)_

## Workers
- Active workers: **84** (peak 99, cap 100)
- Dispatched this cycle: 0

## Chunks
- Completed **818** / running **82** / pending **0** / retry **5** / failed **0** (total 905)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 16359997
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 3
- Remaining unsearched: 1640252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 81.7 over 96 results
- Rolling latency p50/p95: 334.2 / 435.7 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 4 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
