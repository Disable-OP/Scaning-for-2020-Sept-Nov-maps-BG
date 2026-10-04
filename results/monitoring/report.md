# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:52:50Z (cycle 25.3s, 44 API calls)_

## Workers
- Active workers: **92** (peak 99, cap 100)
- Dispatched this cycle: 8

## Chunks
- Completed **212** / running **99** / pending **590** / retry **0** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 4240000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 13760252

## Rate & latency (measured)
- Global budget: 13992.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 70.8 over 94 results
- Rolling latency p50/p95: 337.8 / 441.1 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
