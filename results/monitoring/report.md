# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T22:17:26Z (cycle 21.4s, 46 API calls)_

## Workers
- Active workers: **92** (peak 99, cap 100)
- Dispatched this cycle: 8

## Chunks
- Completed **777** / running **98** / pending **26** / retry **4** / failed **0** (total 905)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 15539997
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 3
- Remaining unsearched: 2460252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 81.7 over 99 results
- Rolling latency p50/p95: 333.5 / 436.3 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 4 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
