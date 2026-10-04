# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T21:42:56Z (cycle 18.4s, 37 API calls)_

## Workers
- Active workers: **97** (peak 99, cap 100)
- Dispatched this cycle: 3

## Chunks
- Completed **638** / running **99** / pending **164** / retry **3** / failed **0** (total 904)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 12759998
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 2
- Remaining unsearched: 5240252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 81.8 over 92 results
- Rolling latency p50/p95: 336.0 / 439.7 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 3 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
