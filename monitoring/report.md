# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T20:20:32Z (cycle 20.3s, 41 API calls)_

## Workers
- Active workers: **95** (peak 99, cap 100)
- Dispatched this cycle: 5

## Chunks
- Completed **321** / running **100** / pending **480** / retry **0** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 6420000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 11580252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 81.7 over 101 results
- Rolling latency p50/p95: 342.2 / 437.3 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
