# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T22:45:16Z (cycle 13.3s, 21 API calls)_

## Workers
- Active workers: **4** (peak 99, cap 100)
- Dispatched this cycle: 0

## Chunks
- Completed **892** / running **8** / pending **0** / retry **5** / failed **0** (total 905)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 17820249
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 3
- Remaining unsearched: 180000

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 82.9 over 105 results
- Rolling latency p50/p95: 340.1 / 440.8 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 4 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
