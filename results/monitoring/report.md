# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T22:19:17Z (cycle 17.4s, 34 API calls)_

## Workers
- Active workers: **98** (peak 99, cap 100)
- Dispatched this cycle: 2

## Chunks
- Completed **781** / running **99** / pending **20** / retry **5** / failed **0** (total 905)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 15619997
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 3
- Remaining unsearched: 2380252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 81.7 over 96 results
- Rolling latency p50/p95: 333.0 / 436.8 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 4 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
