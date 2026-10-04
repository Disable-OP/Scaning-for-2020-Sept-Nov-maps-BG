# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T19:05:58Z (cycle 13.2s, 33 API calls)_

## Workers
- Active workers: **99** (peak 99, cap 100)
- Dispatched this cycle: 1

## Chunks
- Completed **65** / running **98** / pending **687** / retry **51** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 1300000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 16700252

## Rate & latency (measured)
- Global budget: 2281.3 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 55.1 over 45 results
- Rolling latency p50/p95: 341.5 / 442.5 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
