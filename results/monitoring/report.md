# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T20:02:55Z (cycle 17.2s, 40 API calls)_

## Workers
- Active workers: **96** (peak 99, cap 100)
- Dispatched this cycle: 4

## Chunks
- Completed **251** / running **100** / pending **550** / retry **0** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 5020000
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 0
- Remaining unsearched: 12980252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 75.1 over 98 results
- Rolling latency p50/p95: 336.4 / 437.1 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
