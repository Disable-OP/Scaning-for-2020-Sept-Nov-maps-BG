# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T20:55:29Z (cycle 23.9s, 45 API calls)_

## Workers
- Active workers: **93** (peak 99, cap 100)
- Dispatched this cycle: 7

## Chunks
- Completed **456** / running **99** / pending **345** / retry **1** / failed **0** (total 901)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 9119999
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 1
- Remaining unsearched: 8880252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 81.8 over 94 results
- Rolling latency p50/p95: 335.9 / 438.2 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 0 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
