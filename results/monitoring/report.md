# Distributed MAPS scan — live status
_Cycle completed 2026-10-04T21:59:30Z (cycle 30.2s, 52 API calls)_

## Workers
- Active workers: **90** (peak 99, cap 100)
- Dispatched this cycle: 10

## Chunks
- Completed **704** / running **98** / pending **99** / retry **3** / failed **0** (total 904)

## Candidates (unique)
- Scheduled total: 18000252
- Confirmed 404: 14079998
- FOUND 200: **0** (independently verified maps: **0**)
- Undetermined 503/timeout (never counted as misses): 2
- Remaining unsearched: 3920252

## Rate & latency (measured)
- Global budget: 16000.0 rps — bad=0.000<1% raise x1.15
- Measured mean worker rps (recent window): 81.7 over 97 results
- Rolling latency p50/p95: 337.5 / 441.1 ms
- Recent bad-status fraction (503+timeout+other): 0.0000

## Retry
- Retry batches: 3 (candidate probe-round cap 3, failed chunks 0)

Rule: HTTP 503 is UNDETERMINED and is never reported as a miss; a map exists only after direct ZIP + CRC + SHA-256 verification.
