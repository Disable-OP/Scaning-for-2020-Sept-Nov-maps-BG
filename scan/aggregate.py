#!/usr/bin/env python3
"""Final aggregation: unique-candidate accounting across all probe rounds.

Canonical status per unique candidate = strongest observation across every
round (spec probe + retry batches):
    FOUND_200(valid zip) > FOUND_INVALID > CONFIRMED_404 > UNDETERMINED
Duplicate probes are counted separately and never inflate unique coverage.
503/timeout at the end of all rounds stay UNDETERMINED - never a miss.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen_candidates as G  # noqa: E402
from gh import GH  # noqa: E402

STATE = "scan-state"
RANK = {"200": 4, "FOUND_INVALID": 3, "404": 2, "OTHER": 1,
        "503": 0, "TIMEOUT": 0, "NETERR": 0}


def rank(s: str) -> int:
    return RANK.get(s, 1)


def statuses_of(res: dict) -> dict[int, str]:
    """pos -> observed status for one result (default + exceptions)."""
    default = res.get("default_status")
    if not default:
        return {}
    n = res.get("candidates_probed_unique", 0)
    st = {pos: default for pos in range(n)}
    for pos, s in res.get("exceptions", []):
        st[pos] = s
    return st


def iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def main() -> int:
    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/")
    gh = GH(os.environ.get("GITHUB_TOKEN", ""), owner, repo)
    spec = G.load_spec("scan/spec_v1.json")

    queue = gh.get_file_json("queue.json", STATE)
    agg = gh.get_file_json("agg.json", STATE) or {"ingested": {}, "verified": {}}
    chunks = queue["chunks"]

    # ---------------- list + fetch all full results ------------------------
    st, ref, _h = gh.api("GET", f"/repos/{owner}/{repo}/git/ref/heads/{STATE}")
    st, tree, _h = gh.api("GET",
                      f"/repos/{owner}/{repo}/git/trees/{ref['object']['sha']}?recursive=1")
    result_paths = [e["path"] for e in tree.get("tree", [])
                    if e["type"] == "blob" and e["path"].startswith("results/")
                    and e["path"].endswith(".result.json")]
    results = {}
    for p in result_paths:
        key = p.split("/", 1)[1][:-len(".result.json")]
        try:
            r = gh.get_file_json(p, STATE)
        except Exception:
            continue
        if r and r.get("mode") != "test":
            results[key] = r
    print(f"fetched {len(results)} full results")

    # ---------------- batch files + resolution to (chunk, ordinal) --------
    batch_entries = {}
    for key, ent in chunks.items():
        if ent.get("kind") == "retry_batch":
            try:
                b = gh.get_file_json(ent["batch_file"], STATE)
                batch_entries[key] = b
            except Exception:
                pass

    def resolve_batch(key: str, cache: dict) -> dict[int, tuple[str, int]]:
        """candidate index in batch `key` -> (chunk_key, chunk_ordinal)."""
        if key in cache:
            return cache[key]
        b = batch_entries.get(key)
        cache[key] = {}
        out = {}
        if not b:
            cache[key] = out
            return out
        for i, c in enumerate(b.get("candidates", [])):
            src_key, src_pos = c.get("source_key"), c.get("source_ordinal")
            if src_key is None:
                continue
            if src_key.startswith("chunk-"):
                out[i] = (src_key, int(src_pos))
            elif src_key.startswith("batch-"):
                parent_map = resolve_batch(src_key, cache)
                tgt = parent_map.get(int(src_pos))
                if tgt:
                    out[i] = tgt
        cache[key] = out
        return out

    # ---------------- per-chunk canonical merge ---------------------------
    batches_by_parent: dict[str, list[str]] = {}
    for key, ent in chunks.items():
        if ent.get("kind") == "retry_batch" and ent.get("parent_result"):
            batches_by_parent.setdefault(ent["parent_result"], []).append(key)

    uniq = {"found_200": 0, "found_invalid": 0, "confirmed_404": 0,
            "undetermined": 0, "other": 0}
    per_tier_undet: dict[str, int] = {}
    per_chunk_coverage = {}
    dup_probes = 0
    head_attempts_total = 0
    chunk_cache: dict = {}

    tier_of_ordinal = {}
    for tid, start, count in G.tier_offsets(spec):
        for o in range(start, start + count):
            tier_of_ordinal[o] = tid

    for key, ent in chunks.items():
        if ent.get("kind") != "spec":
            continue
        cid = int(key.split("-")[1])
        start, end, _tot = G.chunk_bounds(spec, cid)
        size = end - start
        merged: dict[int, str] = {}
        rounds = 0
        chain = []
        if key in results:
            chain.append(results[key])
        for bkey in batches_by_parent.get(key, []):
            if bkey in results:
                chain.append(results[bkey])
        for res in chain:
            rounds += 1
            st_map = statuses_of(res)
            for pos, s in st_map.items():
                cur = merged.get(pos)
                if cur is None or rank(s) > rank(cur):
                    merged[pos] = s
        # follow-up batches whose parent is a batch of this chunk
        stack = list(batches_by_parent.get(key, []))
        while stack:
            bkey = stack.pop()
            for child in batches_by_parent.get(bkey, []):
                stack.append(child)
            if bkey not in results:
                continue
            rmap = resolve_batch(bkey, chunk_cache)
            st_map = statuses_of(results[bkey])
            for pos_b, s in st_map.items():
                tgt = rmap.get(pos_b)
                if not tgt or tgt[0] != key:
                    continue
                pos = tgt[1]
                cur = merged.get(pos)
                if cur is None or rank(s) > rank(cur):
                    merged[pos] = s
            rounds += 1
        # count canonical statuses for this chunk
        cnt = {"found_200": 0, "found_invalid": 0, "confirmed_404": 0,
               "undetermined": 0, "other": 0}
        for pos, s in merged.items():
            if s == "200":
                cnt["found_200"] += 1
            elif s == "FOUND_INVALID":
                cnt["found_invalid"] += 1
            elif s == "404":
                cnt["confirmed_404"] += 1
            elif s in ("503", "TIMEOUT", "NETERR"):
                cnt["undetermined"] += 1
            else:
                cnt["other"] += 1
        for k in uniq:
            uniq[k] += cnt[k]
        tier_id = tier_of_ordinal.get(start, "unknown")
        per_tier_undet[tier_id] = per_tier_undet.get(tier_id, 0) + cnt["undetermined"]
        probed_unique = len(merged)
        per_chunk_coverage[key] = {
            "size": size, "probed_unique": probed_unique,
            "unsearched_in_chunk": size - probed_unique, "rounds": rounds,
            "canonical": cnt,
        }
        for r in chain:
            head_attempts_total += r.get("head_attempts_total", 0)
    dup_probes = head_attempts_total - sum(
        c["probed_unique"] for c in per_chunk_coverage.values())

    total_universe = G.total_candidates(spec)
    total_probed_unique = sum(c["probed_unique"] for c in per_chunk_coverage.values())
    unsearched = total_universe - total_probed_unique
    accounting = {
        "generated_utc": iso(time.time()),
        "spec_sha256": spec["_sha256"],
        "universe_definition": "scan/spec_v1.json (deterministic; tier order fixed)",
        "universe_total_unique_candidates": total_universe,
        "unique_candidates_probed": total_probed_unique,
        "unique_unsearched": unsearched,
        "coverage_fraction": round(total_probed_unique / total_universe, 6),
        "canonical_unique": uniq,
        "undetermined_by_tier": per_tier_undet,
        "probe_rounds_head_attempts_total": head_attempts_total,
        "duplicate_probes_excluded_from_unique_coverage": max(0, dup_probes),
        "chunks": per_chunk_coverage,
        "notes": [
            "HTTP 503/timeout after all bounded retry rounds remain UNDETERMINED; "
            "they are never counted as confirmed misses.",
            "Unique coverage counts each candidate once regardless of how many "
            "rounds probed it; duplicate probes are reported separately.",
            "FOUND_200 counts candidates whose HEAD returned 200; verified maps "
            "require the independent direct GET + ZIP + CRC + SHA-256 check.",
        ],
    }

    # ---------------- write to main ----------------------------------------
    files = {
        "results/final/unique_accounting.json":
            json.dumps(accounting, indent=1) + "\n",
    }
    verified_maps = [{"url": u, **m} for u, m in (agg.get("verified") or {}).items()
                     if m.get("verified")]
    scan_stats = None
    try:
        scan_stats = json.loads(gh.get_file("scan_statistics.json", "main"))
    except Exception:
        scan_stats = {}
    scan_stats["distributed_v4"] = {
        "generated_utc": accounting["generated_utc"],
        "architecture": "GitHub Actions: controller + up to 100 adaptive workers",
        "spec_sha256": spec["_sha256"],
        "unique_candidates_scheduled": total_universe,
        "unique_candidates_probed": total_probed_unique,
        "unique_unsearched": unsearched,
        "canonical_unique": uniq,
        "head_attempts_total": head_attempts_total,
        "duplicate_probes": max(0, dup_probes),
        "verified_maps": len(verified_maps),
        "honesty_rules": [
            "503/timeout never converted to 404",
            "duplicate probes never counted as unique coverage",
            "map existence claimed only after direct ZIP+CRC+SHA-256 verification",
        ],
    }
    files["scan_statistics.json"] = json.dumps(scan_stats, indent=1) + "\n"

    if verified_maps:
        try:
            maps_doc = json.loads(gh.get_file("maps.json", "main"))
        except Exception:
            maps_doc = {}
        maps_doc["distributed_v4_verified"] = verified_maps
        files["maps.json"] = json.dumps(maps_doc, indent=1) + "\n"

    import hashlib
    existing_sums = {}
    try:
        for line in (gh.get_file("SHA256SUMS", "main") or b"").decode().splitlines():
            if line.strip():
                h, name = line.split(None, 1)
                existing_sums[name.strip()] = h
    except Exception:
        pass
    for p, content in files.items():
        if p == "SHA256SUMS":
            continue
        data = content.encode() if isinstance(content, str) else content
        existing_sums[p] = hashlib.sha256(data).hexdigest()
    # existing evidence files keep their recorded hashes; changed/new files updated
    files["SHA256SUMS"] = ("\n".join(
        f"{h}  {name}" for name, h in sorted(existing_sums.items())) + "\n").encode()

    gh.commit_files("main", files,
                    f"final aggregation: probed_unique={total_probed_unique} "
                    f"404={uniq['confirmed_404']} found={uniq['found_200']} "
                    f"undetermined={uniq['undetermined']} unsearched={unsearched}")
    print(json.dumps({k: v for k, v in accounting.items() if k != "chunks"},
                     indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
