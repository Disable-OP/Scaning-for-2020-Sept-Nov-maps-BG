#!/usr/bin/env python3
"""Final aggregation for the deep scan v5 campaign (per-epoch accounting).

Reads the scan-state branch via a sparse blob-filtered git clone (fast, no
REST rate limits), merges every probe round canonically per unique candidate,
and commits the final evidence to main:
  results/final/deep_v5_accounting.json
  scan_statistics.json   (key "deep_v5")
  maps.json              (key "deep_v5_verified", only when maps were found)
  SHA256SUMS             (updated for changed files)

Canonical status per unique candidate = strongest observation across every
round (epoch probe + retry batches):
    FOUND_200(valid zip) > FOUND_INVALID > CONFIRMED_404 > OTHER > UNDETERMINED
503/timeout after all bounded rounds stay UNDETERMINED - never a miss.
Duplicate probes are counted separately and never inflate unique coverage.
ABANDONED chunks (repeated crashes) are disclosed as unsearched candidates.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen_candidates as G  # noqa: E402
from gh import GH  # noqa: E402

STATE = "scan-state"
RANK = {"200": 4, "FOUND_INVALID": 3, "404": 2, "OTHER": 1,
        "503": 0, "TIMEOUT": 0, "NETERR": 0}
CLONE_DIR = Path(".agg_state")


def rank(s: str) -> int:
    return RANK.get(s, 1)


def statuses_of(res: dict, chunk_size: int) -> dict[int, str]:
    default = res.get("default_status")
    if not default:
        return {}
    src = res.get("candidate_source") or ""
    off = (res["chunk_id"] * chunk_size
           if (src == "spec" or src.startswith("epoch:"))
           and res.get("chunk_id") is not None else 0)
    n = res.get("candidates_probed_unique", 0)
    ex = {int(p): s for p, s in res.get("exceptions", [])}
    return {pos: ex.get(pos + off, default) for pos in range(n)}


def iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def fetch_state_sparse(token: str, owner: str, repo: str) -> Path:
    if CLONE_DIR.exists():
        shutil.rmtree(CLONE_DIR)
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GH_SCAN_TOKEN"] = token
    env["GIT_ASKPASS"] = "/bin/echo"
    env["GIT_CONFIG_COUNT"] = "1"
    env["GIT_CONFIG_KEY_0"] = "credential.helper"
    env["GIT_CONFIG_VALUE_0"] = (
        '!f(){ printf "username=x-access-token\\npassword=%s\\n" "$GH_SCAN_TOKEN"; };f')
    url = f"https://github.com/{owner}/{repo}.git"
    subprocess.run(["git", "clone", "--depth", "1", "--branch", STATE,
                    "--filter=blob:none", "--sparse", url, str(CLONE_DIR)],
                   env=env, check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(CLONE_DIR), "sparse-checkout", "set",
                    "results", "epochs", "retry_batches", "queue.json",
                    "agg.json"],
                   env=env, check=True, capture_output=True, text=True)
    return CLONE_DIR


def main() -> int:
    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/")
    token = os.environ.get("GITHUB_TOKEN", "")
    gh = GH(token, owner, repo)
    cfg = json.loads(Path("scan/deep_config.json").read_text())
    cs_cfg = cfg["chunk_size"]

    if os.environ.get("AGG_DRYRUN") or os.environ.get("AGG_NOCLONE"):
        base = Path(os.environ.get("AGG_STATE_DIR", "."))
    else:
        base = fetch_state_sparse(token, owner, repo)

    queue = json.loads((base / "queue.json").read_text())
    frontier = queue.get("frontier", {})
    epochs_info = frontier.get("epochs", {})

    # ---------------- load epoch specs + results ---------------------------
    especs: dict[int, dict] = {}
    for k in sorted(epochs_info, key=int):
        p = base / f"epochs/epoch_{int(k)}.json"
        if p.exists():
            especs[int(k)] = json.loads(p.read_text())

    results: dict[str, dict] = {}
    res_dir = base / "results"
    if res_dir.exists():
        for p in sorted(res_dir.glob("*.result.json")):
            key = p.name[:-len(".result.json")]
            try:
                r = json.loads(p.read_text())
            except Exception:
                continue
            if r and r.get("mode") != "test":
                results[key] = r
    print(f"loaded {len(results)} full results, {len(especs)} epoch specs")

    batches: dict[str, dict] = {}
    bdir = base / "retry_batches"
    if bdir.exists():
        for p in sorted(bdir.glob("batch-*.json")):
            try:
                b = json.loads(p.read_text())
                batches[p.stem] = b
            except Exception:
                pass

    batches_by_parent: dict[str, list[str]] = {}
    for key, ent in queue.get("chunks", {}).items():
        if ent.get("kind") == "retry_batch" and ent.get("parent_result"):
            batches_by_parent.setdefault(ent["parent_result"], []).append(key)
    # batch files may exist for entries already pruned from the window
    for bkey, b in batches.items():
        parent = b.get("parent_result")
        if parent and bkey not in batches_by_parent.get(parent, []):
            batches_by_parent.setdefault(parent, []).append(bkey)

    def resolve_batch(key: str, cache: dict) -> dict[int, tuple[str, int]]:
        """batch index -> (target chunk key, local position in that chunk)."""
        if key in cache:
            return cache[key]
        cache[key] = {}
        out: dict[int, tuple[str, int]] = {}
        b = batches.get(key)
        if b:
            for i, c in enumerate(b.get("candidates", [])):
                src_key, src_pos = c.get("source_key"), c.get("source_ordinal")
                if src_key is None:
                    continue
                if src_key.startswith("batch-"):
                    tgt = resolve_batch(src_key, cache).get(int(src_pos))
                    if tgt:
                        out[i] = tgt
                else:
                    out[i] = (src_key, int(src_pos))
        cache[key] = out
        return out

    # ---------------- per-epoch canonical merge ----------------------------
    uniq = {"found_200": 0, "found_invalid": 0, "confirmed_404": 0,
            "undetermined": 0, "other": 0}
    per_epoch = {}
    head_attempts_total = 0
    probe_rounds = 0
    unsearched_abandoned = 0
    abandoned_chunks = 0

    for en, espec in sorted(especs.items()):
        total = espec["candidates_emitted"]
        cs = espec["chunk_size"]
        num = (total + cs - 1) // cs
        cnt = {"found_200": 0, "found_invalid": 0, "confirmed_404": 0,
               "undetermined": 0, "other": 0}
        probed_unique = 0
        unsearched = 0
        chunks_acc = 0
        for cid in range(num):
            key = f"e{en}-chunk-{cid:06d}"
            start = cid * cs
            size = min((cid + 1) * cs, total) - start
            merged: dict[int, str] = {}
            chain = []
            if key in results:
                chain.append(results[key])
            # direct children + transitive batch chains
            stack = list(batches_by_parent.get(key, []))
            while stack:
                bkey = stack.pop()
                for child in batches_by_parent.get(bkey, []):
                    stack.append(child)
                if bkey in results:
                    chain.append(results[bkey])
            if chain:
                probe_rounds += len(chain)
                # primary result (chunk probe)
                st_map = statuses_of(results[key], cs) if key in results else {}
                for pos_local, s in st_map.items():
                    pos_g = start + pos_local
                    cur = merged.get(pos_g)
                    if cur is None or rank(s) > rank(cur):
                        merged[pos_g] = s
                # retry batches (resolve to chunk-local positions)
                for bkey in batches_by_parent.get(key, []):
                    if bkey not in results:
                        continue
                    rmap = resolve_batch(bkey, {})
                    st_map_b = statuses_of(results[bkey], cs_cfg)
                    for pos_b, s in st_map_b.items():
                        tgt = rmap.get(pos_b)
                        if not tgt or tgt[0] != key:
                            continue
                        pos_g = start + tgt[1]
                        cur = merged.get(pos_g)
                        if cur is None or rank(s) > rank(cur):
                            merged[pos_g] = s
            else:
                st_map = {}
            for s in merged.values():
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
            probed_unique += len(merged)
            unsearched += size - len(merged)
            chunks_acc += 1
            if key in results:
                head_attempts_total += results[key].get("head_attempts_total", 0)
        for k in uniq:
            uniq[k] += cnt[k]
        ent = epochs_info.get(str(en), {})
        per_epoch[f"epoch_{en}"] = {
            "kind": espec.get("kind"),
            "evidence_basis": espec.get("evidence_basis"),
            "sha256": ent.get("sha256"),
            "total_candidates": total,
            "chunks": num,
            "chunks_with_results": chunks_acc,
            "probed_unique": probed_unique,
            "unsearched": unsearched,
            "canonical": cnt,
        }
    # abandoned materialized chunks -> disclosed unsearched
    for key, ent in queue.get("chunks", {}).items():
        if ent.get("status") == "ABANDONED":
            unsearched_abandoned += ent.get("size", 0)
            abandoned_chunks += 1

    total_universe = sum(e["candidates_emitted"] for e in especs.values())
    total_probed = sum(e["probed_unique"] for e in per_epoch.values())
    unsearched_total = (total_universe - total_probed)

    dup_probes = head_attempts_total - total_probed
    accounting = {
        "generated_utc": iso(time.time()),
        "campaign": cfg["campaign_id"],
        "universe_definition": "scan/deep_config.json epoch ladder "
                               "(deterministic; v1-confirmed candidates excluded)",
        "epochs_committed": len(especs),
        "ladder_exhausted": frontier.get("exhausted", False),
        "universe_total_unique_candidates": total_universe,
        "unique_candidates_probed": total_probed,
        "unique_unsearched": unsearched_total,
        "coverage_fraction": round(total_probed / total_universe, 6)
        if total_universe else 0.0,
        "canonical_unique": uniq,
        "unsearched_in_abandoned_chunks": unsearched_abandoned,
        "abandoned_chunks": abandoned_chunks,
        "per_epoch": per_epoch,
        "probe_rounds_head_attempts_total": head_attempts_total,
        "probe_rounds_counted": probe_rounds,
        "duplicate_probes_excluded_from_unique_coverage": max(0, dup_probes),
        "notes": [
            "HTTP 503/timeout after all bounded retry rounds remain "
            "UNDETERMINED; they are never counted as confirmed misses.",
            "Unique coverage counts each candidate once regardless of rounds.",
            "FOUND_200 counts HEAD 200 candidates; verified maps require the "
            "independent direct GET + ZIP + CRC + SHA-256 check.",
            "v1 campaign (18,000,252 candidates, all confirmed 404) is "
            "accounted separately and is not re-probed by this campaign.",
        ],
    }

    # tally cross-check (live tallies vs recount)
    tal = frontier.get("tallies", {})
    tally_tot = {}
    for k in ("found_200", "found_invalid", "confirmed_404", "undetermined",
              "other", "probed", "head_attempts", "chunks_done"):
        tally_tot[k] = sum(t.get(k, 0) for t in tal.values())
    accounting["live_tally_cross_check"] = {
        "tallies": tally_tot,
        "recount": {"found_200": uniq["found_200"],
                    "found_invalid": uniq["found_invalid"],
                    "confirmed_404": uniq["confirmed_404"],
                    "undetermined": uniq["undetermined"],
                    "other": uniq["other"],
                    "probed": total_probed},
    }

    print(json.dumps({k: v for k, v in accounting.items()
                      if k != "per_epoch"}, indent=1))

    if os.environ.get("AGG_DRYRUN"):
        print("DRYRUN: skipping commits")
        return 0

    # ---------------- write to main ----------------------------------------
    files = {
        "results/final/deep_v5_accounting.json":
            json.dumps(accounting, indent=1) + "\n",
    }
    agg_state = {}
    try:
        agg_state = json.loads((base / "agg.json").read_text())
    except Exception:
        pass
    verified_maps = [{"url": u, **m} for u, m in
                     (agg_state.get("verified") or {}).items() if m.get("verified")]

    scan_stats = None
    try:
        scan_stats = json.loads(gh.get_file("scan_statistics.json", "main"))
    except Exception:
        scan_stats = {}
    scan_stats["deep_v5"] = {
        "generated_utc": accounting["generated_utc"],
        "architecture": "GitHub Actions: controller + up to 100 adaptive "
                        "workers over an unbounded epoch-ladder universe",
        "epochs_committed": len(especs),
        "unique_candidates_scheduled": total_universe,
        "unique_candidates_probed": total_probed,
        "unique_unsearched": unsearched_total,
        "canonical_unique": uniq,
        "head_attempts_total": head_attempts_total,
        "duplicate_probes": max(0, dup_probes),
        "verified_maps": len(verified_maps),
        "honesty_rules": [
            "503/timeout never converted to 404",
            "duplicate probes never counted as unique coverage",
            "map existence claimed only after direct ZIP+CRC+SHA-256 verification",
            "v1-confirmed candidates excluded, never re-probed",
        ],
    }
    files["scan_statistics.json"] = json.dumps(scan_stats, indent=1) + "\n"

    if verified_maps:
        try:
            maps_doc = json.loads(gh.get_file("maps.json", "main"))
        except Exception:
            maps_doc = {}
        maps_doc["deep_v5_verified"] = verified_maps
        files["maps.json"] = json.dumps(maps_doc, indent=1) + "\n"

    existing_sums = {}
    try:
        for line in (gh.get_file("SHA256SUMS", "main") or b"").decode().splitlines():
            if line.strip():
                h, name = line.split(None, 1)
                existing_sums[name.strip()] = h
    except Exception:
        pass
    for p, content in files.items():
        data = content.encode() if isinstance(content, str) else content
        existing_sums[p] = hashlib.sha256(data).hexdigest()
    files["SHA256SUMS"] = ("\n".join(f"{h}  {name}" for name, h in
                                     sorted(existing_sums.items())) + "\n").encode()

    gh.commit_files("main", files,
                    f"deep scan v5 final aggregation: probed_unique={total_probed} "
                    f"404={uniq['confirmed_404']} found={uniq['found_200']} "
                    f"undetermined={uniq['undetermined']} unsearched={unsearched_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
