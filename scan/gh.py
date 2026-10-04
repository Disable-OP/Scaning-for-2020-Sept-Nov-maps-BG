#!/usr/bin/env python3
"""Minimal GitHub REST client for the distributed scan (urllib only, no deps).

Secrets discipline: the token is passed in explicitly (in Actions this is the
built-in GITHUB_TOKEN) and is never logged, never embedded in URLs we print,
never written into artifacts or commits.
"""
from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.request

API = "https://api.github.com"


class GH:
    def __init__(self, token: str, owner: str, repo: str, sleep_fn=time.sleep):
        self.token = token
        self.owner = owner
        self.repo = repo
        self.sleep = sleep_fn
        self.calls = 0

    # ------------------------------------------------------------------ core
    def _req(self, method: str, url: str, *, body=None, headers=None,
             timeout=60, retries=4):
        h = {"Authorization": f"Bearer {self.token}",
             "Accept": "application/vnd.github+json",
             "User-Agent": "blockman-scan-controller/1.0"}
        if headers:
            h.update(headers)
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            h["Content-Type"] = "application/json"
        last = None
        for attempt in range(retries):
            req = urllib.request.Request(url, data=data, headers=h, method=method)
            try:
                self.calls += 1
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    payload = r.read()
                    try:
                        parsed = json.loads(payload) if payload else None
                    except (ValueError, UnicodeDecodeError):
                        parsed = payload  # raw (non-JSON) response body
                    return r.status, parsed, dict(r.headers)
            except urllib.error.HTTPError as e:
                payload = e.read()
                try:
                    parsed = json.loads(payload) if payload else None
                except (ValueError, UnicodeDecodeError):
                    parsed = payload
                if e.code in (409, 500, 502, 503, 504) and attempt < retries - 1:
                    self.sleep(min(2 ** attempt * 1.5, 12))
                    last = e
                    continue
                return e.code, parsed, dict(e.headers or {})
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt < retries - 1:
                    self.sleep(min(2 ** attempt * 1.5, 12))
                    last = e
                    continue
                raise
        raise last  # pragma: no cover

    def api(self, method: str, path: str, *, body=None, headers=None, timeout=60):
        url = path if path.startswith("http") else f"{API}{path}"
        return self._req(method, url, body=body, headers=headers, timeout=timeout)

    # ----------------------------------------------------------------- repos
    def get_ref(self, branch: str):
        st, d, _ = self.api("GET", f"/repos/{self.owner}/{self.repo}/git/ref/heads/{branch}")
        return d if st == 200 else None

    def ensure_branch(self, branch: str, from_branch: str = "main"):
        if self.get_ref(branch):
            return True
        st, d, _ = self.api("GET", f"/repos/{self.owner}/{self.repo}/git/ref/heads/{from_branch}")
        if st != 200:
            raise RuntimeError(f"source branch {from_branch} missing")
        sha = d["object"]["sha"]
        st, _, _ = self.api("POST", f"/repos/{self.owner}/{self.repo}/git/refs",
                            body={"ref": f"refs/heads/{branch}", "sha": sha})
        return st == 201

    def get_file(self, path: str, branch: str):
        st, d, _ = self.api("GET", f"/repos/{self.owner}/{self.repo}/contents/{path}",
                            headers={"Accept": "application/vnd.github.raw"},
                            body=None)
        if st == 200:
            resp = d
            return resp
        if st == 404:
            return None
        raise RuntimeError(f"get_file {path} -> {st}")

    def get_file_json(self, path: str, branch: str):
        raw = self.get_file(path, branch)
        if raw is None:
            return None
        return json.loads(raw)

    def list_dir(self, path: str, branch: str) -> list[str]:
        st, d, _ = self.api("GET", f"/repos/{self.owner}/{self.repo}/contents/{path}?ref={branch}")
        if st == 404:
            return []
        if st != 200 or not isinstance(d, list):
            raise RuntimeError(f"list_dir {path} -> {st}")
        return [e["name"] for e in d if e["type"] == "file"]

    # --------------------------------------------------- atomic multi-commit
    def commit_files(self, branch: str, files: dict, message: str,
                     max_rounds: int = 6) -> str:
        """Atomically commit {path: bytes|str} to branch via Git Data API.
        Retries on ref-update races (409/422) by rebasing onto fresh head."""
        norm = {}
        for p, content in files.items():
            norm[p] = content.encode() if isinstance(content, str) else content
        for rnd in range(max_rounds):
            st, ref, _ = self.api("GET", f"/repos/{self.owner}/{self.repo}/git/ref/heads/{branch}")
            if st != 200:
                raise RuntimeError(f"branch {branch} missing ({st})")
            head_sha = ref["object"]["sha"]
            st, base_commit, _ = self.api("GET", f"/repos/{self.owner}/{self.repo}/git/commits/{head_sha}")
            if st != 200:
                raise RuntimeError(f"head commit missing ({st})")
            base_tree = base_commit["tree"]["sha"]
            tree_entries = []
            for p, content in norm.items():
                st, blob, _ = self.api("POST", f"/repos/{self.owner}/{self.repo}/git/blobs",
                                       body={"content": base64.b64encode(content).decode(),
                                             "encoding": "base64"})
                if st != 201:
                    raise RuntimeError(f"blob create failed {p} ({st})")
                tree_entries.append({"path": p, "mode": "100644",
                                     "type": "blob", "sha": blob["sha"]})
            st, tree, _ = self.api("POST", f"/repos/{self.owner}/{self.repo}/git/trees",
                                   body={"base_tree": base_tree, "tree": tree_entries})
            if st != 201:
                raise RuntimeError(f"tree create failed ({st})")
            st, commit, _ = self.api("POST", f"/repos/{self.owner}/{self.repo}/git/commits",
                                     body={"message": message, "tree": tree["sha"],
                                           "parents": [head_sha]})
            if st != 201:
                raise RuntimeError(f"commit create failed ({st})")
            st, _, _ = self.api("PATCH", f"/repos/{self.owner}/{self.repo}/git/refs/heads/{branch}",
                                body={"sha": commit["sha"], "force": False})
            if st == 200:
                return commit["sha"]
            self.sleep(min(2 ** rnd * 2, 15))  # race: rebase and retry
        raise RuntimeError("commit_files: ref race not resolved")

    # -------------------------------------------------------------- actions
    def dispatch(self, workflow_file: str, ref: str, inputs: dict) -> bool:
        st, _, _ = self.api("POST",
                            f"/repos/{self.owner}/{self.repo}/actions/workflows/{workflow_file}/dispatches",
                            body={"ref": ref, "inputs": inputs})
        return st == 204

    def list_runs(self, workflow_file: str, statuses=("queued", "in_progress"),
                  per_page=100, max_pages=3) -> list[dict]:
        out = []
        for page in range(1, max_pages + 1):
            st, d, _ = self.api("GET",
                                f"/repos/{self.owner}/{self.repo}/actions/workflows/{workflow_file}/runs"
                                f"?per_page={per_page}&page={page}")
            if st != 200:
                break
            runs = d.get("workflow_runs", [])
            out.extend(r for r in runs if r["status"] in statuses)
            if len(runs) < per_page:
                break
        return out

    def all_recent_runs(self, workflow_file: str, per_page=100, max_pages=1) -> list[dict]:
        st, d, _ = self.api("GET",
                            f"/repos/{self.owner}/{self.repo}/actions/workflows/{workflow_file}/runs"
                            f"?per_page={per_page}&page=1")
        return d.get("workflow_runs", []) if st == 200 else []
