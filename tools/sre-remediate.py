#!/usr/bin/env python3
"""
SRE Remediation Pipeline
========================
Fetches current SRE findings, sends each one to the local LLM alongside
the relevant source file, collects the generated fixes, and opens a GitHub
PR for human + Claude review.

Usage:
  python3 tools/sre-remediate.py [--dry-run]

Environment:
  UNSLOTH_API_KEY   API key for the local LLM (default: empty)
  SRE_API           SRE findings endpoint (default: http://localhost:3000/api/sre/findings)
  LLM_URL           OpenAI-compatible completions URL (default: http://localhost:8888/v1/chat/completions)
  LLM_TIMEOUT       Seconds to wait for each LLM response (default: 180)
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────

SRE_API     = os.environ.get("SRE_API", "http://localhost:3000/api/sre/findings")
LLM_URL     = os.environ.get("LLM_URL",  "http://localhost:8888/v1/chat/completions")
LLM_MODEL   = os.environ.get("LLM_MODEL", "unsloth")
LLM_API_KEY = os.environ.get("UNSLOTH_API_KEY", "")
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "180"))
DRY_RUN     = "--dry-run" in sys.argv

REPO_ROOT = Path(
    subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).decode().strip()
)

ACTIONABLE = {"CRITICAL", "ERROR", "WARN"}

# ── Finding → file mapping ─────────────────────────────────────────────────────
# (container_stem, category_or_None) → path relative to repo root
# Most-specific match wins (first match in order).
FILE_MAP = [
    ("postgres-replica", None,                  "deploy/postgres/replica-entrypoint.sh"),
    ("redpanda",         "config_error",         "deploy/docker-compose.yml"),
    ("redpanda",         "connection_refused",   "deploy/docker-compose.yml"),
    ("redpanda",         None,                   "deploy/docker-compose.yml"),
    ("app",              "connection_refused",   "bin/rules-engine/src/main.rs"),
    ("app",              "oom",                  "deploy/docker-compose.yml"),
    ("app",              None,                   "bin/rules-engine/src/main.rs"),
    ("sre-agent",        "connection_refused",   "crates/sre/src/analysis.rs"),
    ("sre-agent",        None,                   "crates/sre/src/analysis.rs"),
    ("frontend",         None,                   "frontend/nginx.conf"),
    ("clickhouse",       None,                   "deploy/docker-compose.yml"),
    ("postgres",         None,                   "deploy/docker-compose.yml"),
]

def relevant_file(finding: dict) -> str:
    stem     = finding["container_name"].lower().replace("rre-", "").rstrip("-0123456789")
    category = finding["category"]
    for c_prefix, c_cat, path in FILE_MAP:
        if stem.startswith(c_prefix) and (c_cat is None or c_cat == category):
            return path
    return "deploy/docker-compose.yml"


# ── SRE API ────────────────────────────────────────────────────────────────────

def fetch_findings() -> list[dict]:
    print(f"[1/5] Fetching findings from {SRE_API} ...")
    req = urllib.request.Request(SRE_API)
    with urllib.request.urlopen(req, timeout=10) as r:
        findings = json.loads(r.read())
    # Deduplicate: keep the most severe per (container, category).
    priority  = {"CRITICAL": 0, "ERROR": 1, "WARN": 2, "INFO": 3}
    best: dict[tuple, dict] = {}
    for f in findings:
        if f["severity"] not in ACTIONABLE:
            continue
        key = (f["container_name"], f["category"])
        if key not in best or priority[f["severity"]] < priority[best[key]["severity"]]:
            best[key] = f
    result = sorted(best.values(), key=lambda f: priority[f["severity"]])
    print(f"    {len(result)} actionable finding(s) after deduplication.")
    return result


# ── LLM call ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert SRE engineer remediating issues in the Rust-Rules-Engine project.
The project runs: Redpanda (Kafka), ClickHouse, PostgreSQL, a Rust rules engine,
and an SRE agent. Infrastructure lives in docker-compose.yml; app code is in Rust.

Given one or more SRE findings and the CURRENT CONTENT of a source file, output
EXACTLY ONE JSON object with these fields:
  "already_fixed" : true/false  — true if the file already fully addresses ALL findings
  "content"       : string      — ONLY if already_fixed is false: complete new file content
  "commit_msg"    : string      — conventional-commits one-liner (e.g. "fix: ...")
  "summary"       : string      — 2-4 sentences explaining what changed and why

Rules:
- Output ONLY the JSON object. No markdown fences, no prose before or after.
- If already_fixed is true, omit the "content" field.
- Make MINIMAL changes — do not restructure or reformat untouched sections.
- The "content" field must contain the COMPLETE file, not a diff or snippet.
- Never introduce secrets or hardcoded credentials.
"""

def call_llm(findings_for_file: list[dict], file_path: str, file_content: str) -> dict | None:
    # Build a numbered list of all findings targeting this file.
    findings_block = "\n".join(
        f"FINDING {i+1}\n"
        f"  container : {f['container_name']}\n"
        f"  severity  : {f['severity']}\n"
        f"  category  : {f['category']}\n"
        f"  finding   : {f['finding'][:400]}\n"
        f"  fix hint  : {f['proposed_fix'][:200]}"
        for i, f in enumerate(findings_for_file)
    )

    user_msg = (
        f"{findings_block}\n\n"
        f"FILE: {file_path}\n"
        f"```\n{file_content}\n```\n\n"
        f"Generate the JSON remediation object for {file_path}."
    )

    body = json.dumps({
        "model":       LLM_MODEL,
        "messages":    [
            {"role": "system",    "content": SYSTEM_PROMPT},
            {"role": "user",      "content": user_msg},
            {"role": "assistant", "content": "{"},
        ],
        "temperature": 0.05,
        "max_tokens":  4000,
    }).encode()

    headers = {"Content-Type": "application/json"}
    if LLM_API_KEY:
        headers["Authorization"] = f"Bearer {LLM_API_KEY}"

    req = urllib.request.Request(LLM_URL, data=body, headers=headers, method="POST")

    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=LLM_TIMEOUT) as r:
                resp = json.loads(r.read())
            raw = resp["choices"][0]["message"]["content"].strip()
            # Prepend the assistant prefill "{" that is NOT returned in the response.
            json_str = "{" + raw
            # Extract first complete {...} even if there's trailing text.
            depth, start, end = 0, 0, -1
            for i, ch in enumerate(json_str):
                if ch == "{":
                    if depth == 0:
                        start = i
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        end = i
                        break
            if end == -1:
                raise ValueError("no complete JSON object in LLM response")
            return json.loads(json_str[start:end + 1])
        except urllib.error.URLError as e:
            if attempt == 0:
                print(f"      connection error ({e}), retrying in 5 s ...")
                time.sleep(5)
            else:
                print(f"      LLM unreachable after 2 attempts: {e}")
                return None
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"      LLM parse error: {e}")
            return None


# ── Git worktree helpers ───────────────────────────────────────────────────────

def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> str:
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n{result.stderr.strip()}")
    return result.stdout.strip()


def create_worktree(branch: str) -> Path:
    wt_path = REPO_ROOT / ".git" / "sre-remediation-wt"
    if wt_path.exists():
        run(["git", "worktree", "remove", "--force", str(wt_path)], cwd=REPO_ROOT, check=False)
    # Base the remediation branch on main so the PR is clean.
    base = run(["git", "rev-parse", "main"], cwd=REPO_ROOT)
    run(["git", "worktree", "add", "-b", branch, str(wt_path), base], cwd=REPO_ROOT)
    return wt_path


def remove_worktree(wt_path: Path) -> None:
    run(["git", "worktree", "remove", "--force", str(wt_path)], cwd=REPO_ROOT, check=False)


# ── PR creation ────────────────────────────────────────────────────────────────

def create_pr(branch: str, findings: list[dict], patches: list[dict]) -> str:
    # Build PR description.
    table_rows = "\n".join(
        f"| `{p['container']}` | {p['severity']} | `{p['category']}` | `{p['file']}` |"
        for p in patches
    )
    change_details = "\n\n".join(
        f"### `{p['file']}`\n**Container:** `{p['container']}`  \n**Finding:** {p['finding_snippet']}\n\n{p['summary']}"
        for p in patches
    )
    skipped_rows = ""
    if len(findings) > len(patches):
        skipped = [f for f in findings if not any(p["container"] == f["container_name"] and p["file"] == relevant_file(f) for p in patches)]
        if skipped:
            skipped_rows = "\n\n### Already-fixed (no change generated)\n" + "\n".join(
                f"- `{f['container_name']}` / {f['category']}" for f in skipped
            )

    body = f"""\
## SRE Remediation — {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}

Findings from the SRE agent were fed to the local LLM (Unsloth), which generated
targeted fixes for each actionable finding. This PR contains those generated changes
for review.

### Findings addressed

| Container | Severity | Category | File changed |
|---|---|---|---|
{table_rows}
{skipped_rows}

---

## Changes

{change_details}

---

> **Review checklist**
> - [ ] Generated changes are correct and minimal
> - [ ] No secrets or credentials introduced
> - [ ] Docker Compose changes work with `deploy/run.sh`
> - [ ] Rust changes compile (`cargo build --release`)
> - [ ] Stack restarts cleanly after applying

🤖 Generated by `tools/sre-remediate.py` via local LLM (Unsloth)
"""
    title = f"fix(sre): remediate {len(patches)} finding(s) — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    cmd = [
        "gh", "pr", "create",
        "--title", title,
        "--body",  body,
        "--base",  "main",
        "--head",  branch,
    ]
    return run(cmd, cwd=REPO_ROOT)


# ── Main pipeline ──────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Fetch and deduplicate findings.
    findings = fetch_findings()
    if not findings:
        print("No actionable findings. Nothing to remediate.")
        return

    for f in findings:
        print(f"    [{f['severity']}] {f['container_name']} / {f['category']}")

    # 2. Group findings by target file, then call LLM once per file.
    file_groups: dict[str, list[dict]] = {}
    for f in findings:
        path = relevant_file(f)
        file_groups.setdefault(path, []).append(f)

    total_files = len(file_groups)
    print(f"\n[2/5] Calling LLM for {total_files} file(s) ({len(findings)} finding(s) grouped) ...")
    patches: list[dict] = []

    for i, (file_path, file_findings) in enumerate(file_groups.items(), 1):
        abs_path = REPO_ROOT / file_path
        if not abs_path.exists():
            print(f"  [{i}/{total_files}] SKIP {file_path} — not found")
            continue

        containers = ", ".join(f["container_name"] for f in file_findings)
        print(f"  [{i}/{total_files}] {file_path}  ({len(file_findings)} finding(s): {containers})")

        file_content = abs_path.read_text()
        result = call_llm(file_findings, file_path, file_content)

        if result is None:
            print(f"      LLM returned no result — skipping.")
            continue
        if result.get("already_fixed"):
            print(f"      LLM: already fixed — skipping.")
            continue
        if not result.get("content"):
            print(f"      LLM: no content generated — skipping.")
            continue

        # Represent the patch under the most severe finding for the PR table.
        primary = file_findings[0]
        patches.append({
            "container":       ", ".join(f["container_name"] for f in file_findings),
            "severity":        primary["severity"],
            "category":        primary["category"],
            "file":            file_path,
            "content":         result["content"],
            "commit_msg":      result.get("commit_msg", f"fix: address findings in {file_path}"),
            "summary":         result.get("summary", ""),
            "finding_snippet": primary["finding"][:200],
        })
        print(f"      ✓ {result.get('commit_msg', '')[:80]}")

    if not patches:
        print("\nNo fixes generated (all already fixed or LLM unavailable).")
        return

    # 3. Create worktree.
    branch = f"sre/remediation-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    print(f"\n[3/5] Creating git worktree on branch {branch} ...")
    if DRY_RUN:
        print("  [dry-run] skipping worktree creation.")
        for p in patches:
            print(f"  Would write {p['file']} ({len(p['content'])} bytes) — {p['commit_msg']}")
        return

    wt = create_worktree(branch)

    # 4. Apply patches: one commit per unique file.
    print(f"\n[4/5] Applying {len(patches)} patch(es) ...")
    committed_files: set[str] = set()
    for p in patches:
        dest = wt / p["file"]
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(p["content"])
        if p["file"] not in committed_files:
            run(["git", "add", p["file"]], cwd=wt)
            run(["git", "commit", "-m", p["commit_msg"]], cwd=wt)
            committed_files.add(p["file"])
            print(f"    ✓ committed {p['file']}")
        else:
            print(f"    ~ {p['file']} already committed (multiple findings mapped to same file)")

    # 5. Push branch and open PR.
    print(f"\n[5/5] Pushing branch and creating PR ...")
    run(["git", "push", "-u", "origin", branch], cwd=wt)
    pr_url = create_pr(branch, findings, patches)
    print(f"\n✅  PR created: {pr_url}")
    print(f"    Review with: gh pr view {branch} --web")

    remove_worktree(wt)


if __name__ == "__main__":
    main()
