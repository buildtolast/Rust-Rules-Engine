#!/usr/bin/env bash
# Generate a single file with the local LLM (Unsloth Studio, OpenAI-compatible).
# Spec is read from stdin; the first..last fenced code block of the reply is
# written to <output-file>. Use -r to print the raw reply instead.
#
# Requires UNSLOTH_API_KEY in the environment. The key is never written to disk.
set -euo pipefail

BASE_URL="${UNSLOTH_BASE_URL:-http://localhost:8888}"
MODEL="${UNSLOTH_MODEL:-unsloth/GLM-4.7-Flash-GGUF}"
: "${UNSLOTH_API_KEY:?UNSLOTH_API_KEY is not set}"

LLM_USAGE_LOG="${LLM_USAGE_LOG:-$(cd "$(dirname "$0")" && pwd)/llm-usage.log}"

RAW=0
OUT=""
if [[ "${1:-}" == "-r" ]]; then RAW=1; else OUT="${1:?usage: llm-gen.sh <output-file> | -r}"; fi

SPEC="$(cat)"
export BASE_URL MODEL LLM_USAGE_LOG RAW OUT SPEC

python3 - <<'PY'
import json, os, re, sys, datetime, urllib.request, urllib.error

SYSTEM = (
    "You are an expert software engineer. Produce complete, compilable code with "
    "no placeholders or TODOs. Reply with exactly one fenced code block containing "
    "the full file and nothing outside it."
)
body = json.dumps({
    "model": os.environ["MODEL"],
    "temperature": 0.2,
    "max_tokens": 16384,
    "messages": [
        {"role": "user", "content": SYSTEM + "\n\n" + os.environ["SPEC"]},
    ],
}).encode()

req = urllib.request.Request(
    os.environ["BASE_URL"] + "/v1/chat/completions", data=body,
    headers={"Content-Type": "application/json",
             "Authorization": "Bearer " + os.environ["UNSLOTH_API_KEY"]})
try:
    with urllib.request.urlopen(req, timeout=600) as r:
        data = json.loads(r.read().decode())
except urllib.error.URLError as e:
    sys.exit("error: LLM request failed: %s" % e)

if "choices" not in data:
    sys.exit("error from LLM API: " + json.dumps(data)[:2000])

content = re.sub(r"<think>.*?</think>", "", data["choices"][0]["message"]["content"],
                 flags=re.DOTALL).strip()
usage = data.get("usage") or {}
sys.stderr.write("[tokens: prompt=%s completion=%s]\n"
                 % (usage.get("prompt_tokens"), usage.get("completion_tokens")))
with open(os.environ["LLM_USAGE_LOG"], "a") as f:
    f.write(json.dumps({
        "ts": datetime.datetime.now().isoformat(timespec="seconds"),
        "tool": "llm-gen",
        "prompt_chars": len(os.environ["SPEC"]),
        "completion_chars": len(content),
        "reported_prompt_tokens": usage.get("prompt_tokens") or 0,
        "reported_completion_tokens": usage.get("completion_tokens") or 0,
    }) + "\n")

if os.environ["RAW"] == "1":
    print(content); sys.exit(0)

lines = content.splitlines()
fences = [i for i, l in enumerate(lines) if l.lstrip().startswith("```")]
if len(fences) < 2:
    sys.exit("error: no code block in reply:\n" + content)
block = "\n".join(lines[fences[0] + 1:fences[-1]]).rstrip() + "\n"
out = os.environ["OUT"]
os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
with open(out, "w") as f:
    f.write(block)
sys.stderr.write("wrote %s (%d chars)\n" % (out, len(block)))
PY
