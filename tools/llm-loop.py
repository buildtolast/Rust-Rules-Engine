#!/usr/bin/env python3
"""Autonomous generate -> verify -> fix loop against a local OpenAI-compatible LLM.

    echo "<spec>" | ./llm-loop.py <output-file> "<verify-cmd>" [--max-iters N]

The spec is read from stdin. Each iteration: generate the file, write it, run
<verify-cmd>; on failure feed the trimmed build output back and retry. Exits 0
when the verify command passes, non-zero after --max-iters failures.

Env: UNSLOTH_API_KEY (required), UNSLOTH_BASE_URL, UNSLOTH_MODEL, LLM_USAGE_LOG.
The API key is never written to disk.
"""
import argparse, datetime, json, os, re, subprocess, sys, urllib.error, urllib.request

BASE_URL = os.environ.get("UNSLOTH_BASE_URL", "http://localhost:8888")
MODEL = os.environ.get("UNSLOTH_MODEL", "unsloth/GLM-4.7-Flash-GGUF")
LOG = os.environ.get("LLM_USAGE_LOG",
                     os.path.join(os.path.dirname(os.path.abspath(__file__)), "llm-usage.log"))


def chat(messages):
    body = json.dumps({"model": MODEL, "temperature": 0.2, "max_tokens": 16384,
                       "messages": messages}).encode()
    req = urllib.request.Request(
        BASE_URL + "/v1/chat/completions", data=body,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + os.environ["UNSLOTH_API_KEY"]})
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            data = json.loads(r.read().decode())
    except urllib.error.URLError as e:
        sys.exit("error: LLM request failed: %s" % e)
    if "choices" not in data:
        sys.exit("error from LLM API: " + json.dumps(data)[:2000])
    content = data["choices"][0]["message"]["content"]
    _log(messages, content, data.get("usage") or {})
    return content


def _log(messages, content, usage):
    with open(LOG, "a") as f:
        f.write(json.dumps({
            "ts": datetime.datetime.now().isoformat(timespec="seconds"),
            "tool": "llm-loop",
            "prompt_chars": sum(len(m.get("content", "")) for m in messages),
            "completion_chars": len(content),
            "reported_prompt_tokens": usage.get("prompt_tokens") or 0,
            "reported_completion_tokens": usage.get("completion_tokens") or 0,
        }) + "\n")


def extract_block(content):
    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL)
    lines = content.split("\n")
    fences = [i for i, l in enumerate(lines) if l.strip().startswith("```")]
    if len(fences) < 2:
        return None
    return "\n".join(lines[fences[0] + 1:fences[-1]]).rstrip() + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("output_file")
    ap.add_argument("verify_cmd")
    ap.add_argument("--max-iters", type=int, default=4)
    args = ap.parse_args()

    if not os.environ.get("UNSLOTH_API_KEY"):
        sys.exit("error: UNSLOTH_API_KEY is not set")

    spec = sys.stdin.read()
    messages = [
        {"role": "system", "content":
            "You are an expert Java 21 / Spring Boot engineer. Always reply with "
            "exactly one fenced code block containing the complete file. No commentary."},
        {"role": "user", "content": spec},
    ]

    for it in range(1, args.max_iters + 1):
        sys.stderr.write("[iter %d] generating...\n" % it)
        block = extract_block(chat(messages))
        if block is None:
            messages += [{"role": "assistant", "content": "(no code block)"},
                         {"role": "user", "content":
                          "Reply again with exactly one fenced code block, complete file."}]
            continue
        os.makedirs(os.path.dirname(args.output_file) or ".", exist_ok=True)
        with open(args.output_file, "w") as f:
            f.write(block)
        sys.stderr.write("[iter %d] wrote %s; verifying...\n" % (it, args.output_file))
        res = subprocess.run(args.verify_cmd, shell=True, capture_output=True, text=True)
        if res.returncode == 0:
            sys.stderr.write("[iter %d] verify OK\n" % it)
            sys.exit(0)
        tail = (res.stdout + res.stderr)[-4000:]
        sys.stderr.write("[iter %d] verify failed; feeding back\n" % it)
        messages += [
            {"role": "assistant", "content": "```\n%s\n```" % block[:6000]},
            {"role": "user", "content":
             "The verify step failed. Fix the file and reply with exactly one fenced "
             "code block containing the COMPLETE corrected file.\n\nOutput:\n" + tail},
        ]

    sys.exit("failed after %d iterations" % args.max_iters)


if __name__ == "__main__":
    main()
