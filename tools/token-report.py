#!/usr/bin/env python3
"""Compare token usage: local LLM (from llm-usage.log) vs Claude/Fable (session transcript).

    ./token-report.py [--usage-log PATH] [--transcript PATH]

Local server reports 0 tokens, so local counts are estimated from char counts
(~3.6 chars/token) when the reported value is 0.
"""
import argparse, glob, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))


def default_usage_log():
    return os.path.join(HERE, "llm-usage.log")


def latest_transcript():
    files = glob.glob(os.path.join(os.path.expanduser("~/.claude/projects"), "*", "*.jsonl"))
    return max(files, key=os.path.getmtime) if files else None


def parse_local(path):
    if not path or not os.path.exists(path):
        return None
    calls = pt = ct = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            rp, rc = rec.get("reported_prompt_tokens", 0), rec.get("reported_completion_tokens", 0)
            pt += rp if rp > 0 else int(rec.get("prompt_chars", 0) / 3.6)
            ct += rc if rc > 0 else int(rec.get("completion_chars", 0) / 3.6)
            calls += 1
    return {"calls": calls, "prompt": pt, "completion": ct}


def parse_transcript(path):
    if not path or not os.path.exists(path):
        return None
    out = {"calls": 0, "fresh": 0, "cache_w": 0, "cache_r": 0, "output": 0}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            msg = rec.get("message")
            usage = msg.get("usage") if isinstance(msg, dict) else None
            if isinstance(usage, dict) and "output_tokens" in usage:
                out["calls"] += 1
                out["fresh"] += usage.get("input_tokens") or 0
                out["cache_w"] += usage.get("cache_creation_input_tokens") or 0
                out["cache_r"] += usage.get("cache_read_input_tokens") or 0
                out["output"] += usage.get("output_tokens") or 0
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--usage-log", default=default_usage_log())
    ap.add_argument("--transcript", default=latest_transcript())
    args = ap.parse_args()

    local = parse_local(args.usage_log)
    fable = parse_transcript(args.transcript)

    print("=== Token usage comparison ===")
    if local:
        lt = local["prompt"] + local["completion"]
        print("  local LLM (estimated where server reported 0):")
        print(f"    calls:             {local['calls']:,}")
        print(f"    prompt tokens:     {local['prompt']:,}")
        print(f"    completion tokens: {local['completion']:,}")
        print(f"    total:             {lt:,}   (cost: $0, local)")
    else:
        print("  local LLM: no data")
        lt = 0

    if fable:
        ft = fable["fresh"] + fable["cache_w"] + fable["cache_r"] + fable["output"]
        print(f"  Claude / Fable (exact, from {args.transcript}):")
        print(f"    calls:             {fable['calls']:,}")
        print(f"    fresh input:       {fable['fresh']:,}")
        print(f"    cache writes:      {fable['cache_w']:,}")
        print(f"    cache reads:       {fable['cache_r']:,}")
        print(f"    output:            {fable['output']:,}")
        print(f"    total processed:   {ft:,}")
    else:
        print("  Claude / Fable: no data")
        ft = 0

    if lt > 0 and ft > 0:
        print(f"  ratio: Fable processes {ft / lt:.1f} x the local total")
    else:
        print("  ratio: no data")


if __name__ == "__main__":
    main()
