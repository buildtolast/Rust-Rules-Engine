Modify the Rust function below. Output EXACTLY ONE fenced ```rust block. Nothing outside the block.

CHANGE: Replace the `strip_fences` function with `extract_json` that is more robust.
The current function only strips markdown code fences. The LLM often outputs prose/markdown
BEFORE and AFTER the JSON object (e.g. numbered reasoning steps). We need to extract the
JSON object from wherever it appears in the response.

New logic for `extract_json(s: &str) -> &str`:
1. Trim the input
2. Strip leading ```json or ``` fence if present, strip trailing ``` if present, trim again
3. Find the first `{` and last `}` in the result
4. If both are found and start < end, return that slice
5. Otherwise return the trimmed string as-is (let serde_json produce the error)

Also update the call site in `fetch_insights` to use `extract_json` instead of `strip_fences`.

INPUT — replace BOTH the helper function AND its call site:

fn strip_fences(s: &str) -> &str {
    let s = s.trim();
    let s = s.strip_prefix("```json").or_else(|| s.strip_prefix("```")).unwrap_or(s);
    let s = s.strip_suffix("```").unwrap_or(s);
    s.trim()
}

// call site (inside fetch_insights):
                let json = strip_fences(&raw);
                match serde_json::from_str::<LlmResponse>(json) {
