# eval — CEL rule evaluation

Replaces the Java SpEL evaluator (`eval/RuleEvaluator.java`) with CEL via
`cel-interpreter`. Compile a rule's expression once (`compile`), then evaluate it
against event payloads on the hot path (`evaluate`). Zero I/O dependencies.

## Binding contract

The event's parsed JSON payload is bound as the CEL activation variable
**`event`**. Java's SpEL binds the parsed `Map` as the expression root, so a SpEL
root index `['amount']` becomes `event.amount` in CEL.

A rule expression **must evaluate to a boolean**. Anything else (a number, a
string, etc.) is treated as ERRORED.

## Outcome taxonomy (mirrors Java)

| Outcome | When |
|---|---|
| `MATCHED` | expression returns `true` |
| `UNMATCHED` | expression returns `false` (reason = the expression text) |
| `ERRORED` | binding failure, runtime error (e.g. missing field in a relational op), or a non-boolean result |

`compile` is **total**: a malformed expression — whether a clean parse error or a
parser panic (the CEL parser, antlr4rust, can `unreachable!()` on some input) —
returns `CompileError` instead of propagating, so a bad rule cannot crash the
loader (S7).

## SpEL → CEL translation rules (Version-A contract)

This is the riskiest semantic gap in the rebuild. We target **Version A**: direct,
idiomatic CEL that is provably correct on events where the referenced fields are
present (the demo/load-test data S11 exercises). See the project plan's S2 risk
notes for the Version-A vs Version-B decision.

| SpEL | CEL | Notes |
|---|---|---|
| `['amount']` | `event.amount` | root map index → member access on `event` |
| `['metadata']['source']` | `event.metadata.source` | nested index → nested member |
| `and` / `or` | `&&` / `\|\|` | CEL has no `and`/`or` keywords |
| `['order'] != null` | `has(event.order)` | CEL presence test; CEL has no null-compare for presence |
| `['order']['items'].?[['price'] > 678].size() > 0` | `event.order.items.exists(i, i.price > 678)` | SpEL *selection* → `exists` macro; element index `['price']` rebinds to `i.price` |
| `['timestamp'].startsWith('202')` | `event.timestamp.startsWith("202")` | `startsWith` is a built-in CEL string function |
| string literals `'web'` | `"web"` | CEL uses double quotes |

### Documented divergence vs Java (Version A)

CEL errors on **any** missing-field access, whereas SpEL returns `null` for a
missing map key. So on **sparse/malformed events** a clause that Java would treat
as `UNMATCHED` (e.g. `null == 'web'` → false) can become `ERRORED` in CEL. On
fully-populated events (all demo/load-test payloads) there is no divergence.
Achieving verdict-parity on arbitrary events would require the Version-B
`has()`-guarded translator (deferred).

### Number types

`serde_json` integers within `i64` range serialize to CEL `Int`, and floats to
CEL `Float`; the seed rules compare like-with-like (`amount > <int>`,
`tax_rate >= <float>`), so no cross-type coercion is triggered. Mixed
int/float comparisons in untrusted rules could error — covered by the ERRORED path.
