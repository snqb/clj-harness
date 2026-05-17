# PS: Streamlining the harness across dependents

> sn-only, not for distribution.

## Current: `:local/root` everywhere

| Project | Dep |
|---------|-----|
| tapalakbot-v2 | `{:local/root "../clj-harness"}` |
| cljr-site-factory | `{:local/root "../clj-harness"}` |

**Risk**: breaking changes propagate silently. Compaction threshold change, removed functions (`send-with-thinking`), new required args — all hit dependents at next `git pull`.

## Options

### A. Git deps with SHA (recommended)

```clojure
;; deps.edn in dependents
;; DEV (fast iteration):
clj-harness {:local/root "../clj-harness"}

;; DEPLOY (pinned):
clj-harness {:git/url "file:///Users/sn/Projects/clj-harness"
             :git/sha "4f08c7e..."}
```

Tag releases (`git tag v2.0.0`) and use `:git/tag` instead of SHA.

**Tradeoff**: need to remember to update SHA. But explicit breakage > silent breakage.

### B. Clojars (if opensource)

`clj-harness "0.2.0"` — real versioning, real SemVer. Dependents bump when ready.

### C. Keep local/root + add version check

```clojure
;; harness core.clj
(def version "2.0.0")

;; dependents check at startup:
(assert (= "2.0.0" h/version) "clj-harness version mismatch!")
```

## What to document per dependent

Each dependent's AGENTS.md should list:

```markdown
## clj-harness features used
- `handle-message` (core)
- `stream-to-telegram` (streaming)
- `reset-keyboard` (telegram)
- `session/sqlite` (persistence)
```

This way, when you change the harness, `grep "feature-used" *.md` tells you who to test.

## Code execution

> Brief take, since you asked.

The harness already has `tools/shell.clj` for calling scripts. A dedicated "code execution" tool would let the LLM run small snippets:

| What | How | Risk |
|------|-----|------|
| Shell | Existing `shell-tool` | ✅ safe (args are strings) |
| Clojure eval | `clojure.core/eval` in sandbox classloader | 🔴 high — full JVM access |
| Python | `uv run --script -` with stdin | 🟡 medium — filesystem access |
| Deno | `deno eval` with `--allow-none` | 🟢 low — fully sandboxed |

**Recommendation**: start with a Deno eval tool (`--allow-none` sandbox). LLM gets a calculator/transformer without fs or network access. Add to `tools/code.clj`:

```clojure
{:name "eval"
 :description "Run JavaScript in sandbox. Use for math, string transforms, JSON."
 :schema {"type" "object" "properties" {"code" {"type" "string"}}}
 :execute (fn [args]
            (let [code (get args "code")
                  result (:out (sh "deno" "eval" "--no-prompt" 
                                   "--allow-none" code))]
              (str/trim result)))}
```

If you want to pursue, I'll build it properly.
