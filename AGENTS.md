<!-- Updated: 2026-05-21 -->
# clj-harness

> Generic Clojure agent harness: middleware-based LLM + tools framework with OpenRouter/DeepSeek, MCP/direct tools, per-user sessions, streaming, Telegram helpers, SQLite persistence, and optional heap storage for large tool outputs. Current: `v2.2.0`. Defaults: `deepseek` provider + `deepseek-v4-pro` model.

## Architecture

`core.clj` is orchestration only. Reusable behavior lives in focused modules:

```text
infra ──┬── llm ───────┐
        ├── mcp ──┐    │
        │         └── middleware
        └────────────── stream ── heap (optional large tool results)

session.memory / session.sqlite ── core ── telegram / telegram.streaming
```

Flow:

```text
create-bot → handle-message → prepare messages → middleware pipeline → LLM/tool loop
                         │
                         ├─ handle-message-stream! → stream/stream-agent
                         └─ handle-message-async  → daemon Thread + core.async channel
```

## Module Map

| File | Purpose |
|------|---------|
| `src/clj_harness/core.clj` | Public bot API + orchestration; delegates to extracted modules; keeps compatibility aliases like `h/llm`, `h/mcp-call`, `h/shell-tool`. |
| `src/clj_harness/infra.clj` | Aero config, pass/env secrets, raw Java HTTP/1.1 client. Bottom of dependency chain. |
| `src/clj_harness/llm.clj` | Provider dispatch, model resolution, raw LLM call, `core-agent`. |
| `src/clj_harness/mcp.clj` | MCPvisor JSON-RPC calls, tool discovery cache, MCP→OpenAI schema conversion. |
| `src/clj_harness/middleware.clj` | `wrap-tools`, `wrap-retry`, `wrap-logging`, tool schema conversion, optional `tool-post-process`. |
| `src/clj_harness/guardrails.clj` | Pure Forge-inspired `:nudges` primitives: tool-call validation, rescue parsing, step nudges, retry budgets, completed-step state. |
| `src/clj_harness/compact.clj` | Pure compaction transform with injected summarizer; no circular dependency. |
| `src/clj_harness/stream.clj` | SSE streaming on raw Thread, streaming tool loop, optional heap integration. |
| `src/clj_harness/heap.clj` | Session-scoped content-addressable store for large tool outputs; adds `fetch_result` pattern. |
| `src/clj_harness/session/*.clj` | In-memory session atom helpers + SQLite persistence config. |
| `src/clj_harness/telegram*.clj` | Telegram Bot API, Markdown→HTML, block-buffered streaming previews, keyboards/inline markup. |
| `src/clj_harness/tools/*.clj` | Shell tool factory and Google Sheets booking tool via gcloud ADC. |
| `src/clj_harness/skills.clj` | Prompt skill/design-system loader from EDN resources. |

## Public Bot API

```clojure
(require '[clj-harness.core :as h])

(def bot
  (h/create-bot
    {:name "helper"
     :prompt "You are helpful. Be concise."
     :tools [{:name "weather"
              :description "Get weather"
              :schema {"type" "object"
                       "properties" {"city" {"type" "string"}}
                       "required" ["city"]}
              :execute (fn [args] (str "Weather in " (get args "city") ": sunny"))}]
     :model :gemini-flash}))

(h/handle-message bot "u1" "Weather in Paris?")
```

Bot options to remember:

| Option | Default | Notes |
|--------|---------|-------|
| `:provider` | `:openrouter` | Also supports `:deepseek`. |
| `:model` | `:claude-sonnet` | `config.edn` maps keys; DeepSeek examples use `:deepseek-v4-pro` / `:deepseek-v4-flash`. |
| `:tools` | `[]` | Maps with `:name`, `:schema`, `:execute`; not keywords unless using `create-mcp-bot`. |
| `:max-turns` | `10` | Tool-call loop limit. |
| `:pre-hook` | nil | `(fn [user-id text session] => extra-system-prompt-string)`. |
| `:on-save` | nil | Synchronous; keep fast or spawn work. |
| `:persistence` | nil | Use `(clj-harness.session.sqlite/create "/tmp/bot.db")`. |
| `:context-reminder?` | true | Adds recent user topics to prompt. |
| `:tool-post-process` | nil | `(fn [tool-name result] => enriched-result)` before truncation. |
| `:nudges` | true | Full guardrail stack under the friendly name: validation/rescue/retry nudges by default; pass `{:required-steps [...] :terminal-tools #{...}}` for step enforcement, or `false` to disable. |

## Tool Modes

Direct tools:

```clojure
{:name "search"
 :description "Search web"
 :schema {"type" "object" "properties" {"q" {"type" "string"}}}
 :execute (fn [args] "results...")}
```

MCP tools:

```clojure
(h/create-mcp-bot
  {:name "tour"
   :prompt "Tour manager..."
   :mcp-tools [:get_today_date :search_tours]
   :model :claude-sonnet})
```

Shell tools:

```clojure
(h/shell-tool "lalafo_search" "Search Lalafo"
  "python3 lalafo_search.py --q='{{query}}' --max={{max}}"
  {:query :string :max :number})
```

## Nudges / Guardrails

`:nudges` is the production name for the full Forge-style stack. Default `true` keeps normal final text responses valid, but validates malformed/unknown tool calls and can rescue JSON tool calls embedded in prose. For workflow rails, pass:

```clojure
{:nudges {:required-steps ["search"]
          :terminal-tools #{"answer"}
          :recover-tool-errors? true}}
```

Tool results can return plain strings or maps like `{:ok? false :content "No data found"}`. Failed/soft-error tools do **not** complete required steps when `:recover-tool-errors?` is true. Same option is passed to `stream/stream-agent`.

Benchmark harness:

```bash
clojure -M:eval
# baseline vs validate-only vs nudges; writes JSONL to .git/reports/
```

## Streaming

Core async:

```clojure
(def ch (h/handle-message-async bot "u1" "Tell a story" :stream? true))
(loop []
  (when-let [msg (<!! ch)]
    (when-let [delta (:delta msg)] (print delta))
    (when-not (= :closed (:done msg)) (recur))))
```

Telegram streaming UX:

- Mid-stream edits use **block-buffered plain text** via `telegram.format/streaming-preview`; unfinished Markdown stays hidden.
- Final edit renders full Markdown to Telegram HTML.
- `telegram.streaming/stream-to-telegram` uses a stopped ticker (`stop-ch`) so no leaking go-loop.
- `telegram/make-handler` supports `:streaming?`, `:reply-markup`, `:post-stream`, and `:on-location`.

## Heap for Large Tool Results

Use `clj-harness.heap/create-heap` and pass `:heap` to `stream/stream-agent`. Results over 2K chars are stored outside context and replaced with compact summaries + heap IDs. A `fetch_result` tool is auto-added so the model can retrieve/filter details on demand.

Downstream example: `../tapalakbot-v2/src/tapalakbot/bot.clj` uses `clj-harness.heap` for large tool outputs.

## Gotchas

- **String keys everywhere for LLM JSON**: use `(get m "key")`, not `(:key m)`, unless a function explicitly returns keywordized data.
- **Core is not the implementation layer**: if adding HTTP/LLM/MCP/session/tool behavior, put it in the focused module and delegate from `core.clj`.
- **Do not reintroduce duplicated code**: `bunx jscpd src --threshold 3 --min-lines 5 --min-tokens 30 --reporters console` should stay at 0 clones.
- **DeepSeek model keys**: use configured keys like `:deepseek-v4-pro`; model resolution falls back to `(name model-key)`.
- **Tool output truncation**: middleware truncates at `:agent :max-tool-output` (default 8000). Streaming heap mode avoids pushing huge outputs into context.
- **Nudges are not safety rails**: `:nudges` means mechanical tool-call reliability, not content moderation. Text final responses remain valid unless `:require-tool? true` or pending required steps force tool use.
- **Telegram Markdown streaming**: preview plain text during stream, final HTML only at the end. Avoid partial Markdown rendering.
- **Public API dead-code false positives**: `clojure-lsp unused-public-var` and Carve reports are low confidence for this library. Use Carve `:api-namespaces` before deleting.
- **Google Sheets auth**: `tools/gsheets.clj` uses gcloud Application Default Credentials, not service-account JSON signing.

## Quality Commands

```bash
# After every .clj edit
clj-paren-repair src/clj_harness/core.clj
clojure-lsp format --filenames src/clj_harness/core.clj
clojure-lsp diagnostics --filenames src/clj_harness/core.clj

# Whole project smoke
clojure -M -e '(doseq [n (quote [clj-harness.core clj-harness.guardrails clj-harness.stream clj-harness.telegram])] (require n)) (println :ok)'

# Guardrail/nudges deterministic benchmark
clojure -M:eval

# Duplicate code gate
bunx jscpd src --threshold 3 --min-lines 5 --min-tokens 30 --reporters console

# Clojure dead-code pass
clojure-lsp diagnostics 2>&1 | grep -E "unused-(private-var|binding|import|namespace|referred-var)|unused-value|unused-public-var|error"
```

## Versioning / Dependents

**Recommended setup** — zero ceremony for solo dev:

```clojure
;; DEV — live local/root, all projects pick up harness changes instantly
clj-harness/clj-harness {:local/root "../clj-harness"}

;; PROD — pin to a git tag when deploying (Railway, etc.)
;; clj-harness/clj-harness {:git/url "file:///Users/sn/Projects/clj-harness"
;;                          :git/tag "v2.2.0"}
```

No vendoring, no Clojars, no Maven. Git tags are checkpoints (v2.0.0 → v2.1.0 → v2.2.0).

**Release history**: `v2.0.0` (streaming+compaction) → `v2.1.0` (heap) → `v2.2.0` (extraction).
Current: `v2.2.0`.

**Dependents** (all `:local/root` for dev):

| Project | Dep type | Status |
|---|---|---|
| cljr-site-factory | git tag v2.0.0 (prod) | ✅ |
| tapalakbot-v2 | local/root | DEV |
| cljr-wedding-factory | local/root ../clj-harness | DEV |
| pb-bot (contracts-railway) | local/root | DEV |

`.pi/` is gitignored — do not commit task state.

## Related

- PI skill: `/Users/sn/.pi/agent/skills/clojure-harness/SKILL.md`
- Production user (pinned to v2.0.0): `/Users/sn/Projects/cljr-site-factory/`
