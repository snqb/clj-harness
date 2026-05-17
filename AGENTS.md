<!-- Updated: 2026-05-16 -->
# clj-harness

> Generic Clojure agent harness — middleware-based LLM + tools framework. OpenRouter + DeepSeek, MCP or direct tools, per-user sessions, streaming, SQLite persistence. ~1724 lines across 16 files.

## Architecture v2

```
create-bot → handle-message → middleware pipeline → compaction
                ↓                    ↓
          session atoms        core-agent (llm.clj)
          (SQLite-backed)      → wrap-tools (middleware.clj)
                               → wrap-retry (middleware.clj)
                               → wrap-logging (middleware.clj)
                               → streaming (stream.clj)
```

**Dependency graph** — clean, acyclic:
```
infra ──────────────────── (no deps)
  ↑         ↑
llm       mcp ───────────── (→ infra)
  ↑         ↑
middleware ───────────────── (→ mcp, infra — handler injected, no llm dep!)
  ↑
core ────────────────────── (→ infra, llm, middleware, mcp, compact, session)
  ↑
session/memory ──────────── (→ nothing)
tools/shell ──────────────── (→ clojure.java.shell only)
```

Middleware stack: composable, testable. Each bot is `{:config {...} :pipeline fn :sessions (atom {})}`.

| File | Lines | Purpose |
|------|-------|---------|
| `core.clj` | 249 | Bot factory + orchestration: create-bot, message handling, async, MCP bot convenience |
| `llm.clj` | 66 | LLM client: provider dispatch (data-driven), API calls, core-agent handler |
| `middleware.clj` | 107 | Middleware stack: wrap-tools (tool loop), wrap-retry (backoff), wrap-logging (telemetry) |
| `infra.clj` | 69 | Shared infrastructure: Aero config, pass/env secrets, raw HTTP/1.1 client — bottom of dependency chain |
| `compact.clj` | 90 | Adaptive compaction: token estimation, injected LLM summarizer, keep-recent scaling |
| `mcp.clj` | 61 | MCPvisor client: tool discovery (cached), JSON-RPC calls, schema→OpenAI conversion |
| `session/memory.clj` | 56 | In-memory session atoms: message history, arbitrary data, complements SQLite persistence |
| `tools/shell.clj` | 43 | Shell command tools: {{key}} template substitution, string/int args, sh interop |
| `stream.clj` | 206 | SSE streaming via Java HttpClient, `llm-stream` → core.async channel, provider-agnostic |
| `telegram.clj` | 252 | Telegram Bot API: send, edit, typing, polling, handler, format helpers |
| `telegram/format.clj` | 191 | Markdown → Telegram HTML (escape → convert → split at 4096 chars), strip-md for streaming |
| `telegram/streaming.clj` | 186 | Progressive streaming to Telegram: consume core.async channel, throttle-edits with strip-md, final md→html |
| `session/sqlite.clj` | 74 | SQLite persistence — per-bot per-user message storage, survive restarts |
| `skills.clj` | 132 | Design system loader: loads 99+ design systems from EDN resources, merges into agent system prompt |
| `tools/gsheets.clj` | 168 | Google Sheets tool: service account auth, append booking rows (date, time, name, phone, service, status) |
| `tools/business_schema.clj` | 65 | Business schema validator: 30+ business types, injects required fields checklist into agent context |

## Two Tool Modes

**Direct tools** (no MCP needed):
```clojure
{:name "search" :description "Search web"
 :schema {"type" "object" "properties" {"q" {"type" "string"}}}
 :execute (fn [args] "results...")}
```

**MCP tools** (via MCPvisor):
```clojure
(create-mcp-bot {:name "bot" :prompt "..." :mcp-tools [:get_weather :get_today_date]})
```

**Shell tools** (call Python/Node scripts):
```clojure
(shell-tool "lalafo_search" "Search Lalafo"
  "python3 lalafo/client.py --q='{{query}}' --max={{max}}"
  {:query :string :max :number})
```

## Middleware

Pipeline: `core-agent → wrap-tools → wrap-retry → wrap-logging`

- **core-agent** — raw LLM call, returns `{:content :tool-calls :finish}`
- **wrap-tools** — auto tool calling loop, feeds results back to LLM
- **wrap-retry** — retries on HTTP/LLM errors with backoff
- **wrap-logging** — logs timing and finish reason

## Bot Options

| Option | Default | Description |
|--------|---------|-------------|
| `:name` | — | Bot name |
| `:prompt` | — | System prompt |
| `:tools` | [] | Tool definitions |
| `:model` | :claude-sonnet | LLM model |
| `:provider` | :openrouter | :openrouter or :deepseek |
| `:max-turns` | 10 | Max tool-calling iterations |
| `:max-retries` | 2 | Retry count on failures |
| `:pre-hook` | nil | `(fn [user-id text session] => extra-prompt)` |
| `:on-save` | nil | `(fn [user-id session])` called after each response |
| `:persistence` | nil | SQLite persistence config from `clj-harness.session.sqlite/create` — {

## Providers

Two providers supported (both read keys from pass store):
- **OpenRouter**: `pass openrouter/token` or `OPENROUTER_API_KEY` env
- **DeepSeek**: `pass deepseek-api/token` or `DEEPSEEK_API_KEY` env

Model resolution: checks `config.edn :models` map first, then falls back to literal string.

## Key Design Decisions

1. **Middleware over monolith** — Each concern is a composable function. Test individually. Swap order easily.
2. **Raw Java HttpClient (HTTP/1.1)** — MCPvisor rejects HTTP/2. Works for all providers.
3. **Cheshire JSON** — `(parse-string s false)` = string keys, `true` = keyword keys.
4. **Atoms for sessions** — Thread-safe, REPL-inspectable.
5. **Pure recursion agent loop** — No mutable state, no classes.
6. **Aero config** — `#env` and `#or` for env-aware `.edn` on classpath.
7. **String keys everywhere** — `(get m "key")` not `(:key m)`. LLM JSON always uses string keys.

## Gotchas

- **Tool defs take maps, not keywords** — `:tools [{:name "x" :execute fn}]` not `:tools [:x]`. Use `create-mcp-bot` for keyword-style MCP tools.
- **`on-save` must be fast** — called synchronously after each response. Use future/thread for slow saves.
- **HTTP/1.1 only** — MCPvisor returns 400 on HTTP/2. Already handled.
- **Config on classpath** — `config.edn` in `resources/` on `:paths`.
- **Compaction threshold** — defaults to 60K tokens. Configurable via `:agent :compact-threshold`.
- **Tool output truncated** — default 8K chars, controlled by `:max-tool-output` in config.

## Recipes

### With SQLite persistence
```clojure
(require '[clj-harness.core :as h]
         '[clj-harness.session.sqlite :as sess])

(def persistence (sess/create "/tmp/mybot.db"))

(def bot (h/create-bot
           {:name "mybot"
            :prompt "You are helpful."
            :tools [{:name "weather" :schema {...} :execute (fn [_] "sunny")}]
            :model :gemini-flash
            :persistence persistence}))

(h/handle-message bot "user-1" "Hi!")
;; => Sessions survive restarts — messages saved to SQLite

;; Create same bot after restart
(def bot2 (h/create-bot {:name "mybot" :prompt "You are helpful." :tools [...]
                          :model :gemini-flash :persistence persistence}))
(h/handle-message bot2 "user-1" "What did I just say?")
;; => Has full conversation history from DB
```

### Quick start (REPL)
```bash
cd /Users/sn/Projects/clj-harness
clj -M:repl
```

```clojure
(require '[clj-harness.core :as h])

;; Direct tools — no MCP needed
(def bot (h/create-bot
           {:name "helper"
            :prompt "You are helpful. Be concise."
            :tools [{:name "weather"
                     :description "Get weather"
                     :schema {"type" "object" "properties" {"city" {"type" "string"}}}
                     :execute (fn [args] (str "Weather in " (get args "city") ": sunny"))}]
            :model :gemini-flash}))

(h/handle-message bot "u1" "Weather in Paris?")
@(:sessions bot)
```

### MCP bot (needs MCPvisor)
```clojure
(def tour-bot (h/create-mcp-bot
                {:name "tour" :prompt "Tour manager..."
                 :mcp-tools [:get_today_date :search_tours]
                 :model :claude-sonnet}))
(h/handle-message tour-bot "u1" "Туры из Бишкека?")
```

### Shell-tool bot (calls Python)
```clojure
(def lalafo-bot (h/create-bot
                  {:name "shopping"
                   :prompt "Shopping assistant. Use lalafo_search."
                   :tools [(h/shell-tool "lalafo_search" "Search Lalafo"
                             "python3 lalafo_search.py --q='{{query}}'"
                             {:query :string})]
                   :model :claude-sonnet}))
```

### Async
```clojure
(def ch (h/handle-message-async bot "u1" "query"))
(println (<!! ch))
```

### Raw LLM call
```clojure
(h/llm :claude-sonnet
  [{"role" "system" "content" "Be brief."}
   {"role" "user" "content" "Say hi in Russian"}]
  [])
```

### Raw MCP
```clojure
(require '[clj-harness.mcp :as mcp])
(mcp/mcp-call :get_today_date)
(mcp/list-mcp-tools)
```

### Reset conversation (/reset command)
```clojure
;; Add to make-handler :commands
(tg/make-handler
  {:commands {"/start" handle-start
              "/reset" (fn [{:keys [chat-id user-id]}]
                         (h/reset-session! @bot user-id)
                         (tg/send-message chat-id "✅ Conversation reset."))}
   ...})
```

### Shell tools
```clojure
(require '[clj-harness.tools.shell :as st])
(st/shell-tool "search" "Search API" "curl -s api.example.com?q={{query}}" {:query :string})
```

### LLM calls (raw)
```clojure
(require '[clj-harness.llm :as llm])
(llm/llm :claude-sonnet
  [{"role" "system" "content" "Be brief."}
   {"role" "user" "content" "Say hi in Russian"}]
  [])
(llm/core-agent {:model :claude-sonnet :messages [...]})
```

## Dependencies
- Java 21+ (for `java.net.http`)
- Clojure 1.12
- Cheshire (JSON), core.async (streaming)
- tools.logging + Logback (structured logging)
- Aero (config)
- next.jdbc + sqlite-jdbc (session persistence)
- MCPvisor (optional, for MCP tools)

## Streaming

`handle-message-async` supports `:stream? true` mode — returns a `core.async` channel of `{:delta "text"}` / `{:finish "stop"}` / `{:done :closed}` chunks:

```clojure
(def ch (h/handle-message-async bot "u1" "Tell a story" {:stream? true}))
(go-loop []
  (when-let [msg (<! ch)]
    (println "CHUNK:" (:delta msg))  ;; nil on finish/done
    (when (not= :closed (:done msg))
      (recur))))
```

Under the hood: SSE parser in `clj-harness.stream` — works for both DeepSeek (native) and OpenRouter (proxied). Self-contained module (no circular deps).

## Compaction (clj-harness.compact)

Adaptive compaction with Unicode-aware token estimation, extracted to its own module:
- Token estimator: char-by-char — ASCII ~0.3 tokens/char, non-ASCII ~0.75 tokens/char
- Default threshold: 60K tokens (configurable via `:agent :compact-threshold`)
- Keep-recent count: dynamically scales (8→6→4→2) based on total token count
- Summary: oldest half summarized via injected `summarize-fn` (set in `create-bot` as `:compact-summarize-fn`) — prepended as `[conversation summary]` system message
- Fallback: if summarizer fails, keep last 10 messages
- No circular dependencies: `compact.clj` is a pure transform, receives summarize-fn as parameter

## Gaps & Current State

Active modules:
- `clj-harness.infra` — Config, secrets, HTTP client (shared foundation) ✅
- `clj-harness.llm` — Provider dispatch + core-agent handler ✅
- `clj-harness.middleware` — Tool loop, retry, logging middleware ✅
- `clj-harness.compact` — Adaptive conversation compaction ✅
- `clj-harness.mcp` — MCPvisor client (tool discovery, execution, schema conversion) ✅
- `clj-harness.session.memory` — In-memory session atoms ✅
- `clj-harness.tools.shell` — Shell command tools ✅
- `clj-harness.stream` — SSE streaming for DeepSeek/OpenRouter ✅
- `clj-harness.telegram` — Bot API ✅
- `clj-harness.telegram.format` — Markdown→Telegram HTML + strip-md for streaming ✅
- `clj-harness.telegram.streaming` — Progressive streaming with throttle-edits ✅
- `clj-harness.session.sqlite` — SQLite persistence ✅
- `clj-harness.skills` — Design system loader (99+ systems) ✅
- `clj-harness.tools.gsheets` — Google Sheets booking tool ✅
- `clj-harness.tools.business-schema` — Business type validator (30+ types) ✅

Planned:
- _(none — compaction, MCP, LLM, middleware, sessions, shell all extracted 2026-05-17)_

## Versioning

- **Git tags** for release pinning: `git tag vX.Y.Z`
- **Dependents use**: `{:git/url "file:///.../clj-harness" :git/tag "v2.0.0"}`
- **Dev**: swap to `:local/root` for live edits
- **Upgrade guide**: `UPGRADE-v2.md` — migration checklist for downstream bots
- **Current**: `v2.0.0` — streaming + reset keyboard + 60K compaction + reply_markup

## Related
- Architecture doc: `../tapalakbot-v2/.git/reports/system-architecture-20260515.md`
- PI skill: `~/.pi/agent/skills/clojure-harness/SKILL.md`
- Site factory (production user): `/Users/sn/Projects/cljr-site-factory/`
