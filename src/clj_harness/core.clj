(ns clj-harness.core
  "Generic Clojure bot factory — middleware-based agent harness.

  core.clj is orchestration only. Implementation details live in focused modules:
  infra, llm, mcp, middleware, compact, session.memory, tools.shell, stream."
  (:require
   [clojure.string :as str]
   [clojure.core.async :refer [chan close! >!!]]
   [clojure.tools.logging :as log]
   [clj-harness.compact :as compact]
   [clj-harness.dashboard :as dashboard]
   [clj-harness.heap :as heap]
   [clj-harness.infra :as infra]
   [clj-harness.llm :as llm-client]
   [clj-harness.mcp :as mcp]
   [clj-harness.middleware :as mw]
   [clj-harness.observe :as observe]
   [clj-harness.session.memory :as memory]
   [clj-harness.stream :as stream]
   [clj-harness.tools.shell :as shell-tools]))

;; ══════════════════════ PUBLIC COMPATIBILITY ALIASES ══════════════════════

(def config infra/config)
(def http-post infra/http-post)
(def mcp-call mcp/mcp-call)
(def list-mcp-tools mcp/list-mcp-tools)
(def llm llm-client/llm)
(def core-agent llm-client/core-agent)
(def wrap-tools mw/wrap-tools)
(def wrap-retry mw/wrap-retry)
(def wrap-logging mw/wrap-logging)
(def make-session memory/make-session)
(def session-add! memory/session-add!)
(def session-messages memory/session-messages)
(def session-data memory/session-data)
(def session-update-data! memory/session-update-data!)

;; ══════════════════════ STATE SNAPSHOT ══════════════════════

(defn snapshot-session
  "Return full session state as an EDN-serializable map.
   Captures messages, data map, and any session metadata.
   Use with [[rehydrate-session]] for full state persistence.

   Unlike the default message-list persistence, this preserves
   guardrail state, tool history, heap references, and other
   session data stored in the data map."
  [session]
  @session)

(defn rehydrate-session
  "Restore a session atom from a previously snapshotted state.
   Resets the session atom to the given state map."
  [session state]
  (reset! session state)
  session)

;; ══════════════════════ SESSION ══════════════════════

(defn reset-session!
  "Clear session messages and data for user-id in bot's session store.
   Returns true.
   Usage: (reset-session! bot \"user-123\")"
  [bot user-id]
  (when-let [sessions (:sessions bot)]
    (if-let [s (get @sessions user-id)]
      (swap! s assoc "messages" [] "data" {})
      (swap! sessions assoc user-id (memory/make-session))))
  true)

(defn get-or-create-session
  "Get existing user session or create a new one.
   Supports two persistence modes:
     Legacy: loads messages from DB on first access.
     Snapshot: loads full state (messages+data) from DB."
  [bot user-id]
  (let [sessions (:sessions bot)
        persistence (:persistence bot)]
    (or (get @sessions user-id)
        (let [loaded-state (when persistence
                             (or (when-let [load-fn (:snapshot-load persistence)]
                                   (try (load-fn (name (:name (:config bot))) user-id)
                                        (catch Exception e
                                          (log/warn e :session-load-fail :user-id user-id)
                                          nil)))
                                 (when-let [load-fn (:load persistence)]
                                   (try
                                     (when-let [msgs (load-fn (name (:name (:config bot))) user-id)]
                                       (when (seq msgs)
                                         {"messages" (vec (take-last 20 msgs)) "data" {}}))
                                     (catch Exception e
                                       (log/warn e :session-load-fail :user-id user-id)
                                       nil)))))
              s (if loaded-state
                  (memory/make-session loaded-state)
                  (memory/make-session))]
          (when (and persistence loaded-state)
            (log/info :session-restored :user-id user-id
                      :msgs (count (get @s "messages" []))
                      :data-keys (keys (get @s "data" {}))))
          (swap! sessions assoc user-id s)
          s))))

;; ══════════════════════ MESSAGE PREPARATION ══════════════════════

(defn compact-history
  "Compress conversation when over token threshold. Public compatibility wrapper
   around clj-harness.compact/compact-history."
  [bot messages]
  (compact/compact-history
   messages
   {:threshold (or (infra/cfg :agent :compact-threshold) 60000)
    :summarize-fn (fn [summary-msgs]
                    (:content (llm-client/core-agent
                               {:messages summary-msgs
                                :model (:model (:config bot))
                                :provider (:provider (:config bot))})))}))

(defn- prompt-with-pre-hook [bot user-id text session]
  (let [extra-context (when-let [hook (:pre-hook bot)]
                        (hook user-id text session))]
    (str (:prompt (:config bot))
         (when extra-context (str "\n\n" extra-context)))))

(defn- context-reminder [bot compacted]
  (when (:context-reminder? bot)
    (let [prev-topics (->> compacted
                           (filter #(= "user" (get % "role")))
                           (take-last 2)
                           (map #(get % "content"))
                           (remove str/blank?)
                           (str/join "; "))]
      (when (seq prev-topics)
        (str "[Conversation history — user recently asked about: " prev-topics "]")))))

(defn- prepare-messages
  "Build LLM messages for all sync/streaming entrypoints. Assumes caller already
   appended the current user message to the session."
  [bot user-id text session]
  (let [base-prompt (prompt-with-pre-hook bot user-id text session)
        compacted (compact-history bot (get @session "messages" []))
        reminder (context-reminder bot compacted)
        enriched-prompt (cond-> base-prompt
                          reminder (str "\n\n" reminder))]
    (vec (cons {"role" "system" "content" enriched-prompt}
               (take-last 20 compacted)))))

(defn- tool-schemas [tools]
  (mapv mw/tool->openai-schema tools))

(defn- tool-map [tools]
  (into {} (map (fn [t] [(:name t) t]) tools)))

(defn- save-session! [bot user-id session]
  (when-let [save-fn (:on-save bot)]
    (save-fn user-id session)))

;; ══════════════════════ BOT FACTORY ══════════════════════

(defn create-bot
  "Create a bot with middleware pipeline.

   Options:
     :name        — bot name (string)
     :prompt      — system prompt (string)
     :tools       — vector of tool defs: [{:name :schema {:execute fn}} ...]
     :model       — model key: :deepseek-v4-pro (default), :claude-sonnet, :gemini-flash
     :provider    — :deepseek (default) or :openrouter
     :max-turns   — max tool-calling iterations (default 10)
     :max-retries — LLM call retries on failure (default 2)
     :effects?    — use effect-driven agent loop (default true). Set false for imperative loop.
     :pre-hook         — (fn [user-id text session] => extra-system-prompt-string)
     :on-save          — (fn [user-id session]) called after each response
     :context-reminder — auto-inject previous user topics into system prompt (default true)
     :tool-post-process — (fn [tool-name result] => enriched-result) optional
     :nudges           — Forge-style tool-call guardrails (default true). Use a map
                         with :required-steps and :terminal-tools, or false to disable.
             :persistence      — SQLite or custom persistence config:
                                 {:type :sqlite :load fn :save fn}
                                 OR {:snapshot-save fn :snapshot-load fn}
                                 for full-state (messages+data) persistence

   Returns {:config {...} :pipeline fn :sessions (atom {})}."
  [{:keys [name prompt tools model provider max-turns max-retries pre-hook on-save
           context-reminder? tool-post-process nudges persistence dashboard effects?]
    :or {provider :deepseek max-turns 10 max-retries 2 context-reminder? true nudges true effects? true}}]
  (let [resolved-provider (or provider :openrouter)
        resolved-model (or model (if (= resolved-provider :deepseek)
                                   :deepseek-v4-pro
                                   :gemini-flash))
        pipeline (-> (fn [ctx]
                       (llm-client/core-agent
                        (merge {:model resolved-model :provider resolved-provider} ctx)))
                     ((if effects? mw/wrap-tools-v2 mw/wrap-tools) tools tool-post-process nudges)
                     (mw/wrap-retry max-retries)
                     mw/wrap-logging)
        sessions-atom (atom {})
        auto-save (when persistence
                    (if-let [snapshot-save (:snapshot-save persistence)]
                      ;; Full-state persistence (messages + data map)
                      (fn [user-id session]
                        (snapshot-save (:name name) user-id (snapshot-session session)))
                      ;; Legacy: message-only persistence
                      (when-let [save (:save persistence)]
                        (fn [user-id session]
                          (save (:name name) user-id (get @session "messages" []))))))
        _ (when dashboard
            (dashboard/start! (if (map? dashboard) dashboard {})))]
    {:config    {:name name :prompt prompt :tools (count tools)
                 :model resolved-model :provider resolved-provider}
     :pipeline  pipeline
     :pre-hook  pre-hook
     :on-save   (or on-save auto-save)
     :context-reminder? context-reminder?
     :tool-post-process tool-post-process
     :nudges    nudges
     :tools     tools
     :persistence persistence
     :max-turns max-turns
     :dashboard dashboard
     :sessions  sessions-atom}))

;; ══════════════════════ MESSAGE HANDLING ══════════════════════

(defn handle-message
  "Process user message through bot. Returns assistant response string.

   (handle-message bot \"user-1\" \"Find me a laptop\")
   (handle-message bot \"user-1\" \"Cheaper?\" :model :gemini-flash)

   If session has a :heap atom (from create-heap), large tool outputs are
   stored externally and replaced with compact summaries.

   Override options: :model :provider :max-turns"
  [bot user-id text & {:keys [model provider max-turns] :as overrides}]
  (let [dialogue-id (str user-id "-" (System/currentTimeMillis))
        _ (observe/record! {:type :msg-in :dialogue-id dialogue-id :user user-id :text text})
        session (get-or-create-session bot user-id)
        _ (memory/session-add! session "user" text)
        msgs (prepare-messages bot user-id text session)
        ;; Extract heap from session data if present
        session-heap (get-in @session ["data" "heap"])
        t0 (System/currentTimeMillis)
        ctx (merge {:messages msgs
                    :max-turns (:max-turns bot)
                    :model (or model (-> bot :config :model))
                    :provider (or provider (-> bot :config :provider))}
                   (when session-heap {:heap session-heap})
                   (when max-turns {:max-turns max-turns})
                   {:nudges (:nudges bot)}
                   {:dialogue-id dialogue-id}
                   overrides)
        resp ((:pipeline bot) ctx)
        result (or (:content resp) "Sorry, something went wrong.")]
    (observe/record!
     {:type :msg-out :dialogue-id dialogue-id
      :text-len (count result) :total-elapsed (- (System/currentTimeMillis) t0)})
    (memory/session-add! session "assistant" result)
    (save-session! bot user-id session)
    ;; GC expired heap entries
    (when session-heap (heap/gc! session-heap))
    result))

(defn handle-message-stream!
  "Like handle-message but streams the final response through stream-cb.
   stream-cb: (fn [text-chunk]) called with incremental text.
   Options:
     :status-cb — (fn [status-text]) called for phase changes (Russian status).
     :events>   — optional core.async channel for structured events
                  (use clj-harness.stream/event-source to create one).
   Uses stream-agent for the agent loop (tools supported).

   If session has a :heap atom, it's passed to stream-agent for tool output storage.

   Returns accumulated full text, or nil on error."
  [bot user-id text stream-cb & {:keys [status-cb events> abort-signal]}]
  (try
    (let [dialogue-id (str user-id "-" (System/currentTimeMillis))
          _ (observe/record! {:type :msg-in :dialogue-id dialogue-id :user user-id :text text})
          session (get-or-create-session bot user-id)
          _ (memory/session-add! session "user" text)
          msgs (prepare-messages bot user-id text session)
          session-heap (get-in @session ["data" "heap"])
          t0 (System/currentTimeMillis)
          result (stream/stream-agent
                  :model (:model (:config bot))
                  :messages msgs
                  :tool-map (tool-map (:tools bot))
                  :tool-schemas (tool-schemas (:tools bot))
                  :stream-cb stream-cb
                  :status-cb status-cb
                  :events> events>
                  :provider (:provider (:config bot))
                  :max-turns (:max-turns bot)
                  :heap session-heap
                  :abort-signal abort-signal
                  :nudges (:nudges bot)
                  :dialogue-id dialogue-id)]
      (observe/record!
       {:type :msg-out :dialogue-id dialogue-id
        :text-len (count (or result "")) :total-elapsed (- (System/currentTimeMillis) t0)})
      (when result
        (memory/session-add! session "assistant" result)
        (save-session! bot user-id session))
      (when session-heap (heap/gc! session-heap))
      result)
    (catch Exception e
      (log/error e :stream-error)
      nil)))

(defn handle-message-async
  "Non-blocking message handling via core.async channel.

   Options:
     :stream? true  — returns channel of {:delta ...} chunks and a final {:done :closed}

   Without streaming:
     (def ch (handle-message-async bot \"u1\" \"Hi\"))

   With streaming:
     (def ch (handle-message-async bot \"u1\" \"Hi\" :stream? true))"
  [bot user-id text & {:keys [stream?] :or {stream? false}}]
  (let [ch (chan)
        worker (Thread.
                (fn []
                  (try
                    (if stream?
                      (do
                        (handle-message-stream! bot user-id text #(>!! ch {:delta %}))
                        (>!! ch {:done :closed}))
                      (>!! ch (handle-message bot user-id text)))
                    (catch Exception e
                      (>!! ch {:error (.getMessage e)}))
                    (finally
                      (close! ch))))
                "clj-harness-handle-message")]
    (.setDaemon worker true)
    (.start worker)
    ch))

;; ══════════════════════ CONVENIENCE ══════════════════════

(defn create-mcp-bot
  "Quick bot creation with MCP tools by keyword name.
   (create-mcp-bot {:name \"weather\" :prompt \"...\" :mcp-tools [:get_weather :get_today_date]})"
  [{:keys [mcp-tools] :as opts}]
  (let [mcp-defs (mapv (fn [tn]
                         (let [tool-name (name tn)
                               td (first (filter #(= tool-name (get % "name")) (mcp/list-mcp-tools)))]
                           (assoc td :mcp true :execute (fn [args] (mcp/mcp-call (keyword tool-name) args)))))
                       mcp-tools)]
    (create-bot (assoc (dissoc opts :mcp-tools) :tools mcp-defs))))

(def shell-tool shell-tools/shell-tool)
(def event-source stream/event-source)

;; ══════════════════════ REPL ══════════════════════

(comment
  ;; Direct tools (no MCP needed)
  (def test-bot (create-bot
                 {:name "weather"
                  :prompt "You are a weather assistant. Use get_weather for conditions. Be concise, in Russian."
                  :tools [{:name "get_weather"
                           :description "Get current weather for a city"
                           :schema {"type" "object"
                                    "properties" {"city" {"type" "string"}}
                                    "required" ["city"]}
                           :execute (fn [args] (str "Weather in " (get args "city") ": 22°C, sunny"))}]
                  :model :gemini-flash}))
  (handle-message test-bot "u1" "Погода в Париже?")

  ;; Async
  (def ch (handle-message-async test-bot "u2" "Погода в Нячанге?"))
  ch)
