(ns clj-harness.core
  "Generic Clojure bot factory — middleware-based agent harness.

  Architecture:
    create-bot → handle-message → middleware pipeline
    core-agent → wrap-tools → wrap-retry → wrap-logging

  Each bot = config map + session atoms.
  Messages = plain maps. Agent loop = pure recursion + middleware stack.

  Two tool modes:
    :mcp:true   — MCPvisor tool (auto-executed)
    plain map   — User-defined: {:name :schema {:execute (fn [args] ...)}}

  Providers: :openrouter (default) or :deepseek. Both via pass/env.

  Usage:
    ;; Direct tools (no MCP needed)
    (def bot (create-bot
               {:name \"weather\"
                :prompt \"You are a weather assistant.\"
                :tools [{:name \"get_weather\" :schema {...} :execute (fn [args] ...)}]
                :model :claude-sonnet}))
    (handle-message bot \"user-1\" \"Weather in Paris?\")

    ;; MCP tools
    (require '[clj-harness.core :as h])
    (def bot (h/create-mcp-bot
               {:name \"tour\" :prompt \"...\"
                :mcp-tools [:get_today_date :search_tours]
                :model :claude-sonnet}))
    (h/handle-message bot \"u1\" \"Туры из Бишкека?\")"
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]
   [clojure.core.async :as a :refer [<!! go >! <! chan close!]]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [aero.core :as aero]
   [clj-harness.stream :as stream])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpClient$Version HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.time Duration]))

;; ══════════════════════ CONFIG ══════════════════════

(def config
  (aero/read-config (io/resource "config.edn")))

(defn- cfg [& path] (get-in config (vec path)))

(defn- read-api-key
  "Read API key: env var or pass store."
  ([provider]
   (case provider
     :openrouter (or (System/getenv "OPENROUTER_API_KEY")
                     (try (-> (shell/sh "pass" "show" "openrouter/token" :out :string) :out str/trim)
                          (catch Exception _ nil)))
     :deepseek   (or (System/getenv "DEEPSEEK_API_KEY")
                     (try (-> (shell/sh "pass" "show" "deepseek-api/token" :out :string) :out str/trim)
                          (catch Exception _ nil)))
     (or (System/getenv "OPENROUTER_API_KEY")
         (try (-> (shell/sh "pass" "show" "openrouter/token" :out :string) :out str/trim)
              (catch Exception _ nil))))))

;; ══════════════════════ HTTP ══════════════════════

(defn http-post
  "Raw Java HttpClient (HTTP/1.1). Works everywhere — MCPvisor, OpenRouter, Telegram.
   Returns parsed JSON with string keys."
  [url body-str & {:keys [headers timeout-ms] :or {timeout-ms 60000}}]
  (let [client (-> (HttpClient/newBuilder)
                   (.version HttpClient$Version/HTTP_1_1)
                   (.connectTimeout (Duration/ofMillis timeout-ms))
                   .build)
        builder (doto (HttpRequest/newBuilder (URI/create url))
                  (.timeout (Duration/ofMillis timeout-ms))
                  (.header "Content-Type" "application/json"))
        _ (doseq [[k v] (merge {"Content-Type" "application/json"} headers)]
            (.header builder k v))
        _ (.POST builder (HttpRequest$BodyPublishers/ofString body-str))
        req (.build builder)
        resp (.send client req (HttpResponse$BodyHandlers/ofString))]
    (if (>= (.statusCode resp) 400)
      (throw (ex-info (str "HTTP " (.statusCode resp))
                      {:status (.statusCode resp) :body (.body resp) :url url}))
      (json/parse-string (str/trim (.body resp)) false))))

;; ══════════════════════ MCP CLIENT ══════════════════════

(defonce ^:private tool-cache (atom nil))

(defn mcp-call
  "Call MCPvisor tool.
   (mcp-call :get_today_date)          ;; no args
   (mcp-call :search_tours {:city 80}) ;; with args"
  ([tool-name] (mcp-call tool-name {}))
  ([tool-name args]
   (let [r (http-post (str (cfg :mcp-url) "/")
                      (json/generate-string
                       {"jsonrpc" "2.0" "id" (rand-int 99999) "method" "tools/call"
                        "params" {"name" (name tool-name) "arguments" args}})
                      :timeout-ms 30000)]
     (if-let [err (get-in r ["error" "message"])]
       (throw (ex-info (str "MCP error: " err) {:tool tool-name :args args}))
       (->> (get-in r ["result" "content"])
            (filter #(= "text" (get % "type")))
            (map #(get % "text"))
            (str/join "\n"))))))

(defn list-mcp-tools
  "Discover available MCP tools. Cached.
   Returns vector of MCP tool definitions with string keys (name, description, inputSchema)."
  []
  (when-not @tool-cache
    (let [r (http-post (str (cfg :mcp-url) "/")
                       (json/generate-string
                        {"jsonrpc" "2.0" "id" 1 "method" "tools/list" "params" {}}))]
      (reset! tool-cache (get-in r ["result" "tools"]))))
  @tool-cache)

(defn- mcp-tool->openai-schema [tool-def]
  (let [schema (or (get tool-def "inputSchema") {"type" "object" "properties" {}})]
    {"type" "function"
     "function" {"name" (get tool-def "name")
                 "description" (get tool-def "description" "")
                 "parameters" (dissoc schema "default")}}))

;; ══════════════════════ LLM CLIENT ══════════════════════

(def ^:private provider-config
  "Supported LLM providers."
  {:openrouter {:url "https://openrouter.ai/api/v1/chat/completions"
                :headers (fn [key] {"Authorization" (str "Bearer " key)
                                    "HTTP-Referer" "http://localhost"
                                    "X-Title" "CljHarness"})}
   :deepseek   {:url "https://api.deepseek.com/chat/completions"
                :headers (fn [key] {"Authorization" (str "Bearer " key)})}})

(defn- resolve-model [model-key]
  (or (get (cfg :models) model-key) (name model-key)))

(defn llm
  "Call LLM via configured provider.
   (llm :claude-sonnet [{:role \"user\" :content \"Hi\"}] [] :provider :openrouter)
   (llm :deepseek-chat [{...}] [{:name \"search\" ...}] :provider :deepseek :max-tokens 2048)"
  [model-key messages tools & {:keys [provider max-tokens]
                               :or {provider :openrouter max-tokens 4096}}]
  (let [pc (get provider-config provider)
        url (:url pc)
        headers ((:headers pc) (read-api-key provider))
        model-name (resolve-model model-key)
        payload (cond-> {"model" model-name "messages" messages "max_tokens" max-tokens}
                  (seq tools) (assoc "tools" tools "tool_choice" "auto"))]
    (log/info :llm-call :provider provider :model model-name :messages (count messages)
              :tools (count tools))
    (http-post url (json/generate-string payload)
               :headers headers :timeout-ms 180000)))

;; ══════════════════════ MIDDLEWARE STACK ══════════════════════

(defn core-agent
  "Base handler: raw LLM call with no tools iteration.
   Returns {:content ... :tool-calls ... :finish ...}."
  [{:keys [model messages tools provider] :or {provider :openrouter}}]
  (let [resp (llm model messages tools :provider provider)
        choice (first (get resp "choices"))
        msg (get choice "message")]
    {:content (get msg "content")
     :tool-calls (get msg "tool_calls")
     :finish (get choice "finish_reason")}))

(defn wrap-tools
  "Middleware: automatic tool calling loop.
   Tool def: {:name \"search\" :description \"...\" :schema {...} :execute (fn [args] \"result\")}
   MCP tools: add :mcp true, auto-resolved from MCPvisor.

   Feeds tool results back to LLM until it produces a text response or hits max-turns."
  [handler tools]
  (let [tool-map (into {} (map (fn [t] [(get t "name" (:name t)) t]) tools))
        tool-schemas (mapv (fn [t]
                             (if (:mcp t)
                               (mcp-tool->openai-schema t)
                               {"type" "function"
                                "function" {"name" (:name t)
                                            "description" (:description t "")
                                            "parameters" (or (:schema t) {"type" "object" "properties" {}})}}))
                           tools)]
    (fn [{:keys [messages max-turns] :as ctx}]
      (let [mt (or max-turns (cfg :agent :max-turns) 10)]
        (loop [msgs messages turn 0]
          (if (>= turn mt)
            {:content (str "⚠️ Reached max turns (" mt "). Try a more specific query.")
             :tool-calls nil}
            (let [resp (handler (assoc ctx :messages msgs :tools tool-schemas))]
              (if-let [calls (:tool-calls resp)]
                (let [tool-results
                      (mapv (fn [tc]
                              (let [tn (get-in tc ["function" "name"])
                                    args-str (get-in tc ["function" "arguments"])
                                    args (try (if (string? args-str)
                                                (json/parse-string args-str false)
                                                args-str)
                                              (catch Exception _ {}))
                                    t (get tool-map tn)
                                    result (if t
                                             (try ((:execute t) args)
                                                  (catch Exception e
                                                    (str "Tool error: " (.getMessage e))))
                                             (str "Unknown tool: " tn))
                                    max-out (or (cfg :agent :max-tool-output) 8000)
                                    truncated (if (and (string? result) (> (count result) max-out))
                                                (str (subs result 0 max-out) "\n...(truncated)")
                                                result)]
                                {"role" "tool"
                                 "tool_call_id" (get tc "id")
                                 "content" (str truncated)}))
                            calls)]
                  (recur (into (conj msgs {"role" "assistant" "content" (:content resp) "tool_calls" calls})
                               tool-results)
                         (inc turn)))
                resp))))))))

(defn wrap-retry
  "Middleware: retry on exception with exponential backoff.
   (wrap-retry handler 3)  ;; up to 3 retries"
  ([handler] (wrap-retry handler 2))
  ([handler max-retries]
   (fn [ctx]
     (loop [attempt 0]
       (let [result (try {:ok (handler ctx)}
                         (catch Exception e {:err e}))]
         (if-let [err (:err result)]
           (if (< attempt (or max-retries 2))
             (do (log/warn :retry (inc attempt) :error (.getMessage err))
                 (Thread/sleep (* 500 (inc attempt)))
                 (recur (inc attempt)))
             {:content (str "Error after " (inc attempt) " retries: " (.getMessage err))
              :tool-calls nil})
           (:ok result)))))))

(defn wrap-logging
  "Middleware: log timing and finish reason for each turn."
  [handler]
  (fn [ctx]
    (let [t0 (System/currentTimeMillis)
          resp (handler ctx)]
      (log/info :turn-complete :msgs (count (:messages ctx))
                :finish (:finish resp)
                :elapsed (- (System/currentTimeMillis) t0))
      resp)))

;; ══════════════════════ SESSION ══════════════════════

(defn make-session
  "Create a new session atom with messages, summary, and arbitrary data."
  []
  (atom {"messages" [] "summary" nil "data" {}}))

(defn session-add!
  "Add a message to session history."
  [session role content]
  (swap! session update "messages" conj {"role" role "content" content}))

(defn reset-session!
  "Clear session messages and data for user-id in bot's session store.
   Returns true.
   Usage: (reset-session! bot \"user-123\")"
  [bot user-id]
  (when-let [sessions (:sessions bot)]
    (if-let [s (get @sessions user-id)]
      (swap! s assoc "messages" [] "data" {})
      (swap! sessions assoc user-id (atom {"messages" [] "summary" nil "data" {}}))))
  true)

(defn session-messages
  "Get messages from session, prepending summary if present."
  [session]
  (let [{:strs [messages summary]} @session]
    (if summary
      (cons {"role" "system" "content" (str "[Earlier]\n" summary)} messages)
      messages)))

(defn session-data
  "Get arbitrary data stored in session. Use for per-user state."
  [session]
  (get @session "data" {}))

(defn session-update-data!
  "Update session data via function.
   (session-update-data! session assoc \"theme\" \"dark\")"
  [session f & args]
  (apply swap! session update "data" f args))

;; ══════════════════════ COMPACTION ══════════════════════

(def ^:private compact-prompt
  "Summarize this conversation in 3-5 sentences. Keep: user preferences, key decisions,
   important facts, specific details (names, dates, numbers). Drop: tool call internals,
   repetitive exchanges, long descriptions. Return ONLY the summary text.")

(defn- estimate-tokens
  "Estimate token count for a message vector.
   Uses byte-length heuristic adjusted for multilingual (Cyrillic ~2x density)."
  [messages]
  (reduce (fn [total msg]
            (let [content (when (map? msg) (str (get msg "content")))
                  ;; Non-ASCII chars (Cyrillic, CJK) are ~2 bytes in UTF-8
                  ;; but ~1 token per char. ASCII is ~1 byte = ~0.25 tokens.
                  ;; Heuristic: count chars, add extra for ASCII token multiplier
                  chars (count content)
                  ascii (count (re-seq #"[a-zA-Z0-9\s]" content))
                  non-ascii (- chars ascii)]
              (+ total (long (+ (* ascii 0.3) (* non-ascii 0.75))))))
          0
          messages))

(defn- keep-recent-count
  "How many recent messages to keep after compaction. Scales with token count."
  [messages]
  (let [total (estimate-tokens messages)]
    (cond
      (< total 20000)  8
      (< total 40000)  6
      (< total 60000)  4
      :else            2)))

(defn compact-history
  "Compress conversation when over token threshold. Uses LLM summarization.
   Strategy:
   1. Estimate tokens
   2. If under threshold, return as-is
   3. Keep last N recent messages (adaptive count)
   4. Summarize older half via LLM
   5. Prepend summary as system message + recent messages
   Returns compacted message vector."
  [bot messages]
  (let [threshold (or (cfg :agent :compact-threshold) 60000)
        tokens (estimate-tokens messages)]
    (if (< tokens threshold)
      messages
      (try
        (let [keep-n (keep-recent-count messages)
              recent (vec (take-last keep-n messages))
              older (vec (drop-last keep-n messages))
              ;; Take first third (earliest) + last third (before recent) for summary
              split-at (max 1 (quot (count older) 2))
              summary-msgs (vec (take split-at (take split-at older)))
              summary-resp (core-agent
                            {:messages (conj summary-msgs
                                             {"role" "system" "content"
                                              "You are a summarizer. Return ONLY 2-4 sentences."}
                                             {"role" "user" "content" compact-prompt})
                             :model (:model (:config bot))
                             :provider (:provider (:config bot))})
              summary (:content summary-resp)]
          (if (and summary (not (str/blank? summary)))
            (let [summary-msg {"role" "system"
                               "content" (str "[Conversation summary — earlier messages compressed]\n" summary)}]
              (log/info :compaction :before (count older) :before-tokens (long tokens)
                        :after (count recent) :summary-len (count summary))
              (into [summary-msg] recent))
            messages))
        (catch Exception e
          (log/warn e :compaction-failed)
          ;; Fallback: just keep last 10 messages
          (vec (take-last 10 messages)))))))

;; ══════════════════════ BOT FACTORY ══════════════════════

(defn create-bot
  "Create a bot with middleware pipeline.

   Options:
     :name        — bot name (string)
     :prompt      — system prompt (string)
     :tools       — vector of tool defs: [{:name :schema {:execute fn}} ...]
     :model       — model key: :claude-sonnet, :gemini-flash, :deepseek-chat
     :provider    — :openrouter (default) or :deepseek
     :max-turns   — max tool-calling iterations (default 10)
     :max-retries — LLM call retries on failure (default 2)
     :pre-hook    — (fn [user-id text session] => extra-system-prompt-string)
     :on-save     — (fn [user-id session]) called after each response
     :persistence — SQLite persistence config from clj-harness.session.sqlite/create
                    {:type :sqlite :load fn :save fn}

   Returns {:config {...} :pipeline fn :sessions (atom {})}."
  [{:keys [name prompt tools model provider max-turns max-retries pre-hook on-save persistence]
    :or {model :claude-sonnet provider :openrouter max-turns 10 max-retries 2}}]
  (let [pipeline (-> (fn [ctx]
                       (core-agent (assoc ctx :model model :provider provider)))
                     (wrap-tools tools)
                     (wrap-retry max-retries)
                     wrap-logging)
        sessions-atom (atom {})
        ;; Auto-save hook: persist messages after each response
        auto-save (when persistence
                    (let [{:keys [save]} persistence
                          bot-name name]
                      (fn [user-id session]
                        (save bot-name user-id (get @session "messages" [])))))]
    {:config    {:name name :prompt prompt :tools (count tools) :model model :provider provider}
     :pipeline  pipeline
     :pre-hook  pre-hook
     :on-save   (or on-save auto-save)
     :persistence persistence
     :max-turns max-turns
     :sessions  sessions-atom}))

(defn get-or-create-session
  "Get existing user session or create a new one.
   If persistence is configured, loads messages from DB on first access."
  [bot user-id]
  (let [sessions (:sessions bot)
        persistence (:persistence bot)]
    (or (get @sessions user-id)
        (let [;; Load from persistence if available
              loaded-msgs (when-let [load-fn (some-> persistence :load)]
                            (try (load-fn (:name (:config bot)) user-id)
                                 (catch Exception e
                                   (log/warn e :session-load-fail :user-id user-id)
                                   [])))
              s (make-session)]
          ;; Restore messages if we have them (keep last 20 to bound memory)
          (when (seq loaded-msgs)
            (reset! s (assoc @s "messages" (vec (take-last 20 loaded-msgs)))))
          (when persistence
            (log/info :session-restored :user-id user-id :msgs (count loaded-msgs)))
          (swap! sessions assoc user-id s)
          s))))

(defn handle-message
  "Process user message through bot. Returns assistant response string.

   (handle-message bot \"user-1\" \"Find me a laptop\")
   (handle-message bot \"user-1\" \"Cheaper?\" :model :gemini-flash)  ;; override model

   Override options: :model :provider :max-turns"
  [bot user-id text & {:keys [model provider max-turns] :as overrides}]
  (let [session (get-or-create-session bot user-id)
        _ (session-add! session "user" text)
        extra-context (when-let [hook (:pre-hook bot)]
                        (hook user-id text session))
        base-prompt (str (:prompt (:config bot))
                         (when extra-context (str "\n\n" extra-context)))
        history (get @session "messages" [])
        compacted (compact-history bot history)
        msgs (vec (cons {"role" "system" "content" base-prompt}
                        (take-last 20 compacted)))
        ctx (merge {:messages msgs :max-turns (:max-turns bot)}
                   (when model {:model model})
                   (when provider {:provider provider})
                   (when max-turns {:max-turns max-turns})
                   overrides)
        resp ((:pipeline bot) ctx)
        result (or (:content resp) "Sorry, something went wrong.")]
    (session-add! session "assistant" result)
    (when-let [save-fn (:on-save bot)]
      (save-fn user-id session))
    result))

;; ══════════════════════ ASYNC ══════════════════════

(defn handle-message-async
  "Non-blocking message handling via core.async channel.

   Options:
     :stream true  — returns channel of delta chunks (for live output)

   Without streaming:
     (def ch (handle-message-async bot \"u1\" \"Hi\"))
     (println (<!! ch))  ;; full response string

   With streaming:
     (def ch (handle-message-async bot \"u1\" \"Hi\" :stream true))
     (loop [chunk (<!! ch)]
       (when (:delta chunk) (print (:delta chunk)) (flush))
       (when (not (:done chunk)) (recur (<!! ch))))"
  [bot user-id text & {:keys [stream?] :or {stream? false}}]
  (let [ch (chan)
        _handle #(handle-message bot user-id text)]
    (if stream?
      ;; Stream mode: encode system prompt + history, stream LLM response
      (go
        (try
          (let [session (get-or-create-session bot user-id)
                _ (session-add! session "user" text)
                extra-context (when-let [hook (:pre-hook bot)]
                                (hook user-id text session))
                base-prompt (str (:prompt (:config bot))
                                 (when extra-context (str "\n\n" extra-context)))
                history (get @session "messages" [])
                compacted (compact-history bot history)
                msgs (vec (cons {"role" "system" "content" base-prompt}
                                (take-last 20 compacted)))
                ;; Stream from LLM
                stream-ch (stream/llm-stream msgs
                                             :model (:model (:config bot))
                                             :provider (:provider (:config bot))
                                             :max-tokens 4096)
                sb (StringBuilder.)]
            (loop []
              (let [chunk (<! stream-ch)]
                (if chunk
                  (do
                    (when (:delta chunk)
                      (.append sb (:delta chunk))
                      (>! ch {:delta (:delta chunk)}))
                    (when (:finish chunk)
                      ;; Save final response to session
                      (session-add! session "assistant" (str sb))
                      (when-let [save-fn (:on-save bot)]
                        (save-fn user-id session)))
                    (when-not (:done chunk)
                      (recur)))
                  (do
                    (>! ch {:done :closed})
                    (close! ch))))))
          (catch Exception e
            (>! ch {:error (.getMessage e)})
            (close! ch))))
      ;; Non-streaming mode (original behavior)
      (go
        (try (let [r (_handle)] (>! ch r))
             (catch Exception e (>! ch (str "Error: " (.getMessage e))))
             (finally (close! ch)))))
    ch))

;; ══════════════════════ CONVENIENCE ══════════════════════

(defn create-mcp-bot
  "Quick bot creation with MCP tools by keyword name.
   (create-mcp-bot {:name \"weather\" :prompt \"...\" :mcp-tools [:get_weather :get_today_date]})"
  [{:keys [mcp-tools] :as opts}]
  (let [mcp-defs (mapv (fn [tn]
                         (let [td (first (filter #(= tn (get % "name")) (list-mcp-tools)))]
                           (assoc td :mcp true :execute (fn [args] (mcp-call (keyword tn) args)))))
                       mcp-tools)]
    (create-bot (assoc (dissoc opts :mcp-tools) :tools mcp-defs))))

(defn shell-tool
  "Create a tool that executes a shell command with {{key}} template substitution.
   (shell-tool \"search\" \"Search web\" \"python3 lalafo_search.py --q='{{query}}' --max={{max}}\"
               {:query :string :max :number})"
  [name description command arg-spec]
  {:name name
   :description description
   :schema {"type" "object"
            "properties" (into {}
                               (map (fn [[k v]]
                                      [(name k) {"type" (if (= v :number) "integer" "string")
                                                 "description" (name k)}])
                                    arg-spec))
            "required" (vec (map name (keys arg-spec)))}
   :execute (fn [args]
              (let [cmd (reduce-kv (fn [s k v]
                                     (str/replace s (str "{{" (name k) "}}") (str v)))
                                   command args)]
                (try (let [r (shell/sh "bash" "-c" cmd :out :string :err :string)]
                       (if (= 0 (:exit r))
                         (str/trim (:out r))
                         (str "Error: " (str/trim (:err r)))))
                     (catch Exception e (str "Shell error: " (.getMessage e))))))})

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

  ;; MCP tools
  (def tour-bot (create-mcp-bot
                 {:name "tour-manager"
                  :prompt "You are a tour manager for Krugosvet. Help find tours from Kyrgyzstan.
                            Start with get_today_date. Search with get_hot_tours and search_tours."
                  :mcp-tools [:get_today_date :get_hot_tours :search_tours
                              :get_tour_details :get_weather :web_search
                              :get_price_calendar :get_countries :get_departure_cities]
                  :model :claude-sonnet}))
  (handle-message tour-bot "u1" "Горящие туры во Вьетнам из Бишкека? Топ-3.")

  ;; Inspect state
  @(:sessions tour-bot)

  ;; Async
  (def ch (handle-message-async tour-bot "u2" "Погода в Нячанге?"))
  (println (<!! ch))

  ;; Raw MCP
  (mcp-call :get_today_date)
  (list-mcp-tools))
