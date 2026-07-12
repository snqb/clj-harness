(ns clj-harness.stream
  "SSE streaming — non-blocking LLM streaming on dedicated Thread.
   Does NOT use core.async go-blocks for I/O — runs HTTP on raw Thread
   and pushes parsed deltas into a core.async channel.

   stream-agent is a standalone agent loop with streaming — call it instead
   of clj-harness.core/handle-message for real-time output."
  (:require
   [cheshire.core :as json]
   [clojure.core.async :refer [chan close! >!! <!! sliding-buffer]]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [clj-harness.guardrails :as gr]
   [clj-harness.infra :as infra]
   [clj-harness.llm :as llm]
   [clj-harness.observe :as observe]
   [clj-harness.tool-loop :as tl])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpClient$Version HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.time Duration]
   [java.io BufferedReader InputStreamReader]))

(defn- http-stream-lines
  "Performs an HTTP POST with JSON body and streams the response line-by-line
   into a core.async channel. Runs on a raw Thread (not a go-block) to avoid
   blocking the async thread pool."
  [url headers body]
  (let [ch (chan 256)]
    (.start
     (Thread.
      (reify Runnable
        (run [_]
          (try
            (let [client (-> (HttpClient/newBuilder)
                             (.connectTimeout (Duration/ofSeconds 30))
                             (.build))
                  req-builder (-> (HttpRequest/newBuilder)
                                  (.uri (URI. url))
                                  (.header "Content-Type" "application/json")
                                  (.version HttpClient$Version/HTTP_1_1)
                                  (.timeout (Duration/ofSeconds 120))
                                  (.POST (HttpRequest$BodyPublishers/ofString (json/generate-string body))))
                  req (.build (reduce (fn [r [k v]] (.header r k v)) req-builder headers))
                  resp (.send client req (HttpResponse$BodyHandlers/ofInputStream))
                  status (.statusCode resp)]
              (if (= status 200)
                (let [reader (BufferedReader. (InputStreamReader. (.body resp)))
                      lines (line-seq reader)]
                  (doseq [line lines]
                    (when (str/starts-with? line "data: ")
                      (let [data (subs line 6)]
                        (if (= "[DONE]" data)
                          (>!! ch {:done true})
                          (try
                            (let [parsed (json/parse-string data true)]
                              (>!! ch parsed))
                            (catch Exception e
                              (log/warn :stream-parse-error (.getMessage e))))))))
                  (>!! ch {:done "eof"}))
                (do
                  (log/error :stream-http-error status)
                  (>!! ch {:done (str "http-error-" status)}))))
            (catch Exception e
              (log/error e :stream-error)
              (>!! ch {:done "error"}))
            (finally
              (close! ch)))))
      (str "http-stream-" (hash url))))
    ch))

(defn- provider-stream-config
  "Returns {:url ... :api-key ... :model ...} for a given provider."
  [provider-key]
  (let [cfg (case provider-key
              :deepseek {:url "https://api.deepseek.com/v1/chat/completions"
                         :api-key (infra/read-api-key :deepseek)}
              :openrouter {:url (or (System/getenv "OPENROUTER_BASE_URL")
                                    "https://openrouter.ai/api/v1/chat/completions")
                           :api-key (infra/read-api-key :openrouter)})]
    cfg))

(defn llm-stream
  "Stream LLM response via SSE. Returns a core.async channel.

   Messages arrive as maps: {:delta \"text\"} {:tool-calls [...]} {:finish \"stop\"} {:done true}

   Parameters (keyword args):
     :model    — model string (e.g. \"deepseek-v4-pro\")
     :messages — vector of message maps
     :tools    — optional tool definitions (OpenAI format)
     :provider — :deepseek (default) or :openrouter
     :max-tokens — default 4096"
  [& {:keys [model messages tools provider max-tokens]
      :or {provider :deepseek max-tokens 4096}}]
  (let [cfg (provider-stream-config provider)
        headers (merge {"Authorization" (str "Bearer " (:api-key cfg))}
                       (case provider
                         :openrouter {"HTTP-Referer" "http://localhost"
                                      "User-Agent" "CljHarness/1.0"}
                         {}))
        body (cond-> {"model" model "messages" messages
                      "max_tokens" max-tokens "stream" true}
               (seq tools) (assoc "tools" tools "tool_choice" "auto"))]
    (log/info :stream-start :provider provider :model model
              :msgs (count messages) :tools (count tools))
    (http-stream-lines (:url cfg) headers body)))

;; ══════════════════════ STREAMING AGENT ══════════════════════

(defn- consume-stream
  "Consume a llm-stream channel: parse OpenAI SSE chunks, accumulate
   content, tool_calls, and call stream-cb.
   Returns {:content ... :tool-calls ... :finish ... :usage ...}.
   Blocks until stream completes.

   Captures :usage from the final SSE chunk (DeepSeek/OpenRouter send
   token counts in the last data frame before [DONE])."
  [ch stream-cb]
  (let [content-sb (StringBuilder.)
        tc-chunks (atom {})
        finish-reason (atom nil)
        usage-atom (atom nil)
        ;; stream-cb fires per text delta (hundreds of times). A throwing
        ;; callback must not break the stream, but must also not vanish
        ;; silently — log the first failure only, to avoid log spam.
        cb-failed? (atom false)
        safe-cb (fn [chunk]
                  (try (stream-cb chunk)
                       (catch Exception e
                         (when (compare-and-set! cb-failed? false true)
                           (log/warn e :stream-cb-failed
                                     :note "further failures suppressed for this stream")))))]
    (loop []
      (let [msg (<!! ch)]
        (when msg
          ;; Capture usage from any chunk that carries it (typically the last)
          (when (:usage msg)
            (reset! usage-atom (:usage msg)))
          ;; Parse OpenAI chunk format: {:choices [{:delta {:content "..." :tool_calls [...]}}]}
          (let [choices (:choices msg)
                delta (when (seq choices) (:delta (first choices)))
                finish (when (seq choices) (:finish_reason (first choices)))]
            (when delta
              ;; Reasoning tokens (GLM, Gemini, o1) are model internal "thinking"
              ;; — typically in English and confusing in non-English conversations.
              ;; Log for observability but do NOT forward to stream-cb (user-facing).
              (when (:reasoning delta)
                (log/debug :reasoning-delta :len (count (:reasoning delta))))
              (when (:content delta)
                (.append content-sb (:content delta))
                (safe-cb (:content delta)))
              (when (:tool_calls delta)
                (doseq [tc (:tool_calls delta)]
                  (let [idx (get tc :index 0)
                        args (get-in tc [:function :arguments])]
                    (swap! tc-chunks
                           (fn [chunks]
                             (let [chunk (or (get chunks idx) {})
                                   ;; Preserve :function :name, append :arguments
                                   func-name (or (get-in tc [:function :name])
                                                 (get-in chunk [:function :name]))
                                   cur-args (get-in chunk [:function :arguments] "")
                                   new-args (str cur-args (or args ""))
                                   merged (merge chunk (dissoc tc :function))
                                   updated (assoc merged :function {:name func-name :arguments new-args})]
                               (assoc chunks idx updated)))))))
              (when finish (reset! finish-reason finish))))
          (if (:done msg) nil (recur)))))
    (let [chunks @tc-chunks
          accumulated-tc (when (pos? (count chunks))
                           (vec (for [i (sort (keys chunks))]
                                  (get chunks i))))]
      {:content (.toString content-sb)
       :tool-calls (when (seq accumulated-tc) accumulated-tc)
       :finish (or @finish-reason "stop")
       :usage @usage-atom})))

;; ══════════════════════ EVENT BUS ══════════════════════

(defn event-source
  "Create a core.async channel for structured agent events.
   Pass to stream-agent as :events> to receive typed events
   (:phase/starting, :text/delta, :tool/start, :tool/end, etc.)
   instead of parsing status-cb strings.

   Returns a channel suitable for stream-agent :events>."
  []
  (chan (sliding-buffer 64)))

(defn- emit!
  "Emit a typed event into the events channel if present.
   Best-effort: a full/closed channel is intentionally ignored — events
   are optional diagnostics and must never block or break the agent loop."
  [events> event]
  (when events>
    (try (>!! events> event) (catch Exception _))))

;; ══════════════════════ STATUS MESSAGES ══════════════════════

(defn- status-text
  "Generate Russian status message for agent phases."
  [phase & {:keys [tool-name]}]
  (case phase
    :starting   "🧠 Анализирую запрос..."
    :tool-call  (str "🔧 Выполняю " (or tool-name "инструмент") "...")
    :after-tool "📊 Обрабатываю результаты..."
    :max-turns  "⚠️ Слишком много шагов. Формирую ответ..."
    :retry      "⚠️ Перепроверяю..."
    "🔄 Обрабатываю..."))

(defn notify-status!
  "Call status-cb (1-arity, receives a status string) for the given phase.
   No-op when status-cb is nil. Swallows callback errors but logs them —
   a broken status callback must never break the agent loop.

   NOTE: status-cb is invoked with a SINGLE string argument. Do not use
   (apply status-cb (status-text ...)) — apply on a string splatters it
   into per-character args and breaks 1-arity callbacks."
  [status-cb phase & args]
  (when status-cb
    (try (status-cb (apply status-text phase args))
         (catch Exception e
           (log/warn e :status-cb-failed :phase phase)))))

(defn stream-agent
  "Run agent with streaming LLM calls. Handles tool execution loop.

   Parameters:
     :model         — model key (e.g. :deepseek-v4-pro)
     :messages      — vector of message maps
     :tool-map      — map of tool-name → {:execute (fn [args] ...)}
     :tool-schemas  — OpenAI-format tool definitions
     :stream-cb     — (fn [text-fragment]) called for each content delta
     :status-cb     — (fn [status-text]) called for phase changes (Russian status)
     :events>       — optional core.async channel for structured events
                       (use clj-harness.stream/event-source to create one)
                       Events: :phase/starting, :phase/retry, :phase/done,
                               :tool/start {:tool-name}, :tool/end {:tool-name :ok? :elapsed},
                               :text/delta {:text}, :error/fatal {:reason}
     :provider      — :deepseek (default) or :openrouter
     :max-turns     — max tool-calling iterations (default 10)
     :max-tokens    — max tokens per LLM call (default 4096)
     :heap          — optional clj-harness.heap atom for tool result storage
                       When provided, large tool outputs (>2K chars) are stored
                       in heap and replaced with compact summary + heap-id.
                       fetch_result tool is automatically added to tool-schemas.
     :abort-signal  — optional atom. When set to true, running tool calls are
                       cancelled. Tools that support 3-arity (args, abort, on-update)
                       can check this atom to abort mid-execution.
     :nudges        — full guardrails under the public nudges name; false disables.
     :max-repeated-tool-calls — abort when same tool called N times in a row (default 4).

   Returns accumulated response string."
  [& {:keys [model messages tool-map tool-schemas stream-cb status-cb events> provider max-turns max-tokens heap abort-signal nudges dialogue-id trace-id max-repeated-tool-calls]
      :or {provider :deepseek max-turns 10 max-tokens 4096 nudges true max-repeated-tool-calls 4}}]
  (if-not stream-cb
    ;; Non-streaming fallback: use regular LLM
    (let [resp (llm/llm model messages tool-schemas :provider provider :max-tokens max-tokens)
          choice (first (get resp "choices"))
          msg (get choice "message")]
      (or (get msg "content") "No response."))

    ;; Streaming path
    (let [{:keys [tool-schemas tool-map]} (tl/with-fetch-result tool-schemas tool-map heap)
          nudge-opts (tl/normalize-nudges nudges nil)
          status! (fn [phase & args] (apply notify-status! status-cb phase args))]
      (loop [msgs messages turn 0 nudge-state (gr/make-state) last-tools []]
        (if (>= turn max-turns)
          (do (status! :max-turns)
              (emit! events> {:type :phase/max-turns :turn turn :dialogue-id dialogue-id})
              "⚠️ Достигнут лимит ходов. Уточните запрос — например, добавьте цену или модель.")
          (let [_ (status! (if (zero? turn) :starting :after-tool))
                _ (when (zero? turn) (emit! events> {:type :phase/starting :dialogue-id dialogue-id}))
                ;; Drain steering queue — inject corrections before this LLM call
                steering-msgs (tl/drain-steering-queue nudge-state)
                inject-msgs (if (seq steering-msgs)
                              (into msgs steering-msgs)
                              msgs)
                llm-t0 (System/nanoTime)
                resp (consume-stream
                      (llm-stream :model (llm/resolve-model model)
                                  :messages inject-msgs :tools tool-schemas
                                  :provider provider :max-tokens max-tokens)
                      (fn [delta]
                        (stream-cb delta)
                        (emit! events> {:type :text/delta :text delta :dialogue-id dialogue-id})))
                llm-latency-ms (int (/ (- (System/nanoTime) llm-t0) 1e6))
                _ (observe/record!
                   {:type :llm-call
                    :dialogue-id dialogue-id
                    :trace-id trace-id
                    :model (llm/resolve-model model)
                    :provider provider
                    :latency-ms llm-latency-ms
                    :prompt-tokens (get-in resp [:usage :prompt_tokens])
                    :completion-tokens (get-in resp [:usage :completion_tokens])
                    :total-tokens (get-in resp [:usage :total_tokens])
                    :stream? true
                    :turn turn})
                content (:content resp)
                cfg (tl/guardrail-config tool-map nudge-opts nudge-state)
                checked (when nudge-opts (gr/check-response nudge-state cfg resp))
                calls (if nudge-opts
                        (mapv :raw (:tool-calls checked))
                        (:tool-calls resp))
                _ (log/info :stream-turn turn :content-len (count content) :calls-len (count calls))
                _ (when (seq calls)
                    (log/info :stream-tool-call (gr/tool-call-name (first calls))))]
            (case (:action checked :disabled)
              :text content
              :fatal (do
                       (emit! events> {:type :error/fatal :turn turn :dialogue-id dialogue-id :reason (:reason checked)})
                       (observe/record!
                        {:type :error :dialogue-id dialogue-id
                         :turn turn :error (str (:reason checked))})
                       (str "⚠️ " (:reason checked)))
              :retry (do
                       (status! :retry)
                       (emit! events> {:type :phase/retry :turn turn :dialogue-id dialogue-id :reason (:nudge checked)})
                       (observe/record!
                        {:type :nudge :dialogue-id dialogue-id
                         :turn turn :kind "retry" :reason (:nudge checked)})
                       (recur (conj msgs (tl/nudge-message (:nudge checked)))
                              (inc turn)
                              (:state checked)
                              last-tools))
              :step-blocked (do
                              (status! :retry)
                              (emit! events> {:type :phase/retry :turn turn :dialogue-id dialogue-id :reason (:nudge checked) :kind :step-blocked})
                              (observe/record!
                               {:type :nudge :dialogue-id dialogue-id
                                :turn turn :kind "step-blocked" :reason (:nudge checked)})
                              (recur (conj msgs (tl/nudge-message (:nudge checked)))
                                     (inc turn)
                                     (:state checked)
                                     last-tools))
              (:execute :disabled)
              (if (seq calls)
                ;; Force final response when near max-turns — don't let the agent
                ;; burn all turns on tool calls with no text response.
                ;; This also takes priority over the repeated-tool-abort guard:
                ;; if the agent is near max-turns AND repeating tools, we still
                ;; try to get a text response instead of just showing an error.
                (let [tool-name (gr/tool-call-name (first calls))
                      last-tools' (conj last-tools tool-name)
                      recent-same (take-last max-repeated-tool-calls last-tools')
                      all-same? (and (= (count recent-same) max-repeated-tool-calls)
                                     (apply = recent-same))
                      near-max? (>= turn (- max-turns 2))]
                  (cond
                    ;; Near max-turns: force text response regardless of loops
                    near-max?
                    (let [force-msg {"role" "system"
                                     "content" (str "У тебя остался 1 ход. НЕ ВЫЗЫВАЙ больше инструменты. "
                                                    "Немедленно напиши ответ пользователю на основе уже найденных данных. "
                                                    "Если данных мало — извинись и предложи уточнить запрос.")}
                          force-resp (consume-stream
                                      (llm-stream :model (llm/resolve-model model)
                                                  :messages (conj inject-msgs force-msg) :tools []
                                                  :provider provider :max-tokens max-tokens)
                                      (fn [delta]
                                        (stream-cb delta)
                                        (emit! events> {:type :text/delta :text delta :dialogue-id dialogue-id})))]
                      (log/info :force-final-response :turn turn)
                      (:content force-resp))

                    ;; Repeated tool loop (not near max-turns): force text response
                    ;; instead of showing an error — the agent has data, just needs
                    ;; to write the response
                    all-same?
                    (let [force-msg {"role" "system"
                                     "content" (str "Ты вызвал «" tool-name "» " max-repeated-tool-calls
                                                    " раз подряд. Хватит искать — у тебя уже есть результаты. "
                                                    "НЕМЕДЛЕННО напиши ответ пользователю на основе найденных данных.")}
                          force-resp (consume-stream
                                      (llm-stream :model (llm/resolve-model model)
                                                  :messages (conj inject-msgs force-msg) :tools []
                                                  :provider provider :max-tokens max-tokens)
                                      (fn [delta]
                                        (stream-cb delta)
                                        (emit! events> {:type :text/delta :text delta :dialogue-id dialogue-id})))]
                      (log/warn :repeated-tool-force-response :tool tool-name :count max-repeated-tool-calls :turn turn)
                      (:content force-resp))

                    ;; Normal tool execution
                    :else
                      (let [_ (status! :tool-call :tool-name tool-name)
                            _ (emit! events> {:type :tool/start :turn turn :dialogue-id dialogue-id :tool-name tool-name})
                            normalized-calls (if nudge-opts
                                               (:tool-calls checked)
                                               (mapv tl/loose-normalize-tool-call calls))
                            results (mapv (fn [call]
                                            (let [t0 (System/currentTimeMillis)
                                                  r (tl/execute-tool-call tool-map nil heap call
                                                                          abort-signal nil)
                                                  elapsed (- (System/currentTimeMillis) t0)
                                                  ok? (if (map? (:message r))
                                                        (not (:error (:message r)))
                                                        true)]
                                              (emit! events> {:type :tool/end :turn turn :dialogue-id dialogue-id
                                                              :tool-name (tl/tool-name call) :ok? ok? :elapsed elapsed})
                                              (observe/record!
                                               {:type :tool :dialogue-id dialogue-id
                                                :turn turn
                                                :name (tl/tool-name call)
                                                :ok? ok?
                                                :elapsed elapsed})
                                              r))
                                          normalized-calls)
                            nudge-state' (tl/next-state nudge-state nudge-opts results)
                            tool-results (mapv :message results)
                            asst-msg (cond-> {"role" "assistant"
                                              "tool_calls" (mapv tl/raw-call->api calls)}
                                       (not (str/blank? content))
                                       (assoc "content" content))]
                        (recur (into (conj msgs asst-msg) tool-results)
                               (inc turn)
                               nudge-state'
                               last-tools'))))
                ;; No tool calls — final response
                (do (emit! events> {:type :phase/done :turn turn :dialogue-id dialogue-id})
                    (if (str/blank? content)
                      ;; Empty response with no tool calls — retry once with nudge
                      (let [nudge-msg {"role" "system"
                                       "content" "Твой ответ пустой. Напиши подробный ответ пользователю на основе найденных данных. Не возвращай пустой ответ."}
                            retry-resp (consume-stream
                                        (llm-stream :model (llm/resolve-model model)
                                                    :messages (conj inject-msgs nudge-msg) :tools []
                                                    :provider provider :max-tokens max-tokens)
                                        (fn [delta]
                                          (stream-cb delta)
                                          (emit! events> {:type :text/delta :text delta :dialogue-id dialogue-id})))]
                        (log/info :empty-response-retry :turn turn)
                        (:content retry-resp))
                      content))))))))))

(comment
  ;; Usage example:
  (def ch (llm-stream
           :model "deepseek-v4-pro"
           :messages [{"role" "user" "content" "Say hello in Russian"}]
           :provider :deepseek))

  (loop [msg (<!! ch)]
    (when msg
      (when (:delta msg) (print (:delta msg)) (flush))
      (when-not (:done msg) (recur (<!! ch)))))

  ;; With stream-agent:
  (def buf (StringBuilder.))
  (stream-agent
   :model :deepseek-v4-pro
   :messages [{"role" "user" "content" "Say hello"}]
   :tool-map {}
   :tool-schemas []
   :stream-cb (fn [delta] (.append buf delta))
   :provider :deepseek
   :max-turns 2))
