(ns clj-harness.llm
  "LLM client — provider dispatch, API calls, core agent handler.

   Supported providers: :openrouter (default), :deepseek.
   Provider config is data, not code — add new providers by adding a map entry.

   core-agent is the base middleware handler: raw LLM call, no tool iteration.
   It wraps llm and normalizes the response to {:content :tool-calls :finish :usage}.

   Observability: every LLM call emits an :llm-call event via observe/record!
   with model, latency, and token usage (when available from the provider)."
  (:require
   [cheshire.core :as json]
   [clojure.tools.logging :as log]
   [clj-harness.infra :as infra :refer [cfg]]
   [clj-harness.observe :as observe]))

;; ══════════════════════ PROVIDER CONFIG ══════════════════════

(def ^:private provider-config
  "Data-driven provider dispatch. Each provider = {:url :headers}.
   :headers is a (fn [api-key] => map) to support different auth schemes."
  {:openrouter {:url (or (System/getenv "OPENROUTER_BASE_URL")
                         "https://openrouter.ai/api/v1/chat/completions")
                :headers (fn [key] {"Authorization" (str "Bearer " key)
                                    "HTTP-Referer" "http://localhost"
                                    "X-Title" "CljHarness"
                                    "User-Agent" "CljHarness/1.0"})}
   :deepseek   {:url "https://api.deepseek.com/chat/completions"
                :headers (fn [key] {"Authorization" (str "Bearer " key)})}})

(defn resolve-model
  "Resolve model key to actual model name.
   Checks config.edn :models map first, falls back to literal name."
  [model-key]
  (or (get (cfg :models) model-key) (name model-key)))

;; ══════════════════════ LLM CALL ══════════════════════

(defn llm
  "Call LLM via configured provider. Returns raw API response JSON.

   (llm :claude-sonnet [{:role \"user\" :content \"Hi\"}] [] :provider :openrouter)
   (llm :deepseek-v4-pro [{...}] [{:name \"search\" ...}] :provider :deepseek :max-tokens 2048)
   (llm :deepseek-v4-pro msgs tools :provider :deepseek :force-tool? true)"
  [model-key messages tools & {:keys [provider max-tokens force-tool?]
                               :or {provider :openrouter max-tokens 4096}}]
  (let [pc (get provider-config provider)
        url (:url pc)
        headers ((:headers pc) (infra/read-api-key provider))
        model-name (resolve-model model-key)
        tool-choice (cond
                      (true? force-tool?) "required"
                      (seq tools) "auto"
                      :else nil)
        payload (cond-> {"model" model-name "messages" messages "max_tokens" max-tokens}
                  (seq tools) (assoc "tools" tools)
                  tool-choice (assoc "tool_choice" tool-choice))]
    (log/info :llm-call :provider provider :model model-name :messages (count messages)
              :tools (count tools) :force-tool? force-tool?)
    (infra/http-post url (json/generate-string payload)
                     :headers headers :timeout-ms 180000)))

;; ══════════════════════ CORE AGENT ══════════════════════

(defn core-agent
  "Base middleware handler: raw LLM call with no tool iteration.
   Normalizes provider response to {:content :tool-calls :finish :usage}.

   This is the innermost handler in the middleware stack — it talks to the LLM,
   extracts the assistant message, and passes control back to the tool loop.

   Emits :llm-call observe events with timing + token usage for observability.
   This is the single chokepoint that ALL sync paths funnel through."
  [{:keys [model messages tools provider force-tool? dialogue-id trace-id]
    :or {provider :openrouter}}]
  (let [t0 (System/nanoTime)
        resp (llm model messages tools :provider provider :force-tool? force-tool?)
        choice (first (get resp "choices"))
        msg (get choice "message")
        usage (get resp "usage")
        latency-ms (int (/ (- (System/nanoTime) t0) 1e6))]
    (observe/record!
     {:type :llm-call
      :dialogue-id dialogue-id
      :trace-id trace-id
      :model (resolve-model model)
      :provider provider
      :latency-ms latency-ms
      :prompt-tokens (get usage "prompt_tokens")
      :completion-tokens (get usage "completion_tokens")
      :total-tokens (get usage "total_tokens")
      :stream? false})
    {:content (get msg "content")
     :reasoning-content (get msg "reasoning_content")
     :tool-calls (get msg "tool_calls")
     :finish (get choice "finish_reason")
     :usage usage}))
