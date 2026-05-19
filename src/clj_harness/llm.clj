(ns clj-harness.llm
  "LLM client — provider dispatch, API calls, core agent handler.

   Supported providers: :openrouter (default), :deepseek.
   Provider config is data, not code — add new providers by adding a map entry.

   core-agent is the base middleware handler: raw LLM call, no tool iteration.
   It wraps llm and normalizes the response to {:content :tool-calls :finish}."
  (:require
   [cheshire.core :as json]
   [clojure.tools.logging :as log]
   [clj-harness.infra :as infra :refer [cfg]]))

;; ══════════════════════ PROVIDER CONFIG ══════════════════════

(def ^:private provider-config
  "Data-driven provider dispatch. Each provider = {:url :headers}.
   :headers is a (fn [api-key] => map) to support different auth schemes."
  {:openrouter {:url "https://openrouter.ai/api/v1/chat/completions"
                :headers (fn [key] {"Authorization" (str "Bearer " key)
                                    "HTTP-Referer" "http://localhost"
                                    "X-Title" "CljHarness"})}
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
   (llm :deepseek-v4-pro [{...}] [{:name \"search\" ...}] :provider :deepseek :max-tokens 2048)"
  [model-key messages tools & {:keys [provider max-tokens]
                               :or {provider :openrouter max-tokens 4096}}]
  (let [pc (get provider-config provider)
        url (:url pc)
        headers ((:headers pc) (infra/read-api-key provider))
        model-name (resolve-model model-key)
        payload (cond-> {"model" model-name "messages" messages "max_tokens" max-tokens}
                  (seq tools) (assoc "tools" tools "tool_choice" "auto"))]
    (log/info :llm-call :provider provider :model model-name :messages (count messages)
              :tools (count tools))
    (infra/http-post url (json/generate-string payload)
                     :headers headers :timeout-ms 180000)))

;; ══════════════════════ CORE AGENT ══════════════════════

(defn core-agent
  "Base middleware handler: raw LLM call with no tool iteration.
   Normalizes provider response to {:content :tool-calls :finish}.

   This is the innermost handler in the middleware stack — it talks to the LLM,
   extracts the assistant message, and passes control back to the tool loop."
  [{:keys [model messages tools provider] :or {provider :openrouter}}]
  (let [resp (llm model messages tools :provider provider)
        choice (first (get resp "choices"))
        msg (get choice "message")]
    {:content (get msg "content")
     :tool-calls (get msg "tool_calls")
     :finish (get choice "finish_reason")}))
