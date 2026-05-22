(ns clj-harness.middleware
  "Middleware stack — composable functions that wrap the core agent handler.

   Each middleware = (fn [handler] => handler). They form a pipeline:
     core-agent → wrap-tools → wrap-retry → wrap-logging

   Key insight: middleware only knows about its own tools and the handler contract,
   not about the rest of the stack. wrap-tools receives the handler as a parameter
   and never calls llm or core-agent directly."
  (:require
   [clojure.tools.logging :as log]
   [clj-harness.guardrails :as gr]
   [clj-harness.infra :as infra :refer [cfg]]
   [clj-harness.mcp :as mcp]
   [clj-harness.tool-loop :as tl]))

;; ══════════════════════ TOOL LOOP ══════════════════════

(defn tool->openai-schema
  "Convert a harness tool definition to an OpenAI function-calling schema."
  [t]
  (if (:mcp t)
    (mcp/mcp-tool->openai-schema t)
    {"type" "function"
     "function" {"name" (:name t)
                 "description" (:description t "")
                 "parameters" (or (:schema t) {"type" "object" "properties" {}})}}))

(defn wrap-tools
  "Middleware: automatic tool calling loop.

   Tool defs look like maps with :name, :description, :schema, and :execute.
   MCP tools use the same shape plus :mcp true.

   :nudges enables Forge-style full guardrails under a simpler name:
     {:required-steps [search] :terminal-tools #{answer}}

   When :heap is present in ctx, large tool outputs (>2K chars) are stored
   externally and replaced with compact summaries + heap-id references.
   fetch_result tool is auto-injected to allow LLM to retrieve full results.

   Feeds tool results back to the handler (LLM) until it produces a text response
   or hits max-turns. Returns {:content ... :tool-calls nil} when done."
  ([handler tools] (wrap-tools handler tools nil true))
  ([handler tools tool-post-process] (wrap-tools handler tools tool-post-process true))
  ([handler tools tool-post-process default-nudges]
   (let [_tool-map (into {} (map (fn [t] [(tl/tool-name t) t]) tools))
         _tool-schemas (mapv tool->openai-schema tools)]
     (fn [{:keys [messages max-turns heap nudges] :as ctx}]
       (let [mt (or max-turns (cfg :agent :max-turns) 10)
             {:keys [tool-schemas tool-map]} (tl/with-fetch-result _tool-schemas _tool-map heap)
             nudge-opts (tl/infer-nudges (if (contains? ctx :nudges) nudges default-nudges) tools)]
         (loop [msgs messages turn 0 nudge-state (gr/make-state)]
           (if (>= turn mt)
             {:content (str "⚠️ Reached max turns (" mt "). Try a more specific query.")
              :tool-calls nil}
             (let [resp (handler (assoc ctx :messages msgs :tools tool-schemas))
                   cfg (tl/guardrail-config tool-map nudge-opts nudge-state)
                   checked (when nudge-opts (gr/check-response nudge-state cfg resp))]
               (if (#{:retry :step-blocked} (:action checked))
                 (recur (conj msgs (tl/nudge-message (:nudge checked)))
                        (inc turn)
                        (:state checked))
                 (case (:action checked :disabled)
                   :text resp
                   :fatal {:content (str "⚠️ " (:reason checked)) :tool-calls nil}
                   (:execute :disabled)
                   (if-let [calls (if nudge-opts
                                    (mapv :raw (:tool-calls checked))
                                    (:tool-calls resp))]
                     (let [normalized-calls (if nudge-opts
                                              (:tool-calls checked)
                                              (mapv tl/loose-normalize-tool-call calls))
                           results (mapv #(tl/execute-tool-call tool-map tool-post-process heap %)
                                         normalized-calls)
                           nudge-state' (tl/next-state nudge-state nudge-opts results)
                           tool-results (mapv :message results)]
                       (recur (into (conj msgs (cond-> {"role" "assistant"
                                                        "content" (:content resp)
                                                        "tool_calls" calls}
                                                 (:reasoning-content resp)
                                                 (assoc "reasoning_content" (:reasoning-content resp))))
                                    tool-results)
                              (inc turn)
                              nudge-state'))
                     resp)))))))))))

;; ══════════════════════ RETRY ══════════════════════

(defn wrap-retry
  "Middleware: retry on exception with exponential backoff.

   (wrap-retry handler)      ;; up to 2 retries (default)
   (wrap-retry handler 3)    ;; up to 3 retries"
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

;; ══════════════════════ LOGGING ══════════════════════

(defn wrap-logging
  "Middleware: log timing and finish reason for each turn.
   Pure telemetry — doesn't modify the response."
  [handler]
  (fn [ctx]
    (let [t0 (System/currentTimeMillis)
          resp (handler ctx)]
      (log/info :turn-complete :msgs (count (:messages ctx))
                :finish (:finish resp)
                :elapsed (- (System/currentTimeMillis) t0))
      resp)))
