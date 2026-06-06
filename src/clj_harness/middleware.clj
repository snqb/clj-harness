(ns clj-harness.middleware
  "Middleware stack — composable functions that wrap the core agent handler.

   Each middleware = (fn [handler] => handler). They form a pipeline:
     core-agent → wrap-tools → wrap-retry → wrap-logging

   Key insight: middleware only knows about its own tools and the handler contract,
   not about the rest of the stack. wrap-tools receives the handler as a parameter
   and never calls llm or core-agent directly."
  (:require
   [clojure.tools.logging :as log]
   [clojure.core.async :refer [chan sliding-buffer put! close!]]
   [clj-harness.agent-loop :as aloop]
   [clj-harness.effects :as fx]
   [clj-harness.guardrails :as gr]
   [clj-harness.infra :as infra :refer [cfg]]
   [clj-harness.mcp :as mcp]
   [clj-harness.observe :as observe]
   [clj-harness.tool-loop :as tl]
   [malli.json-schema :as mjs]))

;; ══════════════════════ TOOL LOOP ══════════════════════

(defn tool->openai-schema
  "Convert a harness tool definition to an OpenAI function-calling schema.
  :schema accepts JSON Schema maps, Malli forms, or nil (empty schema)."
  [t]
  (if (:mcp t)
    (mcp/mcp-tool->openai-schema t)
    (let [raw-schema (:schema t)
          params (cond
                   (nil? raw-schema) {"type" "object" "properties" {}}
                   (sequential? raw-schema) (try (mjs/transform raw-schema)
                                                 (catch Exception _
                                                   {"type" "object" "properties" {}}))
                   :else raw-schema)]
      {"type" "function"
       "function" {"name" (:name t)
                   "description" (:description t "")
                   "parameters" params}})))

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
             (let [force-tool? (when nudge-opts
                                 (seq (gr/pending-steps nudge-state (:required-steps nudge-opts))))
                   resp (handler (assoc ctx :messages msgs :tools tool-schemas
                                        :force-tool? force-tool?))
                   cfg (tl/guardrail-config tool-map nudge-opts nudge-state)
                   checked (when nudge-opts (gr/check-response nudge-state cfg resp))]
               (if (#{:retry :step-blocked} (:action checked))
                 (do
                   (observe/record!
                    {:type :nudge :dialogue-id (:dialogue-id ctx)
                     :turn turn
                     :kind (name (:action checked))
                     :reason (:nudge checked)})
                   (recur (conj msgs (tl/nudge-message (:nudge checked)))
                          (inc turn)
                          (:state checked)))
                 (case (:action checked :disabled)
                   :text resp
                   :fatal (do
                            (observe/record!
                             {:type :error :dialogue-id (:dialogue-id ctx)
                              :turn turn :error (str (:reason checked))})
                            {:content (str "⚠️ " (:reason checked)) :tool-calls nil})
                   (:execute :disabled)
                   (if-let [calls (if nudge-opts
                                    (mapv :raw (:tool-calls checked))
                                    (:tool-calls resp))]
                     (let [normalized-calls (if nudge-opts
                                              (:tool-calls checked)
                                              (mapv tl/loose-normalize-tool-call calls))
                           results (mapv (fn [call]
                                           (let [t0 (System/currentTimeMillis)
                                                 r (tl/execute-tool-call tool-map tool-post-process heap call)
                                                 elapsed (- (System/currentTimeMillis) t0)]
                                             (observe/record!
                                              {:type :tool :dialogue-id (:dialogue-id ctx)
                                               :turn turn
                                               :name (tl/tool-name call)
                                               :ok? (if (map? (:message r))
                                                      (not (:error (:message r)))
                                                      true)
                                               :elapsed elapsed})
                                             r))
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

;; ══════════════════════ EFFECT-DRIVEN TOOL LOOP (v2) ══════════════════════

(defn wrap-tools-v2
  "Middleware: effect-driven tool calling loop (experimental).

   Uses the pure state machine from clj-harness.agent-loop instead of
   the imperative loop/recur in wrap-tools. Tool execution and LLM calls
   are emitted as effect descriptions and interpreted by clj-harness.effects.

   Benefits over wrap-tools:
     - Pure state machine is testable without mocking
     - Event bus (events> channel) for streaming/observability
     - Same tool-loop semantics (nudges, max-turns, heap)

   Opt-in via create-bot {:effects? true}."
  ([handler tools] (wrap-tools-v2 handler tools nil true))
  ([handler tools tool-post-process] (wrap-tools-v2 handler tools tool-post-process true))
  ([handler tools tool-post-process default-nudges]
   (let [_tool-map (into {} (map (fn [t] [(tl/tool-name t) t]) tools))
         _tool-schemas (mapv tool->openai-schema tools)
         ;; Resolve defaults by calling handler once with empty messages
         ;; to see what model/provider it uses
         dummy-resp (try (handler {:messages [] :tools _tool-schemas})
                         (catch Exception _ {:content "" :tool-calls nil}))]
     (fn [{:keys [messages max-turns heap nudges events>] :as ctx}]
       (let [mt (or max-turns (cfg :agent :max-turns) 10)
             {:keys [tool-schemas tool-map]} (tl/with-fetch-result _tool-schemas _tool-map heap)
             nudge-opts (tl/infer-nudges (if (contains? ctx :nudges) nudges default-nudges) tools)
             own-events> (chan (sliding-buffer 64))
             events> (or events> own-events>)
             ;; Resolve model from ctx or config
             model (or (:model ctx)
                       (cfg :agent :model)
                       :deepseek-v4-pro)
             provider (or (:provider ctx)
                          (cfg :agent :provider)
                          :deepseek)
             ;; LLM wrapper — delegates to inner handler
             llm-fn (fn [model-key msgs ts & [opts]]
                      (handler (assoc ctx
                                      :messages msgs
                                      :tools ts
                                      :model model-key
                                      :provider provider
                                      :force-tool? (:force-tool? opts))))
             env {:llm-fn llm-fn
                  :tool-map tool-map
                  :tool-post-process tool-post-process
                  :heap heap
                  :events> events>
                  :abort-signal (:abort-signal ctx)}
             initial-state (aloop/make-state
                            {:messages messages
                             :tool-schemas tool-schemas
                             :tool-map tool-map
                             :max-turns mt
                             :model model
                             :provider provider
                             :nudge-opts nudge-opts
                             :max-tool-output (or (cfg :agent :max-tool-output) 8000)})
             result (aloop/run env initial-state)]
         (when-not (:events> ctx)
           (close! events>))
         result)))))
