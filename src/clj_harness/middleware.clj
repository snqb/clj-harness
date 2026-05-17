(ns clj-harness.middleware
  "Middleware stack — composable functions that wrap the core agent handler.

   Each middleware = (fn [handler] => handler). They form a pipeline:
     core-agent → wrap-tools → wrap-retry → wrap-logging

   Key insight: middleware only knows about its own tools and the handler contract,
   not about the rest of the stack. wrap-tools receives the handler as a parameter
   and never calls llm or core-agent directly."
  (:require
   [cheshire.core :as json]
   [clojure.tools.logging :as log]
   [clj-harness.infra :as infra :refer [cfg]]
   [clj-harness.mcp :as mcp]))

;; ══════════════════════ TOOL LOOP ══════════════════════

(defn wrap-tools
  "Middleware: automatic tool calling loop.

   Tool defs:
     {:name \"search\" :description \"...\" :schema {...} :execute (fn [args] \"result\")}
     {:mcp true :name \"get_weather\" ...}  ;; auto-resolved from MCPvisor

   Feeds tool results back to the handler (LLM) until it produces a text response
   or hits max-turns. Returns {:content ... :tool-calls nil} when done."
  [handler tools]
  (let [tool-map (into {} (map (fn [t] [(get t "name" (:name t)) t]) tools))
        tool-schemas (mapv (fn [t]
                             (if (:mcp t)
                               (mcp/mcp-tool->openai-schema t)
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
