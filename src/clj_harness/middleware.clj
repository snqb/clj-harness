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
   [clj-harness.heap :as heap]
   [clj-harness.infra :as infra :refer [cfg]]
   [clj-harness.mcp :as mcp]))

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

   Tool defs:
     {:name \"search\" :description \"...\" :schema {...} :execute (fn [args] \"result\")}
     {:mcp true :name \"get_weather\" ...}  ;; auto-resolved from MCPvisor

   When :heap is present in ctx, large tool outputs (>2K chars) are stored
   externally and replaced with compact summaries + heap-id references.
   fetch_result tool is auto-injected to allow LLM to retrieve full results.

   Feeds tool results back to the handler (LLM) until it produces a text response
   or hits max-turns. Returns {:content ... :tool-calls nil} when done."
  ([handler tools] (wrap-tools handler tools nil))
  ([handler tools tool-post-process]
   (let [_tool-map (into {} (map (fn [t] [(get t "name" (:name t)) t]) tools))
         _tool-schemas (mapv tool->openai-schema tools)]
     (fn [{:keys [messages max-turns heap] :as ctx}]
       (let [mt (or max-turns (cfg :agent :max-turns) 10)
             ;; Auto-add fetch_result when heap is active
             tool-schemas (if heap
                            (conj _tool-schemas
                                  {"type" "function"
                                   "function" {"name" "fetch_result"
                                               "description" "Get full details from a previously stored tool result. Use this when you need to see specific items from a large search result that was stored in the heap."
                                               "parameters" {"type" "object"
                                                             "properties" {"heap_id" {"type" "string"
                                                                                      "description" "The heap ID reference from a previous tool result (e.g. heap:abc123)"}
                                                                           "query" {"type" "string"
                                                                                    "description" "Optional: filter results matching this query"}}
                                                             "required" ["heap_id"]}}})
                            _tool-schemas)
             tool-map (if heap
                        (assoc _tool-map
                               "fetch_result"
                               {:name "fetch_result"
                                :description "Get full details from a previously stored tool result"
                                :execute (fn [args]
                                           (let [hid (get args "heap_id")
                                                 q (get args "query")]
                                             (if q
                                               (heap/fetch-with-query heap hid q)
                                               (or (heap/fetch heap hid)
                                                   (str "Heap entry " hid " not found or expired.")))))})
                        _tool-map)]
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
                                     enriched (if tool-post-process
                                                (try (tool-post-process tn result)
                                                     (catch Exception _ result))
                                                result)
                                     result-str (str enriched)
                                     ;; Heap storage: if heap active and result > 2K chars
                                     heap-ref (when heap
                                                (heap/store! heap tn result-str))
                                     fmt-result (if heap-ref
                                                  (str (heap/extract-key-items result-str)
                                                       "\n\n📦 Stored in heap: " (:heap-id heap-ref)
                                                       " (" (:size heap-ref) " chars)."
                                                       " Use fetch_result to get full details.")
                                                  ;; No heap: truncate to 8K as before
                                                  (let [max-out (or (cfg :agent :max-tool-output) 8000)]
                                                    (if (> (count result-str) max-out)
                                                      (str (subs result-str 0 max-out)
                                                           "\n...(truncated)")
                                                      result-str)))]
                                 {"role" "tool"
                                  "tool_call_id" (get tc "id")
                                  "content" fmt-result}))
                             calls)]
                   (recur (into (conj msgs {"role" "assistant" "content" (:content resp) "tool_calls" calls})
                                tool-results)
                          (inc turn)))
                 resp)))))))))

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
