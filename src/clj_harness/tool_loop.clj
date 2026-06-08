(ns clj-harness.tool-loop
  "Shared tool-loop helpers used by sync middleware and streaming agents."
  (:require
   [clj-harness.guardrails :as gr]
   [clj-harness.heap :as heap]
   [clj-harness.infra :refer [cfg]]
   [clojure.string :as str]))

(defn tool-name [t]
  (or (get t "name") (:name t)))

(defn marked-tool-names [tools pred]
  (->> tools
       (filter pred)
       (map tool-name)
       (remove str/blank?)
       vec))

(defn normalize-nudges
  "Normalize public :nudges option. `true` means full nudges; `false` disables."
  [nudges _tools]
  (cond
    (false? nudges) nil
    (nil? nudges) {:enabled? true :recover-tool-errors? true}
    (true? nudges) {:enabled? true :recover-tool-errors? true}
    (map? nudges) (merge {:enabled? true :recover-tool-errors? true} nudges)
    :else {:enabled? true :recover-tool-errors? true}))

(defn infer-nudges [nudges tools]
  (when-let [opts (normalize-nudges nudges tools)]
    (let [required (or (:required-steps opts)
                       (marked-tool-names tools #(or (:required-step? %)
                                                     (:required-step %)
                                                     (get % "required_step"))))
          terminal (or (:terminal-tools opts)
                       (marked-tool-names tools #(or (:terminal? %)
                                                     (:terminal %)
                                                     (get % "terminal"))))]
      (assoc opts
             :required-steps (vec required)
             :terminal-tools (gr/as-set terminal)))))

(def fetch-result-schema
  {"type" "function"
   "function" {"name" "fetch_result"
               "description" "Get full details from a previously stored tool result. Use this when you need to see specific items from a large search result that was stored in the heap."
               "parameters" {"type" "object"
                             "properties" {"heap_id" {"type" "string"
                                                      "description" "The heap ID reference from a previous tool result (e.g. 3)"}
                                           "query" {"type" "string"
                                                    "description" "Optional: filter results matching this query"}}
                             "required" ["heap_id"]}}})

(defn fetch-result-tool [heap-atom]
  {:name "fetch_result"
   :description "Get full details from a previously stored tool result"
   :execute (fn [args]
              (let [hid (get args "heap_id")
                    q (get args "query")]
                (if q
                  (heap/fetch-with-query heap-atom hid q)
                  (or (heap/fetch heap-atom hid)
                      (str "Heap entry " hid " not found or expired.")))))})

(defn with-fetch-result [tool-schemas tool-map heap-atom]
  (if heap-atom
    {:tool-schemas (conj (or tool-schemas []) fetch-result-schema)
     :tool-map (assoc (or tool-map {}) "fetch_result" (fetch-result-tool heap-atom))}
    {:tool-schemas (or tool-schemas [])
     :tool-map (or tool-map {})}))

(defn guardrail-config [tool-map nudge-opts nudge-state]
  (when nudge-opts
    {:tool-names (keys tool-map)
     :required-steps (:required-steps nudge-opts)
     :terminal-tools (:terminal-tools nudge-opts)
     :max-retries (:max-retries nudge-opts 3)
     :max-step-blocks (:max-step-blocks nudge-opts 3)
     :require-tool? (or (:require-tool? nudge-opts)
                        (seq (gr/pending-steps nudge-state (:required-steps nudge-opts))))}))

(defn format-tool-output
  ([heap-atom tool-name result-str]
   (format-tool-output heap-atom tool-name result-str (or (cfg :agent :max-tool-output) 8000)))
  ([heap-atom tool-name result-str max-out]
   (if-let [heap-ref (when heap-atom (heap/store! heap-atom tool-name result-str))]
     (str (heap/extract-key-items result-str)
          "\n\n📦 Stored in heap: " (:heap-id heap-ref)
          " (" (:size heap-ref) " chars)."
          " Use fetch_result to get full details.")
     (if (> (count result-str) max-out)
       (str (subs result-str 0 max-out) "\n...(truncated)")
       result-str))))

(defn result-content [result]
  (cond
    (and (map? result) (contains? result :content)) (:content result)
    (and (map? result) (contains? result "content")) (get result "content")
    :else result))

(defn result-ok? [result]
  (cond
    (and (map? result) (contains? result :ok?)) (:ok? result)
    (and (map? result) (contains? result "ok")) (get result "ok")
    :else true))

(defn loose-normalize-tool-call [tc]
  (let [parsed (gr/parse-args (gr/tool-call-raw-args tc))]
    {:id (or (get tc "id") (:id tc) (gr/tool-call-name tc))
     :name (gr/tool-call-name tc)
     :args (if (:ok? parsed) (:args parsed) {})
     :raw tc}))

(defn execute-tool-call
  "Execute a tool call. Supports two tool signatures:
     (fn [args]) -> result              ;; legacy (1 arity)
     (fn [args abort-signal on-update]) -> result  ;; new (3 arity)
   abort-signal: atom, set to true to cancel
   on-update: (fn [partial]) for progress callbacks"
  ([tool-map tool-post-process heap-atom tool-call]
   (execute-tool-call tool-map tool-post-process heap-atom tool-call nil nil))
  ([tool-map tool-post-process heap-atom tool-call abort-signal on-update]
   (let [{:keys [id name args]} tool-call
         tool (get tool-map name)
         execute-fn (:execute tool)
         result (if tool
                  (try
                    ;; Try 3-arg first, fall back to 1-arg
                    (let [r (if (and abort-signal on-update)
                              (try (execute-fn args abort-signal on-update)
                                   (catch clojure.lang.ArityException _
                                     (execute-fn args)))
                              (execute-fn args))]
                      r)
                    (catch Exception e
                      {:ok? false :content (str "Tool error: " (.getMessage e))}))
                  {:ok? false :content (str "Unknown tool: " name)})
         enriched (if tool-post-process
                    (try (tool-post-process name result)
                         (catch Exception _ result))
                    result)
         result-str (str (result-content enriched))]
     {:tool name
      :ok? (boolean (and tool (result-ok? enriched)))
      :message {"role" "tool"
                "tool_call_id" id
                "content" (format-tool-output heap-atom name result-str)}
      :structured (when (and (map? enriched) (:structured enriched))
                    (:structured enriched))})))

(defn next-state [nudge-state nudge-opts results]
  (if nudge-opts
    (let [successful (if (:recover-tool-errors? nudge-opts)
                       (->> results (filter :ok?) (map :tool))
                       (map :tool results))]
      (gr/record-executed nudge-state successful))
    nudge-state))

(defn nudge-message [{:keys [content]}]
  {"role" "user" "content" content})

(defn drain-steering-queue
  "Drain steering queue messages and convert to LLM-injectable user messages.
   Returns vector of message maps to prepend before the next LLM call.
   These are NOT stored in conversation history — only injected for this turn.
   Call at turn boundaries before building the LLM request."
  [nudge-state]
  (when-let [drained (gr/steering-drain! nudge-state)]
    (mapv nudge-message drained)))

(defn raw-call->api [tc]
  {"id" (or (:id tc) (get tc "id") (gr/tool-call-name tc))
   "type" "function"
   "function" {"name" (gr/tool-call-name tc)
               "arguments" (or (get-in tc [:function :arguments])
                               (get-in tc ["function" "arguments"])
                               "")}})
