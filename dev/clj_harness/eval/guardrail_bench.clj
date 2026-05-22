(ns clj-harness.eval.guardrail-bench
  "Deterministic guardrail benchmark for clj-harness.

  This does not claim model quality. It measures harness behavior: given a model
  that can self-correct after a nudge, which failure modes does each harness mode
  recover from, and how many extra calls did recovery cost?"
  (:gen-class)
  (:require
   [cheshire.core :as json]
   [clj-harness.guardrails :as gr]
   [clojure.java.io :as io]
   [clojure.string :as str]))

(defn tc
  "OpenAI-style tool call fixture."
  [name args]
  {"id" (str "call_" name "_" (abs (hash [name args])))
   "function" {"name" name
               "arguments" (json/generate-string args)}})

(defn bad-tc [name raw-args]
  {"id" (str "bad_" name)
   "function" {"name" name "arguments" raw-args}})

(def tools
  {"search" (fn [{:strs [query]}]
              (if (= query "missing")
                {:ok? false :kind :not-found :content "No matching data found."}
                {:ok? true :content (str "Found data for " query)}))
   "lookup" (fn [{:strs [id]}]
              {:ok? true :content (str "Details for " id)})
   "answer" (fn [{:strs [text]}]
              {:ok? true :content text})})

(def scenarios
  [{:id :simple-tool
    :description "Happy path: model calls required tool, then terminal tool."
    :required-steps ["search"]
    :terminal-tools #{"answer"}
    :script [{:tool-calls [(tc "search" {"query" "q3"})]}
             {:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}]}
   {:id :text-instead-of-tool
    :description "Model narrates instead of calling a tool, then self-corrects after retry nudge."
    :required-steps ["search"]
    :terminal-tools #{"answer"}
    :script [{:content "I should search for Q3 first."}
             {:tool-calls [(tc "search" {"query" "q3"})]}
             {:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}]}
   {:id :unknown-tool
    :description "Model hallucinates a tool name, then uses the valid tool after nudge."
    :required-steps ["search"]
    :terminal-tools #{"answer"}
    :script [{:tool-calls [(tc "web_search" {"query" "q3"})]}
             {:tool-calls [(tc "search" {"query" "q3"})]}
             {:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}]}
   {:id :malformed-args
    :description "Model emits invalid JSON arguments, then fixes the call after nudge."
    :required-steps ["search"]
    :terminal-tools #{"answer"}
    :script [{:tool-calls [(bad-tc "search" "{query: q3}")]}
             {:tool-calls [(tc "search" {"query" "q3"})]}
             {:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}]}
   {:id :rescued-json-in-text
    :description "Model wraps a valid tool call in prose; rescue parsing recovers it."
    :required-steps ["search"]
    :terminal-tools #{"answer"}
    :script [{:content "Sure, I'll do it: {\"tool\":\"search\",\"args\":{\"query\":\"q3\"}}"}
             {:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}]}
   {:id :premature-terminal
    :description "Model jumps to answer before required search; full guardrails block it."
    :required-steps ["search"]
    :terminal-tools #{"answer"}
    :script [{:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}
             {:tool-calls [(tc "search" {"query" "q3"})]}
             {:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}]}
   {:id :soft-tool-error
    :description "Tool succeeds mechanically but finds no data; full guardrails keep the step open."
    :required-steps ["search"]
    :terminal-tools #{"answer"}
    :script [{:tool-calls [(tc "search" {"query" "missing"})]}
             {:tool-calls [(tc "answer" {"text" "No data found"})]}
             {:tool-calls [(tc "search" {"query" "q3"})]}
             {:tool-calls [(tc "answer" {"text" "Q3 looks good"})]}]}])

(def modes
  {:baseline {:guardrails? false}
   :validate-only {:guardrails? true :enforce-steps? false :recover-tool-errors? false}
   ;; Public name: "nudges" means the full Forge-style stack.
   :nudges {:guardrails? true :enforce-steps? true :recover-tool-errors? true}})

(defn- execute-call [tool-map {:keys [name args]}]
  (if-let [f (get tool-map name)]
    (try
      (merge {:tool name :args args} (f args))
      (catch Exception e
        {:tool name :args args :ok? false :kind :exception :content (.getMessage e)}))
    {:tool name :args args :ok? false :kind :unknown-tool :content (str "Unknown tool: " name)}))

(defn- successful-tool-names [results]
  (->> results
       (filter :ok?)
       (map :tool)))

(defn- terminal-result? [scenario results]
  (let [terminal-set (:terminal-tools scenario)]
    (some #(contains? terminal-set (:tool %)) results)))

(defn- success? [scenario trace]
  (let [tool-results (filter #(= :tool-result (:event %)) trace)
        required (set (:required-steps scenario))
        terminal-set (:terminal-tools scenario)
        terminal-index (first (keep-indexed (fn [idx event]
                                              (when (and (= :tool-result (:event event))
                                                         (contains? terminal-set (:tool event)))
                                                idx))
                                            trace))
        successful-before-terminal (->> trace
                                        (take (or terminal-index (count trace)))
                                        (filter #(and (= :tool-result (:event %)) (:ok? %)))
                                        (map :tool)
                                        set)]
    (boolean
     (and terminal-index
          (not-any? #(= :fatal (:event %)) trace)
          (every? successful-before-terminal required)
          (some #(and (= :tool-result (:event %))
                      (contains? terminal-set (:tool %))
                      (:ok? %))
                tool-results)))))

(defn- mode-config [scenario mode]
  (let [mode-settings (get modes mode)]
    {:tool-names (keys tools)
     :required-steps (when (:enforce-steps? mode-settings)
                       (:required-steps scenario))
     :terminal-tools (:terminal-tools scenario)
     :max-retries 2
     :max-step-blocks 2
     :require-tool? true}))

(defn- run-baseline-step [scenario response trace]
  (let [normalized (gr/normalize-tool-calls (:tool-calls response))]
    (cond
      (not (seq (:tool-calls response)))
      {:done? true
       :trace (conj trace {:event :text-response :content (:content response)})}

      (not (:ok? normalized))
      {:done? true
       :trace (conj trace {:event :fatal :reason (:reason normalized)})}

      :else
      (let [results (mapv #(execute-call tools %) (:tool-calls normalized))
            trace' (into trace (map #(assoc % :event :tool-result) results))]
        {:done? (terminal-result? scenario results)
         :trace trace'}))))

(defn- run-guarded-step [scenario mode state response trace]
  (let [mode-settings (get modes mode)
        check-fn (if (:enforce-steps? mode-settings)
                   gr/check-response
                   (fn [s cfg resp] (gr/validate-response s cfg resp)))
        checked (check-fn state (mode-config scenario mode) response)]
    (case (:action checked)
      :fatal
      {:done? true
       :state (:state checked)
       :trace (conj trace {:event :fatal :reason (:reason checked)})}

      :retry
      {:state (:state checked)
       :trace (conj trace (assoc (:nudge checked) :event :nudge))}

      :step-blocked
      {:state (:state checked)
       :trace (conj trace (assoc (:nudge checked) :event :nudge))}

      :execute
      (let [results (mapv #(execute-call tools %) (:tool-calls checked))
            ok-tools (successful-tool-names results)
            state' (gr/record-executed (:state checked) ok-tools)
            trace' (into trace (map #(assoc % :event :tool-result) results))
            has-error? (some (complement :ok?) results)
            terminal? (terminal-result? scenario results)]
        {:done? (and terminal? (not (and has-error? (:recover-tool-errors? mode-settings))))
         :state state'
         :trace trace'}))))

(defn run-scenario [mode scenario]
  (loop [idx 0
         state (gr/make-state)
         trace []]
    (if-let [response (nth (:script scenario) idx nil)]
      (let [trace' (conj trace {:event :model-response :idx idx})
            result (if (:guardrails? (get modes mode))
                     (run-guarded-step scenario mode state response trace')
                     (run-baseline-step scenario response trace'))]
        (if (:done? result)
          (let [final-trace (:trace result)]
            {:scenario (:id scenario)
             :mode mode
             :success? (success? scenario final-trace)
             :llm-calls (inc idx)
             :nudges (count (filter #(= :nudge (:event %)) final-trace))
             :tool-results (count (filter #(= :tool-result (:event %)) final-trace))
             :trace final-trace})
          (recur (inc idx) (or (:state result) state) (:trace result))))
      {:scenario (:id scenario)
       :mode mode
       :success? false
       :llm-calls idx
       :nudges (count (filter #(= :nudge (:event %)) trace))
       :tool-results (count (filter #(= :tool-result (:event %)) trace))
       :trace (conj trace {:event :fatal :reason "script exhausted"})})))

(defn run-benchmark []
  (vec (for [scenario scenarios
             mode (keys modes)]
         (run-scenario mode scenario))))

(defn summarize [rows]
  (->> rows
       (group-by :mode)
       (map (fn [[mode xs]]
              {:mode mode
               :scenarios (count xs)
               :successes (count (filter :success? xs))
               :success-rate (double (/ (count (filter :success? xs)) (count xs)))
               :llm-calls (reduce + (map :llm-calls xs))
               :nudges (reduce + (map :nudges xs))}))
       (sort-by :mode)
       vec))

(defn default-output-path []
  (let [dir (io/file ".git" "reports")]
    (.mkdirs dir)
    (str (.getPath dir) "/guardrail-bench-" (System/currentTimeMillis) ".jsonl")))

(defn write-jsonl! [path rows]
  (with-open [w (io/writer path)]
    (doseq [row rows]
      (.write w (json/generate-string row))
      (.write w "\n")))
  path)

(defn file-uri [path]
  (str "file://" (.getAbsolutePath (io/file path))))

(defn- parse-args [args]
  (loop [xs args
         opts {}]
    (if-let [k (first xs)]
      (recur (nnext xs) (assoc opts k (second xs)))
      opts)))

(defn print-summary! [summary]
  (println "mode           success  calls  nudges")
  (println "-------------  -------  -----  ------")
  (doseq [{:keys [mode scenarios successes llm-calls nudges]} summary]
    (println (format "%-13s  %d/%d      %d      %d"
                     (name mode) successes scenarios llm-calls nudges))))

(defn -main [& args]
  (let [opts (parse-args args)
        out (or (get opts "--out") (default-output-path))
        rows (run-benchmark)
        summary (summarize rows)]
    (write-jsonl! out rows)
    (print-summary! summary)
    (println "\nWrote JSONL:" (file-uri out))))
