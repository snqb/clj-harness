(ns clj-harness.guardrails
  "Small, tool-agnostic guardrail primitives for agent loops.

  These functions are intentionally pure/data-first so they can be used by
  clj-harness middleware, eval runners, or downstream bots without owning the
  whole orchestration loop. They validate tool-call shape, emit typed nudges,
  enforce required steps before terminal tools, and track retry/step budgets."
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]))

(def default-limits
  {:max-retries 3
   :max-step-blocks 3})

(defn as-set [x]
  (cond
    (nil? x) #{}
    (set? x) x
    (sequential? x) (set x)
    :else #{x}))

(defn make-state
  "Create guardrail state for one task/session. Keep this outside LLM context."
  ([] (make-state {}))
  ([{:keys [completed retry-count step-block-count]}]
   {:completed (as-set completed)
    :retry-count (or retry-count 0)
    :step-block-count (or step-block-count 0)}))

(defn nudge
  "Typed corrective message to inject into the conversation."
  ([kind content] (nudge kind content nil))
  ([kind content extra]
   (merge {:role "user" :kind kind :content content} extra)))

(defn retry-nudge [reason]
  (nudge :retry
         (str "Your previous response was not a valid tool call. "
              (when (seq reason) (str reason " "))
              "Respond with a valid tool call using one of the available tools.")))

(defn unknown-tool-nudge [tool-name tool-names]
  (nudge :unknown-tool
         (str "Tool '" tool-name "' does not exist. Available tools: "
              (str/join ", " (sort tool-names)) ". Call one of them.")))

(defn step-nudge [terminal-tool pending tier]
  (let [steps (str/join ", " pending)]
    (nudge :step
           (case (long (max 1 (min 3 tier)))
             1 (str "You cannot call " terminal-tool " yet. "
                    "First complete: " steps ".")
             2 (str "You must call one of these tools now: " steps ".")
             (str "STOP. Do not call " terminal-tool ". "
                  "Your next response must call one of: " steps "."))
           {:tier tier})))

(defn parse-args [raw]
  (cond
    (nil? raw) {:ok? true :args {}}
    (map? raw) {:ok? true :args raw}
    (string? raw) (try
                    {:ok? true :args (json/parse-string raw false)}
                    (catch Exception e
                      {:ok? false :error (.getMessage e)}))
    :else {:ok? false :error (str "Unsupported arguments value: " (pr-str raw))}))

(defn tool-call-name [tool-call]
  (or (get-in tool-call ["function" "name"])
      (get-in tool-call [:function :name])
      (get tool-call "name")
      (:name tool-call)
      (:tool tool-call)))

(defn tool-call-raw-args [tool-call]
  (or (get-in tool-call ["function" "arguments"])
      (get-in tool-call [:function :arguments])
      (get tool-call "arguments")
      (:arguments tool-call)
      (:args tool-call)))

(defn normalize-tool-call
  "Normalize OpenAI-style or simple Clojure tool call maps.
   Returns {:ok? true :tool-call {...}} or {:ok? false :reason ...}."
  [tool-call]
  (let [name (tool-call-name tool-call)
        parsed (parse-args (tool-call-raw-args tool-call))]
    (cond
      (str/blank? (str name))
      {:ok? false :reason "Tool call is missing a function name."}

      (not (:ok? parsed))
      {:ok? false :reason (str "Tool call arguments are invalid JSON: " (:error parsed))}

      :else
      {:ok? true
       :tool-call {:id (or (get tool-call "id") (:id tool-call) name)
                   :name name
                   :args (:args parsed)
                   :raw tool-call}})))

(defn normalize-tool-calls [tool-calls]
  (let [normalized (mapv normalize-tool-call tool-calls)
        bad (first (remove :ok? normalized))]
    (if bad
      bad
      {:ok? true :tool-calls (mapv :tool-call normalized)})))

(defn rescue-tool-calls
  "Best-effort rescue for text responses that contain a JSON-ish tool call.
   Supports maps with tool/name plus args/arguments fields."
  [content]
  (when-let [jsonish (and (string? content) (re-find #"(?s)\{.*\}" content))]
    (try
      (let [m (json/parse-string jsonish false)
            name (or (get m "tool") (get m "name"))
            args (or (get m "args") (get m "arguments") {})]
        (when name
          [{"id" "rescued_0"
            "function" {"name" name
                        "arguments" (json/generate-string args)}}]))
      (catch Exception _ nil))))

(defn- response-tool-calls [response]
  (or (:tool-calls response)
      (get response "tool_calls")
      (rescue-tool-calls (or (:content response) (get response "content")))))

(defn- bump-or-fatal [state counter limit fatal-reason retry-action]
  (let [n (inc (or (get state counter) 0))
        state' (assoc state counter n)]
    (if (> n limit)
      {:action :fatal :state state' :reason fatal-reason}
      (assoc retry-action :state state'))))

(defn validate-response
  "Validate response shape/tool names. Does not enforce workflow steps.

  By default, a text response with no tool calls is treated as final text
  (:action :text). Pass :require-tool? true when the current workflow state
  demands a tool call and text should be nudged instead."
  [state {:keys [tool-names max-retries require-tool?]
          :or {max-retries (:max-retries default-limits)}} response]
  (let [known (as-set tool-names)
        calls (response-tool-calls response)]
    (if (seq calls)
      (let [normalized (normalize-tool-calls calls)]
        (if-not (:ok? normalized)
          (bump-or-fatal state :retry-count max-retries
                         "too many invalid tool-call responses"
                         {:action :retry
                          :nudge (retry-nudge (:reason normalized))})
          (if-let [unknown (first (remove known (map :name (:tool-calls normalized))))]
            (bump-or-fatal state :retry-count max-retries
                           "too many unknown-tool responses"
                           {:action :retry
                            :nudge (unknown-tool-nudge unknown known)})
            {:action :execute
             :state (assoc state :retry-count 0)
             :tool-calls (:tool-calls normalized)})))
      (if require-tool?
        (bump-or-fatal state :retry-count max-retries
                       "too many text responses when a tool call was required"
                       {:action :retry
                        :nudge (retry-nudge "No tool call was found.")})
        {:action :text
         :state (assoc state :retry-count 0)}))))

(defn pending-steps [state required-steps]
  (let [completed (:completed state #{})]
    (vec (remove completed required-steps))))

(defn enforce-steps
  "Block terminal tools until required-steps have succeeded."
  [state {:keys [required-steps terminal-tools max-step-blocks]
          :or {max-step-blocks (:max-step-blocks default-limits)}} tool-calls]
  (let [terminal-set (as-set terminal-tools)
        terminal (first (filter terminal-set (map :name tool-calls)))
        pending (pending-steps state required-steps)]
    (if (and terminal (seq pending))
      (let [n (inc (:step-block-count state 0))
            state' (assoc state :step-block-count n)]
        (if (> n max-step-blocks)
          {:action :fatal
           :state state'
           :reason "model repeatedly skipped required steps"}
          {:action :step-blocked
           :state state'
           :nudge (step-nudge terminal pending (min 3 n))}))
      {:action :execute
       :state (assoc state :step-block-count 0)
       :tool-calls tool-calls})))

(defn check-response
  "Full pre-execution guardrail check: validate response, then enforce steps."
  [state config response]
  (let [validated (validate-response state config response)]
    (if (= :execute (:action validated))
      (enforce-steps (:state validated) config (:tool-calls validated))
      validated)))

(defn record-executed
  "Record successfully executed tool names. Failed/soft-error tools should be omitted."
  [state tool-names]
  (-> state
      (update :completed into tool-names)
      (assoc :retry-count 0 :step-block-count 0)))

