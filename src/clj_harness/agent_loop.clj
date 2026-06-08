(ns clj-harness.agent-loop
  "Pure agent state machine — emits effect descriptions, never performs I/O.

   Inspired by llx (ol.llx.agent.loop). Separates 'what to do' (effects)
   from 'how to do it' (interpreter in clj-harness.effects).

   ## Architecture

   State machine phases:
     :idle             → waiting for user input
     :thinking         → LLM is being called (effect emitted, awaiting response)
     :executing-tools  → tool(s) being executed (effects emitted)
     :done             → final response ready
     :error            → error state

   Each phase has a transition fn: (state, signal) => [state', effects]
   Effects are pure maps — no side effects here.

   ## Usage

     (require '[clj-harness.agent-loop :as loop]
              '[clj-harness.effects :as fx])

     ;; Create initial state
     (def state (loop/make-state
                 {:model :deepseek-v4-pro
                  :messages [{:role \"user\" :content \"...\"}]
                  :tool-schemas [...]}))

     ;; Pure step — no I/O
     (def [state' effects] (loop/step state {:type ::loop/signal-start}))

     ;; effects = [{::fx/type ::fx/call-llm ...}]
     ;; Driver interprets them with fx/handle-effect

     ;; Run until done (driver side)
     (loop/run env state) → final response"
  (:require
   [clj-harness.effects :as fx]
   [clj-harness.guardrails :as gr]
   [clj-harness.tool-loop :as tl]
   [clojure.string :as str]))

(defn- pending-tool-required?
  "Check if the guardrail requires a tool call (pending required steps).
   Also returns true for the first call if required-steps are configured
   and no tools have been called yet (prevent hallucinated answers)."
  [state]
  (let [nudge-state (:nudge-state state)
        nudge-opts (:nudge-opts state)
        required (or (:required-steps nudge-opts)
                     (when nudge-state (:required-steps nudge-state)))
        pending (when (seq required)
                  (gr/pending-steps nudge-state required))]
    (boolean (seq pending))))

;; ══════════════════════ SIGNALS ══════════════════════

;; Internal signals between state machine and driver
(def signal-start   ::signal-start)    ;; driver → machine: begin processing
(def signal-llm-done ::signal-llm-done) ;; driver → machine: LLM response received
(def signal-tool-done ::signal-tool-done) ;; driver → machine: tool result received
(def signal-max-turns ::signal-max-turns) ;; driver → machine: max turns reached
(def signal-error    ::signal-error)     ;; driver → machine: error occurred

;; ══════════════════════ INITIAL STATE ══════════════════════

(defn make-state
  "Create initial agent state.

   opts:
     :messages      — vector of message maps (required)
     :tool-schemas  — vector of OpenAI tool schemas
     :tool-map      — {name {:execute fn ...}}
     :max-turns     — max tool-calling iterations (default 10)
     :model         — model key
     :provider      — :deepseek or :openrouter (default :deepseek)
     :nudge-state   — guardrails state (from gr/make-state)
     :nudge-opts    — guardrails opts (from tl/infer-nudges)
     :max-tool-output — max chars per tool result (default 8000)"
  [{:keys [messages tool-schemas tool-map max-turns model provider
           nudge-state nudge-opts max-tool-output]
    :or {max-turns 10 provider :deepseek max-tool-output 8000}}]
  (merge
   {:phase        :idle
    :messages     (vec messages)
    :tool-schemas (or tool-schemas [])
    :tool-map     (or tool-map {})
    :max-turns    max-turns
    :model        model
    :provider     provider
    :turn         0
    :nudge-state  (or nudge-state (gr/make-state))
    :nudge-opts   nudge-opts
    :max-tool-output max-tool-output
    :steering-queue clojure.lang.PersistentQueue/EMPTY}
   ;; Internal mutable slots (set by transitions)
   {:response     nil    ;; last LLM response
    :error        nil}))

;; ══════════════════════ PURE TRANSITIONS ══════════════════════

(defn- idle-transition
  "Transition from :idle.
   On signal-start: emit call-llm effect.
   On signal-error: move to :error."
  [state signal]
  (case signal
    ::signal-start
    (let [msgs (:messages state)
          tools (:tool-schemas state)
          nudge-opts (:nudge-opts state)]
      [(assoc state :phase :thinking
              :turn (inc (:turn state)))
       [(fx/make-emit-event fx/event-turn-start
                            :turn (:turn state)
                            :messages (count msgs))
        (fx/make-call-llm (:model state) msgs tools
                          :provider (:provider state)
                          :force-tool? (pending-tool-required? state))]])

    ::signal-error
    [(assoc state :phase :error :error "Unknown error")
     [(fx/make-emit-event fx/event-agent-error :error "Unknown error")]]

    ;; Stay idle
    [state []]))

(defn- thinking-transition
  "Transition from :thinking (LLM response received).
   Checks for tool calls, nudges, or final text response."
  [state signal]
  (case signal
    ::signal-llm-done
    (let [resp (:response state)
          content (:content resp)
          tool-calls (:tool-calls resp)
          finish (:finish resp)
          nudge-opts (:nudge-opts state)
          nudge-state (:nudge-state state)
          cfg (when nudge-opts
                (tl/guardrail-config (:tool-map state) nudge-opts nudge-state))
          checked (when nudge-opts
                    (gr/check-response nudge-state cfg resp))
          action (or (:action checked) :disabled)]

      (case action
        :text
        ;; Guardrails blocked tool calls → return as final text
        [(assoc state :phase :done :response {:content content})
         [(fx/make-emit-event fx/event-turn-end :finish finish)]]

        :fatal
        ;; Guardrail gave up. If the LLM skipped required steps, don't return
        ;; its hallucinated content — say we can't answer instead.
        (let [step-fail? (str/includes? (str (:reason checked)) "skipped required steps")
              fallback (str "⚠️ " (:reason checked))]
          (do [(assoc state :phase :done :response {:content (if step-fail? fallback (or content fallback))})
               [(fx/make-emit-event fx/event-turn-end :reason :nudge-fatal)]]))

        ;; :retry / :step-blocked → nudge message, loop back to :thinking
        ;; Nudges do NOT count as turns — they have their own retry limit
        (:retry :step-blocked)
        (let [nudge-msg (tl/nudge-message (:nudge checked))
              new-msgs (conj (:messages state) (assoc nudge-msg ::private true))
              state' (-> state
                         (assoc :messages new-msgs
                                :nudge-state (:state checked)))]
          [state'
           [(fx/make-call-llm (:model state) new-msgs (:tool-schemas state)
                              :provider (:provider state)
                              :force-tool? true)]])

        ;; (:execute :disabled)
        (let [normalized-calls (if nudge-opts
                                 (:tool-calls checked)
                                 (when (seq tool-calls)
                                   (mapv tl/loose-normalize-tool-call tool-calls)))]
          (if (seq normalized-calls)
            ;; Tool calls present → transition to :executing-tools
            (let [tool-calls (if nudge-opts
                               (mapv :raw (:tool-calls checked))
                               tool-calls)
                  asst-msg (cond-> {"role" "assistant"
                                    "tool_calls" (mapv tl/raw-call->api tool-calls)}
                             (not (str/blank? content))
                             (assoc "content" content)
                             (:reasoning-content resp)
                             (assoc "reasoning_content" (:reasoning-content resp)))
                  new-msgs (conj (:messages state) asst-msg)]
              [(-> state
                   (assoc :phase :executing-tools
                          :messages new-msgs
                          :pending-tool-calls (vec normalized-calls)
                          :tool-call-index 1)  ;; first tool emitted below
                   (merge (when nudge-opts {:nudge-state (:state checked)})))
               (into [(fx/make-emit-event fx/event-llm-done
                                          :finish finish
                                          :tool-calls (count normalized-calls))]
                     (cons (fx/make-execute-tool (first normalized-calls))
                           (map (fn [tc]
                                  (fx/make-emit-event fx/event-tool-start
                                                      :name (:name tc)
                                                      :id (:id tc)))
                                normalized-calls)))])

            ;; No tool calls → final response
            [(assoc state :phase :done :response {:content content})
             [(fx/make-emit-event fx/event-llm-done :finish finish)
              (fx/make-emit-event fx/event-turn-end :finish finish)]]))))

    ::signal-max-turns
    [(assoc state :phase :done
            :response {:content (str "⚠️ Reached max turns ("
                                     (:max-turns state)
                                     "). Try a more specific query.")})
     [(fx/make-emit-event fx/event-turn-end :reason :max-turns)]]

    ::signal-error
    [(assoc state :phase :error :error "LLM call failed")
     [(fx/make-emit-event fx/event-agent-error :error "LLM call failed")]]

    [state []]))

(defn- executing-tools-transition
  "Transition from :executing-tools.
   On signal-tool-done: check if more tools pending, else call LLM again."
  [state signal]
  (case signal
    ;; Not a signal we handle — emit error
    ::signal-tool-done
    (let [index (:tool-call-index state)
          calls (:pending-tool-calls state)
          tool-call (nth calls index nil)]
      (if-not tool-call
        ;; No more tools pending → call LLM with results
        ;; Increment turn counter for this new LLM call cycle
        [(assoc state :phase :thinking
                :turn (inc (:turn state)))
         [(fx/make-call-llm (:model state) (:messages state) (:tool-schemas state)
                              :provider (:provider state)
                              :force-tool? (pending-tool-required? state))]]

        ;; (:execute :disabled)
        (let [state' (update state :tool-call-index inc)]
          [state'
           [(fx/make-execute-tool tool-call)]])))

    ::signal-error
    [(assoc state :phase :error :error "Tool execution failed")
     [(fx/make-emit-event fx/event-agent-error :error "Tool execution failed")]]

    [state []]))

(defn- done-transition
  "Transition from :done — terminal state, no further transitions."
  [state _signal]
  [state []])

(defn- error-transition
  "Transition from :error — terminal state."
  [state _signal]
  [state []])

;; ══════════════════════ STATE MACHINE ══════════════════════

(def ^:private transitions
  "State machine as pure data — phase → (state, signal) => [state', effects]."
  {:idle              idle-transition
   :thinking          thinking-transition
   :executing-tools   executing-tools-transition
   :done              done-transition
   :error             error-transition})

(defn step
  "Pure step: (state, signal) => [state', effects].
   Dispatches to the transition fn for the current phase.
   Returns [new-state, vector-of-effect-maps]."
  [state signal]
  (let [phase (:phase state)
        transition (get transitions phase)]
    (if transition
      (transition state signal)
      [(assoc state :phase :error :error (str "Unknown phase: " phase))
       [(fx/make-emit-event fx/event-agent-error
                            :error (str "Unknown phase: " phase))]])))

;; ══════════════════════ DRIVER (RUN LOOP) ══════════════════════

(defn steer
  "Queue a steering message that will be injected at the next
   opportunity (after current tool/LLM completes).
   The message is treated as a user message with ::private metadata.
   Returns updated state."
  [state message]
  (update state :steering-queue conj
          (assoc message ::private true :role "user")))

(defn snapshot
  "Extract a plain map snapshot of the current agent state.
   Can be rehydrated later via make-state."
  [state]
  state)

(defn- flush-steering
  "If steering queue is non-empty, return [injected-state effects] with
   the queued messages injected and a fresh call-llm effect. Returns nil
   when queue is empty."
  [state]
  (let [sq (:steering-queue state)]
    (when (seq sq)
      (let [steering-msgs (vec sq)
            new-msgs (into (:messages state) steering-msgs)
            state' (-> state
                       (assoc :messages new-msgs)
                       (assoc :steering-queue clojure.lang.PersistentQueue/EMPTY))]
        [state'
         [(fx/make-emit-event fx/event-turn-start :turn (:turn state))
          (fx/make-call-llm (:model state) new-msgs (:tool-schemas state)
                            :provider (:provider state)
                            :force-tool? (pending-tool-required? state))]]))))

(defn run
  "Run the agent to completion. Takes env + initial state, returns final response.

   env must include:
     :llm-fn  — (fn [model msgs tools] => LLM response)
     :events> — core.async channel for events (optional)

   Returns {:content \"...\" :tool-calls [...] :finish \"stop\"}"
  [env state]
  (let [[state' effects] (step state signal-start)]
    (loop [st state' pending effects]
      (if (empty? pending)
        (case (:phase st)
          :done (or (:response st) {:content "No response."})
          :error {:content (str "Error: " (:error st)) :error (:error st)}
          {:content (str "Unexpected phase: " (:phase st))})

        (let [effect (first pending)
              remaining (rest pending)]
          (case (::fx/type effect ::unknown)

            ;; Call LLM → feed response back to machine
            ::fx/call-llm
            (let [llm-resp (fx/handle-effect env effect)
                  [st' new-fx] (step (assoc st :response llm-resp) signal-llm-done)]
              ;; Check steering queue
              (if-let [[st-steered steer-fx] (flush-steering st')]
                (recur st-steered (into remaining steer-fx))
                (if (and (= (:phase st') :thinking)
                         (>= (:turn st') (:max-turns st')))
                  (let [[st-done done-fx] (step st' signal-max-turns)]
                    (recur st-done (into remaining done-fx)))
                  (recur st' (into remaining new-fx)))))

            ;; Execute tool → feed result back to machine
            ::fx/execute-tool
            (let [tool-result (fx/handle-effect env effect)
                  tool-call (::tool-call effect)
                  ;; Apply result to messages in state BEFORE feeding signal
                  result-msg (or (:message tool-result)
                                 {:role "tool" :content (str tool-result)})
                  st-with-result (update st :messages
                                         conj
                                         (if (map? result-msg)
                                           (update result-msg "content"
                                                   #(tl/format-tool-output
                                                     nil (:tool tool-result) %
                                                     (:max-tool-output st)))
                                           {:role "tool" :content (str result-msg)}))
                  ;; Feed signal → machine emits next effect or transitions
                  [st' new-fx] (step st-with-result signal-tool-done)]
              ;; Emit tool event
              (when (:events> env)
                (let [{:keys [tool ok?]} tool-result]
                  (fx/handle-effect env
                                    (fx/make-emit-event (if ok? fx/event-tool-done fx/event-tool-error)
                                                        :name tool :ok? ok?))))
              ;; Check steering queue
              (if-let [[st-steered steer-fx] (flush-steering st')]
                (recur st-steered (into remaining steer-fx))
                (recur st' (into remaining new-fx))))

            ;; Emit event → fire and forget
            ::fx/emit-event
            (do (fx/handle-effect env effect)
                (recur st remaining))

            ::fx/sleep
            (do (fx/handle-effect env effect)
                (recur st remaining))

            ;; Unknown — skip
            (recur st remaining)))))))
