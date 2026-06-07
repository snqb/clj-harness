(ns clj-harness.effects
  "Effect system — pure descriptions of side effects, interpreted separately.

   The agent loop emits effect maps. This namespace interprets them into
   concrete side effects. Inspired by llx (ol.llx.agent.fx)."
  (:require
   [clojure.core.async :refer [put!]]
   [clj-harness.tool-loop :as tl]))

;; ══════════════════════ EFFECT TYPES ══════════════════════

(def call-llm     ::call-llm)
(def execute-tool ::execute-tool)
(def emit-event  ::emit-event)
(def sleep       ::sleep)

;; ══════════════════════ EVENTS ══════════════════════

(def event-turn-start  ::turn-start)
(def event-llm-start   ::llm-start)
(def event-llm-chunk   ::llm-chunk)
(def event-llm-done    ::llm-done)
(def event-tool-start  ::tool-start)
(def event-tool-done   ::tool-done)
(def event-tool-error  ::tool-error)
(def event-turn-end    ::turn-end)
(def event-agent-error ::agent-error)

;; ══════════════════════ EFFECT FACTORIES ══════════════════════

(defn make-call-llm
  "Create a ::call-llm effect."
  [model messages tools & {:as opts}]
  (merge {::type call-llm
          ::model model
          ::messages messages
          ::tools tools}
         opts))

(defn make-execute-tool
  "Create a ::execute-tool effect.
   tool-call = {:name \"search\" :id \"call_1\" :args {\"query\" \"...\"}}"
  [tool-call]
  {::type execute-tool
   ::tool-call tool-call})

(defn make-emit-event
  "Create an ::emit-event effect."
  [event-type & {:as data}]
  (merge {::type emit-event
          ::event-type event-type}
         data))

(defn make-sleep
  "Create a ::sleep effect (for retry backoff)."
  [ms]
  {::type sleep ::ms ms})

;; ══════════════════════ INTERPRETER ══════════════════════

(defn handle-effect
  "Interpret a single effect description. Dispatches on ::type.

   env carries runtime dependencies:
     :llm-fn            — (fn [model msgs tools opts] => LLM response)
     :tool-map          — {tool-name {:execute fn ...}}
     :tool-post-process — (fn [tool-name result] => enriched-result)
     :heap              — optional heap atom
     :events>           — core.async channel for events (optional)"
  [env effect]
  (case (::type effect)
    ::call-llm
    (let [{:keys [llm-fn]} env
          model (::model effect)
          messages (::messages effect)
          tools (::tools effect)
          force-tool? (:force-tool? effect)]
      (llm-fn model messages tools (when force-tool? {:force-tool? true})))

    ::execute-tool
    (let [{:keys [tool-map tool-post-process heap abort-signal]} env
          tool-call (::tool-call effect)]
      ;; Build on-update callback from events> if available
      (let [on-update (when-let [events> (:events> env)]
                        (fn [partial]
                          (put! events>
                                {::event-type ::tool-update
                                 :tool-name (:name tool-call)
                                 :partial partial})))]
        (tl/execute-tool-call tool-map tool-post-process heap
                              tool-call abort-signal on-update)))

    ::emit-event
    (when-let [events> (:events> env)]
      (put! events> (dissoc effect ::type)))

    ::sleep
    (Thread/sleep (::ms effect))

    (throw (ex-info (str "Unknown effect type: " (::type effect))
                    {:effect effect}))))

(defn handle-effects
  "Interpret a vector of effects in sequence.
   Returns {:llm-response ... :tool-results [...]}.

   Events are emitted as encountered. LLM calls and tool executions
   are processed sequentially."
  [env effects]
  (reduce
   (fn [acc effect]
     (let [result (handle-effect env effect)]
       (case (::type effect)
         ::call-llm    (assoc acc :llm-response result)
         ::execute-tool (update acc :tool-results (fnil conj []) result)
         acc)))
   {:llm-response nil :tool-results []}
   effects))

;; ══════════════════════ EVENT → STATUS HELPERS ══════════════════════

(defn event->status
  "Convert an agent event to a Russian status string.
   Useful for Telegram 'thinking' indicators."
  [event]
  (let [event-type (::event-type event)]
    (case event-type
      ::turn-start     "🧠 Анализирую запрос..."
      ::tool-start     (str "🔧 Выполняю " (or (:name event) "инструмент") "...")
      ::tool-done      nil  ;; silent
      ::tool-error     "⚠️ Ошибка инструмента"
      ::llm-done       nil  ;; silent
      ::turn-end       nil  ;; silent
      ::agent-error    (str "❌ Ошибка: " (or (:error event) "неизвестная"))
      nil)))

(defn subscribe-events
  "Read events from channel and call status-fn for each non-nil status.
   Runs in a future. Returns the future."
  [events-chan status-fn]
  (future
    (loop []
      (when-let [event (clojure.core.async/<!! events-chan)]
        (when-let [status (event->status event)]
          (try (status-fn status) (catch Exception _)))
        (recur)))))
