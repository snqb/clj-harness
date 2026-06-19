(ns clj-harness.observe
  "In-process observability ring buffer. Zero deps — just an atom.
   dashboard.clj reads this to serve the web panel; middleware/stream
   call record! at key points.

   Events are small maps pushed into a bounded vector. The SSE feed
   in dashboard.clj polls this atom."
  (:require [cheshire.core :as json]))

;; ══════════════════════ RING BUFFER ══════════════════════

(defonce ^:private events (atom []))

(def max-events
  "Soft cap on stored events. Oldest events are dropped when exceeded."
  2000)

(defn- next-index
  "Monotonic index for event ordering."
  []
  (let [idx (volatile! 0)]
    (fn [] (vswap! idx inc))))

(def ^:private next-idx (next-index))

;; ══════════════════════ PUBLIC API ══════════════════════

(defn record!
  "Push an event into the ring buffer. Thread-safe.
   event is a map; :ts and :idx are added automatically.
   Returns nil (fire-and-forget)."
  [event]
  (let [enriched (assoc event
                        :ts (System/currentTimeMillis)
                        :idx (next-idx))]
    (swap! events
           (fn [v]
             (let [v' (conj v enriched)]
               (if (> (count v') max-events)
                 (subvec v' (- (count v') max-events))
                 v'))))
    nil))

(defn recent
  "Return the N most recent events, newest last.
   (recent)    → last 100
   (recent 50) → last 50"
  ([]
   (let [v @events]
     (if (> (count v) 100)
       (subvec v (- (count v) 100))
       v)))
  ([n]
   (let [v @events]
     (if (> (count v) n)
       (subvec v (- (count v) n))
       v))))

(defn snapshot
  "Return all events as a plain vector (for debugging)."
  []
  @events)

(defn count-events
  "Number of events currently buffered."
  []
  (count @events))

;; ══════════════════════ SSE FEED ══════════════════════

(defn sse-subscriber
  "Returns a function that polls for new events since last-index.
   Called by dashboard SSE handler.

   (let [poll (sse-subscriber)]
     (poll)   → [] initially
     ... record! some events ...
     (poll)   → [new events since last call])"
  []
  (let [last-idx (volatile! 0)]
    (fn []
      (let [all @events
            start-idx @last-idx
            new-events (filterv #(> (:idx %) start-idx) all)]
        (when (seq new-events)
          (vreset! last-idx (:idx (peek new-events))))
        new-events))))

;; ══════════════════════ JSON HELPERS ══════════════════════

(defn events->json
  "Serialize events vector to JSON string."
  [events]
  (json/generate-string events))

;; ══════════════════════ STATS ══════════════════════

(defn compute-stats
  "Aggregate stats from buffered events for the panel header.
   Returns a map with counts and rates."
  []
  (let [all @events
        dialogues (into #{} (keep :dialogue-id all))
        msg-in (count (filter #(= :msg-in (:type %)) all))
        tool-calls (filter #(= :tool (:type %)) all)
        tool-ok (count (filter :ok? tool-calls))
        tool-fail (- (count tool-calls) tool-ok)
        nudges (count (filter #(= :nudge (:type %)) all))
        errors (count (filter #(= :error (:type %)) all))
        llm-calls (filter #(= :llm-call (:type %)) all)
        llm-stream (count (filter :stream? llm-calls))
        llm-sync (- (count llm-calls) llm-stream)
        total-tokens (reduce + 0 (keep :total-tokens llm-calls))
        prompt-tokens (reduce + 0 (keep :prompt-tokens llm-calls))
        completion-tokens (reduce + 0 (keep :completion-tokens llm-calls))
        latencies (keep :latency-ms llm-calls)
        turns (filter #(= :turn-complete (:type %)) all)
        turn-latencies (keep :latency-ms turns)]
    {:dialogues (count dialogues)
     :messages msg-in
     :tools {:total (count tool-calls)
             :ok tool-ok
             :fail tool-fail}
     :llm {:total (count llm-calls)
           :stream llm-stream
           :sync llm-sync
           :total-tokens total-tokens
           :prompt-tokens prompt-tokens
           :completion-tokens completion-tokens
           :avg-latency-ms (when (seq latencies) (int (/ (reduce + latencies) (count latencies))))
           :max-latency-ms (when (seq latencies) (reduce max latencies))}
     :turns {:total (count turns)
             :avg-latency-ms (when (seq turn-latencies)
                               (int (/ (reduce + turn-latencies) (count turn-latencies))))}
     :nudges nudges
     :errors errors
     :events (count all)
     :max max-events}))
