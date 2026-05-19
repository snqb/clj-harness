(ns clj-harness.heap
  "Content-addressable tool result store.

   Pattern: instead of pushing 24K tool outputs into LLM context,
   store them in a heap and return compact summaries with heap-id references.
   LLM can fetch full results on demand via fetch_result tool.

   Lifecycle:
   - Created when tool output exceeds threshold (default: 2000 chars)
   - Stored in session-scoped atom (dies with session)
   - Optional: persist to SQLite for cross-session memory
   - GC: entries older than TTL (default: 10 min) are evicted

   Architecture:
     Active Context (small)        Heap (external)
     ┌──────────────────┐        ┌─────────────────────┐
     │ [heap:abc123]     │   ←→  │ abc123 → [24K data]  │
     │ Top: HP 21K, ...  │        │ def456 → [shell out] │
     └──────────────────┘        └─────────────────────┘"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [java.util UUID]))

;; ══════════════════════ CONFIG ══════════════════════

(def ^:dynamic *heap-threshold*
  "Max chars before storing result in heap instead of inline."
  2000)

(def ^:dynamic *heap-ttl-ms*
  "10 minutes — entries older than this get GC'd."
  600000)

(def ^:dynamic *heap-max-entries*
  "Max heap entries per session. Oldest evicted (LRU)."
  50)

;; ══════════════════════ STATE ══════════════════════

(defn create-heap
  "Create a fresh heap (per-session atom)."
  []
  (atom {:entries {}     ;; {heap-id {:data ... :tool ... :created ... :size ...}}
         :order []}))     ;; LRU order: oldest first

;; ══════════════════════ CORE OPERATIONS ══════════════════════

(defn store!
  "Store a tool result in the heap. Returns {:heap-id ... :summary ... :size ...}.

   If result is under threshold, returns nil (caller should inline it).
   If over threshold, stores full result and returns compact summary + heap-id."
  [heap tool-name result]
  (let [result-str (str result)
        size (count result-str)]
    (if (< size *heap-threshold*)
      nil  ;; caller responsibility: use inline
      (let [heap-id (str (UUID/randomUUID))
            ;; Extract summary: first 3 items (or first 500 chars)
            summary (subs result-str 0 (min 500 size))
            entry {:tool tool-name
                   :data result-str
                   :size size
                   :summary summary
                   :created (System/currentTimeMillis)}]
        (swap! heap (fn [h]
                      (let [entries (assoc (:entries h) heap-id entry)
                            order (conj (:order h) heap-id)
                            ;; Evict oldest if over max
                            [entries order]
                            (if (> (count order) *heap-max-entries*)
                              (let [evict-id (first order)]
                                (log/debug :heap-evict evict-id :reason :lru)
                                [(dissoc entries evict-id) (subvec order 1)])
                              [entries order])]
                        {:entries entries :order order})))
        (log/debug :heap-store heap-id :size size :tool tool-name)
        {:heap-id heap-id
         :summary summary
         :size size
         :truncated true}))))

(defn fetch
  "Fetch full tool result from heap by heap-id.
   Returns full data string, or nil if not found/expired."
  [heap heap-id]
  (when-let [entry (get (:entries @heap) heap-id)]
    (let [age (- (System/currentTimeMillis) (:created entry))]
      (if (> age *heap-ttl-ms*)
        (do (log/debug :heap-fetch heap-id :status :expired :age-ms age)
            nil)
        (do (log/debug :heap-fetch heap-id :status :ok)
            (:data entry))))))

(defn fetch-with-query
  "Fetch result from heap and filter lines matching query (case-insensitive).
   Returns matching lines joined, or 'No matching results.'"
  [heap heap-id query]
  (if-let [data (fetch heap heap-id)]
    (let [q (str/lower-case query)
          lines (str/split-lines data)
          matching (filter #(str/includes? (str/lower-case %) q) lines)]
      (if (seq matching)
        (str/join "\n" matching)
        (str "No results matching '" query "' in heap:" heap-id)))
    (str "Heap entry " heap-id " not found or expired.")))

(defn summary
  "Get compact summary of a heap entry (without fetching full data)."
  [heap heap-id]
  (when-let [entry (get (:entries @heap) heap-id)]
    (str "[" heap-id "] " (:tool entry) " — " (:size entry) " chars, "
         (quot (- (System/currentTimeMillis) (:created entry)) 1000) "s ago\n"
         (:summary entry) "...")))

;; ══════════════════════ MAINTENANCE ══════════════════════

(defn gc!
  "Remove expired entries (older than TTL). Returns count of evicted entries."
  [heap]
  (let [now (System/currentTimeMillis)
        expired (atom 0)]
    (swap! heap (fn [h]
                  (let [expired-ids (keep (fn [hid]
                                            (let [e (get (:entries h) hid)]
                                              (when (> (- now (:created e)) *heap-ttl-ms*)
                                                hid)))
                                          (:order h))]
                    (swap! expired + (count expired-ids))
                    (when (pos? (count expired-ids))
                      (log/debug :heap-gc :evicted (count expired-ids)))
                    {:entries (apply dissoc (:entries h) expired-ids)
                     :order (vec (remove (set expired-ids) (:order h)))})))
    @expired))

(defn clear!
  "Clear all heap entries."
  [heap]
  (reset! heap {:entries {} :order []}))

(defn stats
  "Return {:entries N :total-size bytes :oldest-age-ms N}."
  [heap]
  (let [h @heap
        now (System/currentTimeMillis)
        entries (vals (:entries h))]
    {:entries (count entries)
     :total-size (reduce + 0 (map :size entries))
     :oldest-age-ms (when-let [oldest (first (sort-by :created entries))]
                      (- now (:created oldest)))
     :tool-breakdown (frequencies (map :tool entries))}))

;; ══════════════════════ FORMATTING HELPERS ══════════════════════

(defn format-heap-ref
  "Format a heap reference for LLM context.
   Returns compact summary string that LLM can use to decide
   whether to call fetch_result for details."
  [heap-id {:keys [tool size truncated] :as _meta}]
  (str "📦 Tool result stored in heap: " heap-id
       " (" tool ", " size " chars)"
       (when truncated " — truncated for context")
       ". Use fetch_result to get details."))

(defn extract-key-items
  "Extract key items from tool result for the LLM summary.
   Returns first 3 lines that look like list items (start with #, •, -, or digit)."
  [result-str]
  (let [lines (str/split-lines result-str)
        items (filter #(re-find #"^[#\d•\-]" (str/trim %)) lines)]
    (str/join "\n" (take 5 items))))
