(ns clj-harness.compact
  "Conversation compaction — token estimation and LLM summarization.

   Keeps conversations within token budgets by summarizing older messages.
   Uses an injected summarize-fn to avoid circular dependencies on core.clj.

   Usage:
     (compact-history messages
       {:threshold 60000
        :summarize-fn (fn [msgs] (:content (core-agent {:messages msgs ...})))})"
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]))

;; ══════════════════════ TOKEN ESTIMATION ══════════════════════

(defn estimate-tokens
  "Estimate token count for a message vector.
   Char-level heuristic: ASCII ~0.3 tokens/char, non-ASCII (Cyrillic/CJK) ~0.75 tokens/char."
  [messages]
  (reduce (fn [total msg]
            (let [content (when (map? msg) (str (get msg "content")))
                  chars (count content)
                  ascii-count (count (re-seq #"[a-zA-Z0-9\s]" content))
                  non-ascii (- chars ascii-count)]
              (+ total (long (+ (* ascii-count 0.3) (* non-ascii 0.75))))))
          0
          messages))

;; ══════════════════════ COMPACTION ══════════════════════

(def ^:private compact-prompt
  "Summarize this conversation in 3-5 sentences. Keep: user preferences, key decisions,
   important facts, specific details (names, dates, numbers). Drop: tool call internals,
   repetitive exchanges, long descriptions. Return ONLY the summary text.")

(defn- keep-recent-count
  "How many recent messages to keep after compaction. Scales with token count:
   <20K tokens → 8 messages, <40K → 6, <60K → 4, else → 2."
  [messages]
  (let [total (estimate-tokens messages)]
    (cond
      (< total 20000)  8
      (< total 40000)  6
      (< total 60000)  4
      :else            2)))

(defn compact-history
  "Compress conversation when over token threshold. Uses injected LLM summarizer.

   Options map:
     :threshold    — token threshold to trigger compaction (default 60000)
     :summarize-fn — (fn [messages] => summary-string) for LLM summarization

   Strategy:
   1. Estimate tokens
   2. If under threshold, return as-is
   3. Keep last N recent messages (adaptive count)
   4. Summarize older messages via injected summarize-fn
   5. Prepend summary as system message + recent messages

   Returns compacted message vector."
  [messages {:keys [threshold summarize-fn]
             :or {threshold 60000}}]
  (let [tokens (estimate-tokens messages)]
    (if (< tokens threshold)
      messages
      (try
        (let [keep-n (keep-recent-count messages)
              recent (vec (take-last keep-n messages))
              older (vec (drop-last keep-n messages))
              ;; Take half of older messages for summarizer context
              split-at (max 1 (quot (count older) 2))
              summary-msgs (vec (take split-at (take split-at older)))
              summary (when summarize-fn
                        (summarize-fn (conj summary-msgs
                                            {"role" "system" "content"
                                             "You are a summarizer. Return ONLY 2-4 sentences."}
                                            {"role" "user" "content" compact-prompt})))]
          (if (and summary (not (str/blank? summary)))
            (let [summary-msg {"role" "system"
                               "content" (str "[Conversation summary — earlier messages compressed]\n" summary)}]
              (log/info :compaction :before (count older) :before-tokens (long tokens)
                        :after (count recent) :summary-len (count summary))
              (into [summary-msg] recent))
            messages))
        (catch Exception e
          (log/warn e :compaction-failed)
          ;; Fallback: just keep last 10 messages
          (vec (take-last 10 messages)))))))
