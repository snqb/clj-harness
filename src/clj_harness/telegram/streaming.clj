(ns clj-harness.telegram.streaming
  "Progressive streaming response to Telegram.

  Consumes a core.async channel of {:type word :text ...} messages
  and progressively edits a single Telegram message to simulate
  live thinking/typing.

  Channel protocol:
    {:type :status :text \"🔍 Ищу...\"}  — status update, edits placeholder immediately
    {:type :delta  :text \"токен\"}     — text chunk, accumulated + throttle-edited
    {:type :finish :reason \"stop\"}    — final edit with md→html + split
    {:delta \"токен\"} / {:done :closed} — also accepted from handle-message-async
    channel closes                     — equivalent to :done

  Throttle: edits fire at most every 800ms (status messages are immediate).
  Mid-stream previews are block-buffered plain text: unfinished trailing
  markdown stays hidden until a paragraph, line/list item, or sentence
  completes. Final edit uses full md→html formatting.

  Usage:
    (require '[clj-harness.telegram.streaming :as ts]
             '[clojure.core.async :as a])

    (def ch (a/chan))
    (ts/stream-to-telegram chat-id ch)
    ;; In agent loop:
    (a/>!! ch {:type :status :text \"🔍 Ищу туры...\"})
    ;; ... tool call ...
    (a/>!! ch {:type :delta :text \"Нашёл\"})
    (a/>!! ch {:type :delta :text \" несколько вариантов.\"})
    (a/>!! ch {:type :finish :reason \"stop\"})
    (a/close! ch)"
  (:require [clj-harness.telegram :as tg]
            [clj-harness.telegram.format :as fmt]
            [clojure.core.async :as a :refer [go <! chan offer! timeout alts! close!]]
            [clojure.tools.logging :as log]))

(def ^:private default-throttle-ms 800)
(def ^:private default-placeholder "🧠 Думаю...")

;; ══════════════════════ THROTTLE TICKER ══════════════════════

(defn- ticker-chan
  "Create a channel that receives :tick every interval-ms until stop-ch closes.
   Uses offer! to avoid backpressure — drops ticks if consumer is slow."
  [interval-ms stop-ch]
  (let [tick-ch (chan 1)]
    (go (loop []
          (let [[_ port] (alts! [(timeout interval-ms) stop-ch])]
            (when-not (= port stop-ch)
              (offer! tick-ch :tick)
              (recur)))))
    tick-ch))

;; ══════════════════════ HELPERS ══════════════════════

(defn- msg-type
  "Normalize stream message shapes from both telegram.streaming and core async."
  [msg]
  (or (:type msg)
      (cond
        (:delta msg) :delta
        (:finish msg) :finish
        (:done msg) :finish)))

(defn- msg-text
  "Extract text from either {:type :delta :text ...} or {:delta ...}."
  [msg]
  (or (:text msg) (:delta msg) ""))

(defn- render-final
  "Render final accumulated text: md→html → split → edit first + send rest.
   Passes reply_markup to the edit (e.g. reset keyboard)."
  [chat-id msg-id ^String text & {:keys [reply_markup]}]
  (let [html (fmt/md->html text)
        chunks (fmt/split-message html)]
    (if (= 1 (count chunks))
      (tg/edit-message chat-id msg-id (first chunks)
                       :parse-mode "HTML" :preview false :reply_markup reply_markup)
      (do
        (tg/edit-message chat-id msg-id (first chunks)
                         :parse-mode "HTML" :preview false :reply_markup reply_markup)
        (doseq [chunk (rest chunks)]
          (tg/send-message chat-id chunk :parse-mode "HTML" :preview false :reply_markup reply_markup))))))

;; ══════════════════════ MAIN ══════════════════════

(defn stream-to-telegram
  "Stream LLM response to a Telegram chat with progressive editing.

   chat-id   — Telegram chat id (number or string)
   ch        — core.async channel of {:type :status|:delta|:finish :text \"...\"}
   Options:
     :placeholder    — initial message (default \"🧠 Думаю...\")
     :throttle-ms    — edit throttle interval in ms (default 800)
     :parse-mode     — text parse-mode for status/delta previews (default nil = plain)
                       Final edit always uses HTML.
     :reset-button?  — attach '🔄 Новый диалог' ReplyKeyboardMarkup to final message
                       Tapping sends '/reset' as text — wire to your command handler.

   Returns a go-block that resolves to the final accumulated text."
  [chat-id ch & {:keys [placeholder throttle-ms parse-mode reset-button?]
                 :or {placeholder default-placeholder
                      throttle-ms default-throttle-ms
                      parse-mode nil}}]
  (let [reply_markup (when reset-button? (tg/reset-keyboard))]
    (go
      (try
        (let [placeholder-msg (tg/send-message chat-id placeholder
                                               :parse-mode parse-mode
                                               :preview false)
              msg-id (some-> placeholder-msg (get "result") (get "message_id"))]
          (if-not msg-id
            ;; Fallback: no message to edit — accumulate, then send final Markdown.
            (loop [acc ""]
              (if-let [msg (<! ch)]
                (case (msg-type msg)
                  :delta (recur (str acc (msg-text msg)))
                  :finish (do (tg/send-md chat-id acc :reply_markup reply_markup)
                              acc)
                  ;; :status, unknown — ignore
                  (recur acc))
                (do
                  (when (seq acc)
                    (tg/send-md chat-id acc :reply_markup reply_markup))
                  acc)))

            ;; Normal path: edit placeholder with block-buffered previews.
            (let [stop-ch (chan)
                  tick-ch (ticker-chan throttle-ms stop-ch)
                  acc (volatile! "")
                  last-preview (volatile! "")]
              (try
                (loop []
                  (let [[msg port] (alts! [ch tick-ch])]
                    (cond
                      (= port tick-ch)
                      (let [preview (fmt/streaming-preview @acc)]
                        (when (and (seq preview) (not= preview @last-preview))
                          (tg/edit-message chat-id msg-id preview
                                           :parse-mode parse-mode :preview false)
                          (vreset! last-preview preview))
                        (recur))

                      (nil? msg)
                      (let [text @acc]
                        (when (seq text)
                          (render-final chat-id msg-id text :reply_markup reply_markup))
                        text)

                      :else
                      (case (msg-type msg)
                        :status
                        (let [text (fmt/strip-md (msg-text msg))]
                          (tg/edit-message chat-id msg-id text
                                           :parse-mode parse-mode :preview false)
                          (vreset! last-preview text)
                          (vreset! acc "")
                          (recur))

                        :delta
                        (do
                          (vreset! acc (str @acc (msg-text msg)))
                          (recur))

                        :finish
                        (let [text @acc]
                          (render-final chat-id msg-id text :reply_markup reply_markup)
                          text)

                        ;; Unknown type → ignore
                        (recur)))))
                (finally
                  (close! stop-ch)
                  (close! tick-ch))))))
        (catch Exception e
          (log/error e :stream-error)
          (try (tg/send-message chat-id "❌ Произошла ошибка. Попробуйте ещё раз."
                                :reply_markup reply_markup)
               (catch Exception _))
          nil)))))
;; ══════════════════════ CONVENIENCE ══════════════════════

;; ── Non-streaming (still useful as a one-shot convenience) ──

(defn send-response
  "Send a complete response (non-streaming convenience).
   Sends formatted HTML.
   Options:
     :reply_markup  — passed through to send-md
     :reset-button?  — attach '🔄 Новый диалог' keyboard to response"
  [chat-id response-text & {:keys [reply_markup reset-button?]}]
  (let [markup (or reply_markup (when reset-button? (tg/reset-keyboard)))]
    (tg/send-md chat-id response-text :reply_markup markup)))
