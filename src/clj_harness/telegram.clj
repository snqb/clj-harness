(ns clj-harness.telegram
  "Telegram Bot API abstraction.

  Thin wrapper around HTTP calls to api.telegram.org.
  Handles retry on rate limits, parse_mode fallback (HTML → plain text),
  and long message splitting.

  Design:
  - Pure functions, no state (except token atom)
  - Token configurable via set-token!
  - Retry on 429 (rate limit) with exponential backoff
  - Auto-split messages > 4096 chars
  - Re-exports format functions for convenience"
  (:require [clj-harness.telegram.format :as fmt]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clj-harness.observe :as observe])
  (:import [java.net URI]
           [java.net.http HttpClient HttpClient$Version HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.time Duration]))

;; ══════════════════════ CONFIG ══════════════════════

(def api-base "https://api.telegram.org")

(defonce ^:private token-atom
  (atom (or (System/getenv "BOT_TOKEN") (System/getenv "TG_TOK") "")))

(defn set-token!
  "Set the bot token. Call before start-polling.
   Load from pass: (set-token! (pass-show \"telegram/bot/token\"))"
  [tk]
  (reset! token-atom tk)
  (log/info :token-set (subs tk 0 6)))

(defn- token []
  @token-atom)

;; Re-export format functions for convenience
(def md->html fmt/md->html)
(def split-message fmt/split-message)
(def escape-html fmt/escape-html)
(def rewrite-markdown-tables fmt/rewrite-markdown-tables)
(def render-table fmt/render-table)

;; ══════════════════════ HTTP ══════════════════════

(defn- call
  "Raw Telegram API call. Returns parsed JSON with string keys, or nil on error."
  [method body & {:keys [max-retries timeout-ms] :or {max-retries 3 timeout-ms 65000}}]
  (let [tk (token)
        url (str api-base "/bot" tk "/" method)
        client (-> (HttpClient/newBuilder)
                   (.version HttpClient$Version/HTTP_1_1)
                   (.connectTimeout (Duration/ofMillis 10000))
                   .build)]
    (loop [attempt 0]
      (let [builder (doto (HttpRequest/newBuilder (URI/create url))
                      (.timeout (Duration/ofMillis timeout-ms))
                      (.header "Content-Type" "application/json"))
            _ (.POST builder (HttpRequest$BodyPublishers/ofString (json/generate-string body)))
            resp (.send client (.build builder) (HttpResponse$BodyHandlers/ofString))
            status (.statusCode resp)]
        (cond
          (< status 400)
          (json/parse-string (str/trim (.body resp)) false)

          (= status 429)
          (if (< attempt max-retries)
            (let [backoff (* 1000 (Math/pow 2 attempt))]
              (log/warn :tg-rate-limited :retry attempt :backoff-ms backoff)
              (Thread/sleep (long backoff))
              (recur (inc attempt)))
            (do (log/error :tg-rate-limited-exhausted) nil))

          :else
          (do (log/error :tg-error status :body (.body resp)) nil))))))

;; ══════════════════════ MESSAGE API ══════════════════════

(defn send-message
  "Send a text message to chat.
   Options:
     :parse-mode  — \"HTML\" (default), \"Markdown\", \"MarkdownV2\", or nil (plain)
     :preview     — link preview: true (default) or false
     :reply-to    — message_id to reply to
     :reply_markup — ReplyKeyboardMarkup, InlineKeyboardMarkup, or remove map

   Returns Telegram Message object or nil on failure."
  [chat-id text & {:keys [parse-mode preview reply-to reply_markup]
                   :or {parse-mode "HTML" preview true}}]
  (let [safe-text (fmt/rewrite-markdown-tables text)
        body (cond-> {"chat_id" (str chat-id) "text" safe-text}
               parse-mode (assoc "parse_mode" parse-mode)
               (false? preview) (assoc "disable_web_page_preview" true)
               reply-to (assoc "reply_to_message_id" (str reply-to))
               reply_markup (assoc "reply_markup" reply_markup))
        result (call "sendMessage" body)]
    ;; Fallback: if HTML/Markdown fails, retry as plain text
    (when (and (nil? result) parse-mode)
      (log/info :tg-fallback-plain-text)
      (call "sendMessage" (dissoc body "parse_mode")))
    result))

(defn edit-message
  "Edit an existing message.
   Options: :parse-mode (\"HTML\" default), :preview (true default), :reply_markup"
  [chat-id message-id text & {:keys [parse-mode preview reply_markup]
                              :or {parse-mode "HTML" preview true}}]
  (let [safe-text (fmt/rewrite-markdown-tables text)
        body (cond-> {"chat_id" (str chat-id)
                      "message_id" (str message-id)
                      "text" safe-text}
               parse-mode (assoc "parse_mode" parse-mode)
               (false? preview) (assoc "disable_web_page_preview" true)
               reply_markup (assoc "reply_markup" reply_markup))]
    (call "editMessageText" body)))

(defn answer-callback-query
  "Answer a callback query to stop Telegram's loading spinner.
   Must be called ASAP after receiving callback_query — Telegram shows
   a spinning hourglass until answered.

   Options:
     :text    — optional toast text shown to user (max 200 chars)
     :show-alert — if true, shows alert dialog instead of toast (default false)

   Works in both polling and webhook mode. In webhook mode the response body
   can also answer it, but direct API call is safer and mode-agnostic."
  [callback-query-id & {:keys [text show-alert]}]
  (let [body (cond-> {"callback_query_id" (str callback-query-id)}
               text (assoc "text" text)
               show-alert (assoc "show_alert" true))]
    (call "answerCallbackQuery" body)))

(defn send-typing
  "Send typing indicator to chat."
  [chat-id]
  (call "sendChatAction" {"chat_id" (str chat-id) "action" "typing"}))

(defn delete-message
  "Delete a message (bot must have permission)."
  [chat-id message-id]
  (call "deleteMessage" {"chat_id" (str chat-id) "message_id" (str message-id)}))

;; ══════════════════════ KEYBOARD ══════════════════════

(defn reset-keyboard
  "Create ReplyKeyboardMarkup with a single '🔄 Новый диалог' button.
   Tapping it sends '/reset' as text — the command handler catches it.
   Pass as :reply_markup to send-message or send-md.

   Options:
     :label      — button text (default \"🔄 Новый диалог\")
     :resize?    — resize keyboard to fit (default true)
     :one-time?  — hide after tap (default true)"
  [& {:keys [label resize? one-time?]
      :or {label "🔄 Новый диалог"
           resize? true
           one-time? true}}]
  {"keyboard"       [[{"text" label}]]
   "resize_keyboard"  (boolean resize?)
   "one_time_keyboard" (boolean one-time?)})

(defn location-keyboard
  "Keyboard with a single '📍 Отправить геолокацию' button that requests user location.
   Returns ReplyKeyboardMarkup for send-message :reply_markup."
  [& {:keys [label resize?]
      :or {label "📍 Отправить геолокацию"
           resize? true}}]
  {"keyboard"       [[{"text" label "request_location" true}]]
   "resize_keyboard"  (boolean resize?)})

(defn inline-button
  "Create an inline keyboard button map.
   (inline-button \"📞 Call\" \"call_venue_1\")     ;; callback version
   (inline-button \"📞 Call\" {:url \"tel:...\"}) ;; URL version"
  [text opts]
  (if (map? opts)
    (cond-> {"text" text}
      (:url opts) (assoc "url" (:url opts))
      (:callback_data opts) (assoc "callback_data" (:callback_data opts)))
    ;; String shorthand → callback_data
    {"text" text "callback_data" opts}))

(defn inline-keyboard
  "Build InlineKeyboardMarkup from rows of buttons.
   Each button is [label {:keys [url callback_data]}] or [label callback-data].
   Example:
   (inline-keyboard [[\"📞 Позвонить\" {:url \"tel:+996...\"}]
                     [\"🗺 2GIS\" {:url \"https://2gis.kg/...\"}]])"
  [& rows]
  {"inline_keyboard"
   (mapv (fn [row]
           (mapv (fn [btn]
                   (let [[label opts] (if (string? (second btn))
                                        [(first btn) {:callback_data (second btn)}]
                                        [(first btn) (second btn)])]
                     (inline-button label opts)))
                 row))
         rows)})

(defn edit-reply-markup
  "Edit only the reply markup of a message — useful for adding inline buttons
   after streaming completes."
  [chat-id message-id markup]
  (call "editMessageReplyMarkup"
        {"chat_id" chat-id
         "message_id" message-id
         "reply_markup" (json/generate-string markup)}))

(defn send-md
  "Send LLM markdown text — converts to HTML, splits if needed.
   Options: :reply_markup passed through to send-message.
   Returns sequence of sent Message objects."
  [chat-id text & {:keys [reply_markup]}]
  (if (str/blank? text)
    (do (log/warn :empty-response) nil)
    (let [html (fmt/md->html text)
          chunks (fmt/split-message html)]
      (doall
       (map (fn [chunk]
              (send-message chat-id chunk :parse-mode "HTML" :preview false :reply_markup reply_markup))
            chunks)))))

;; ══════════════════════ POLLING ══════════════════════

(defn get-updates
  "Fetch updates via long polling.
   Options:
     :offset  — update_id offset (only newer)
     :timeout — long poll timeout in seconds (default 30)
     :limit   — max updates (default 10)"
  [& {:keys [offset timeout limit]
      :or {timeout 45 limit 10}}]
  (let [body (cond-> {"timeout" timeout "limit" limit}
               offset (assoc "offset" offset))]
    (call "getUpdates" body :timeout-ms 70000)))

(defn- parse-update
  "Extract chat-id, user-id, first-name, text, location from update."
  [update]
  (when-let [msg (or (get update "message") (get update "edited_message"))]
    (let [chat (get msg "chat")
          user (get msg "from")
          loc (get msg "location")]
      (when loc
        (log/info :parse-update-location :has-location true :lat (get loc "latitude") :lon (get loc "longitude")))
      (cond->
       {:chat-id    (get chat "id")
        :user-id    (get user "id")
        :first-name (get user "first_name" "друг")
        :text       (get msg "text")
        :message-id (get msg "message_id")}
       loc
       (assoc :location {:lat (get loc "latitude")
                         :lon (get loc "longitude")})))))

(defn poll-loop
  "Start polling loop. Calls handler-fn for each message.
   handler-fn receives: {:keys [chat-id user-id first-name text]}

   Returns immediately — runs on current thread.
   Wrap in (future ...) or Thread/start for async."
  [handler-fn & {:keys [interval-ms] :or {interval-ms 1500}}]
  (let [init (get-updates :offset -1 :limit 1 :timeout 1)
        offset (atom (if-let [u (first (get init "result" []))]
                       (inc (get u "update_id")) 0))]
    (log/info :poll-start :offset @offset)
    (while true
      (try
        (let [resp (get-updates :offset @offset)]
          (doseq [u (get resp "result" [])]
            (try
              (when-let [parsed (parse-update u)]
                (handler-fn parsed))
              (catch Exception e (log/error e :handler-error)))
            (reset! offset (inc (get u "update_id")))))
        (catch Exception e (log/error e :poll-error)))
      (Thread/sleep interval-ms))))

;; ══════════════════════ BOT HANDLER PATTERN ══════════════════════

(defn make-handler
  "Create a standard Telegram message handler with command/fast-path/agent dispatch.

   Options:
     :commands  — map of {\"/start\" handler-fn} where handler-fn receives
                  {:keys [chat-id user-id first-name text]}
     :fast-path — map of {\"привет\" response-text} for no-LLM responses
                  (response-text can be a string or HTML-formatted string)
     :agent-fn  — (fn [user-id text]) → response-text for all other messages.
                  Or with streaming: (fn [user-id text {:keys [stream-cb]}]) → string-or-nil
     :on-location — (fn [chat-id user-id lat lon]) called when user shares location
     :on-error  — (fn [chat-id e]) called on handler exceptions
     :streaming? — enable progressive message editing via stream-cb (default false)
     :reply-markup — Telegram keyboard map (ReplyKeyboardMarkup or InlineKeyboardMarkup)
                     to attach to placeholder and final message. Or fn: (fn [chat-id]) → keyboard.
     :post-stream — (fn [chat-id user-id msg-id final-text]) called after streaming completes.
                    Use for adding inline buttons or follow-up messages.

   Returns a handler fn suitable for poll-loop.

   Example:
   (def handler
     (tg/make-handler
       {:commands {\"/start\" (fn [m] (str \"Привет, \" (:first-name m)))}
        :fast-path {\"привет\" \"👋 Салам!\"}
        :agent-fn (fn [uid text] (agent/ask uid text))}))

   (tg/poll-loop handler)"
  [{:keys [commands fast-path agent-fn on-error on-location streaming?
           reply-markup post-stream]
    :or {streaming? false}}]
  (let [call-count (atom 0)]
    (fn [{:keys [chat-id user-id first-name text location] :as msg}]
      (let [call-n (swap! call-count inc)
            text (str/trim (or text ""))
            dialogue-id (str chat-id "-" (System/currentTimeMillis))]
        (log/info :handler-called :count call-n :chat-id chat-id :user first-name :text (subs text 0 (min 60 (count text))) :has-location (boolean location))
        (observe/record!
         {:type :msg-in :dialogue-id dialogue-id
          :user first-name :text text})
        (try
          (cond
            ;; Location shared
            location
            (when on-location
              (let [lat (:lat location)
                    lon (:lon location)]
                (log/info :location-received :chat-id chat-id :lat lat :lon lon)
                (on-location chat-id user-id lat lon)))

            (str/blank? text)
            nil

            ;; Commands
            (str/starts-with? text "/")
            (let [[cmd _] (str/split text #"\s+" 2)
                  handler (get commands cmd)]
              (if handler
                (let [resp (handler msg)]
                  (when resp (send-message chat-id resp)))
                (send-message chat-id "Неизвестная команда. Напиши /help.")))

            ;; Fast-path words
            (get fast-path (str/lower-case text))
            (send-message chat-id (get fast-path (str/lower-case text)))

            ;; Agent
            agent-fn
            (if streaming?
              ;; Streaming mode: status → block-buffered previews → final HTML
              (let [txt (volatile! "")
                    msg-id (volatile! nil)
                    last-preview (volatile! "")
                    status-cb (fn [status-text]
                                (when-let [mid @msg-id]
                                  (try
                                    (vreset! txt "")
                                    (vreset! last-preview status-text)
                                    (edit-message chat-id mid status-text
                                                  :parse-mode nil :preview false)
                                    (catch Exception e
                                      (log/warn e :stream-status-edit-fail :msg-id mid)))))
                    stream-cb (fn [delta]
                                (vswap! txt str delta)
                                (when-let [mid @msg-id]
                                  (try
                                    (let [preview (fmt/streaming-preview @txt)]
                                      (when (and (seq preview) (not= preview @last-preview))
                                        (edit-message chat-id mid preview
                                                      :parse-mode nil :preview false)
                                        (vreset! last-preview preview)))
                                    (catch Exception e
                                      (log/warn e :stream-edit-fail :msg-id mid :text-len (count @txt))))))
                    placeholder (send-message chat-id "🧠 Анализирую запрос…"
                                              :parse-mode nil)
                    mid-val (get-in placeholder ["result" "message_id"])]
                (log/info :stream-placeholder-sent :msg-id mid-val)
                (vreset! msg-id mid-val)
                (let [result (agent-fn user-id text {:stream-cb stream-cb :status-cb status-cb})
                      final-text (or result @txt)]
                  ;; Final edit renders full Markdown as Telegram HTML.
                  (if result
                    (edit-message chat-id @msg-id (fmt/md->html result)
                                  :parse-mode "HTML" :preview false)
                    ;; Agent streamed without returning full text — finalize accumulated content.
                    (if (str/blank? @txt)
                      (edit-message chat-id @msg-id "Извините, не получилось ответить." :parse-mode nil)
                      (edit-message chat-id @msg-id (fmt/md->html @txt)
                                    :parse-mode "HTML" :preview false)))
                  ;; Post-stream callback — for inline buttons, follow-up messages
                  (when post-stream
                    (try
                      (post-stream chat-id user-id @msg-id (str final-text))
                      (catch Exception e
                        (log/warn e :post-stream-error))))
                  (observe/record!
                   {:type :msg-out :dialogue-id dialogue-id
                    :text-len (count (str final-text))})))
              ;; Non-streaming fallback
              (do
                (let [t0 (System/currentTimeMillis)
                      resp (agent-fn user-id text)
                      elapsed (- (System/currentTimeMillis) t0)]
                  (when resp
                    (send-md chat-id resp)
                    (observe/record!
                     {:type :msg-out :dialogue-id dialogue-id
                      :text-len (count resp) :total-elapsed elapsed})))))

            :else
            (log/warn :no-handler :text text))
          (catch Exception e
            (log/error e :handler-error)
            (when on-error (on-error chat-id e))
            (try (send-message chat-id "❌ Произошла ошибка." :parse-mode nil)
                 (catch Exception _))
            nil))))))

(defn start-polling
  "Convenience: start bot polling with a make-handler config.

   Example:
   (tg/start-polling
     {:commands {\"/start\" handle-start}
      :agent-fn agent-ask-fn
      :streaming? true})"
  [handler-config & {:keys [interval-ms] :or {interval-ms 1500}}]
  (poll-loop (make-handler handler-config) :interval-ms interval-ms))
