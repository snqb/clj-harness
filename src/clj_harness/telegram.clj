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
            [clojure.tools.logging :as log])
  (:import [java.net URI URLEncoder]
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

   Returns Telegram Message object or nil on failure."
  [chat-id text & {:keys [parse-mode preview reply-to]
                   :or {parse-mode "HTML" preview true}}]
  (let [body (cond-> {"chat_id" (str chat-id) "text" text}
               parse-mode (assoc "parse_mode" parse-mode)
               (false? preview) (assoc "disable_web_page_preview" true)
               reply-to (assoc "reply_to_message_id" (str reply-to)))
        result (call "sendMessage" body)]
    ;; Fallback: if HTML/Markdown fails, retry as plain text
    (when (and (nil? result) parse-mode)
      (log/info :tg-fallback-plain-text)
      (call "sendMessage" (dissoc body "parse_mode")))
    result))

(defn edit-message
  "Edit an existing message.
   Options: :parse-mode (\"HTML\" default), :preview (true default)"
  [chat-id message-id text & {:keys [parse-mode preview]
                              :or {parse-mode "HTML" preview true}}]
  (let [body (cond-> {"chat_id" (str chat-id)
                      "message_id" (str message-id)
                      "text" text}
               parse-mode (assoc "parse_mode" parse-mode)
               (false? preview) (assoc "disable_web_page_preview" true))]
    (call "editMessageText" body)))

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

(defn- hide-keyboard
  "RemoveReplyKeyboard markup — hides custom keyboard after message."
  []
  {"remove_keyboard" true})

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
       (map-indexed
        (fn [i chunk]
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
  "Extract chat-id, user-id, first-name, text from update."
  [update]
  (when-let [msg (get update "message")]
    (let [chat (get msg "chat")
          user (get msg "from")]
      {:chat-id    (get chat "id")
       :user-id    (get user "id")
       :first-name (get user "first_name" "друг")
       :text       (get msg "text")
       :message-id (get msg "message_id")})))

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
     :agent-fn  — (fn [user-id text]) → response-text for all other messages
     :on-error  — (fn [chat-id e]) called on handler exceptions

   Returns a handler fn suitable for poll-loop.

   Example:
   (def handler
     (tg/make-handler
       {:commands {\"/start\" (fn [m] (str \"Привет, \" (:first-name m)))}
        :fast-path {\"привет\" \"👋 Салам!\"}
        :agent-fn (fn [uid text] (agent/ask uid text))}))

   (tg/poll-loop handler)"
  [{:keys [commands fast-path agent-fn on-error]}]
  (fn [{:keys [chat-id user-id first-name text] :as msg}]
    (try
      (let [text (str/trim text)]
        (cond
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
          (let [resp (agent-fn user-id text)]
            (when resp (send-md chat-id resp)))

          :else
          (log/warn :no-handler :text text)))
      (catch Exception e
        (log/error e :handler-error)
        (when on-error (on-error chat-id e))
        (try (send-message chat-id "❌ Произошла ошибка." :parse-mode nil)
             (catch Exception _))
        nil))))

(defn start-polling
  "Convenience: start bot polling with a make-handler config.

   Example:
   (tg/start-polling
     {:commands {\"/start\" handle-start}
      :fast-path {\"привет\" greeting}
      :agent-fn agent-ask-fn})"
  [handler-config & {:keys [interval-ms] :or {interval-ms 1500}}]
  (poll-loop (make-handler handler-config) :interval-ms interval-ms))
