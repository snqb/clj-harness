(ns clj-harness.stream
  "Streaming LLM client — SSE chunk parsing for live output.

   Supports OpenRouter and DeepSeek streaming endpoints.
   Returns a core.async channel of delta chunks.

   Usage:
     (require '[clj-harness.stream :as stream])
     (def ch (stream/llm-stream
              [{:role \"user\" :content \"Hello\"}]
              :model :deepseek-chat :provider :deepseek))
     ;; Read chunks: {:delta \"text\"} {:finish \"stop\"} {:done :timeout}
     (loop [chunk (<!! ch)]
       (when (:delta chunk) (print (:delta chunk)))
       (when (not (:done chunk)) (recur (<!! ch))))"
  (:require
   [cheshire.core :as json]
   [clojure.core.async :as a :refer [chan go >! close! <!]]
   [clojure.string :as str]
   [clojure.java.shell :as shell]
   [clojure.tools.logging :as log])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.time Duration]
   [java.util.stream Collectors]
   [java.util.concurrent CompletableFuture TimeUnit]))

;; ══════════════════════ Config ══════════════════════

(def ^:private provider-config
  {:openrouter {:url "https://openrouter.ai/api/v1/chat/completions"
                :headers-fn (fn [key] {"Authorization" (str "Bearer " key)
                                       "HTTP-Referer" "http://localhost"
                                       "X-Title" "CljHarness"})}
   :deepseek   {:url "https://api.deepseek.com/chat/completions"
                :headers-fn (fn [key] {"Authorization" (str "Bearer " key)})}})

(defn- read-api-key [provider]
  (case provider
    :openrouter (or (System/getenv "OPENROUTER_API_KEY")
                    (try (-> (shell/sh "pass" "show" "openrouter/token" :out :string) :out str/trim)
                         (catch Exception _ nil)))
    :deepseek   (or (System/getenv "DEEPSEEK_API_KEY")
                    (try (-> (shell/sh "pass" "show" "deepseek-api/token" :out :string) :out str/trim)
                         (catch Exception _ nil)))
    (or (System/getenv "OPENROUTER_API_KEY")
        (try (-> (shell/sh "pass" "show" "openrouter/token" :out :string) :out str/trim)
             (catch Exception _ nil)))))

(defn- resolve-model
  "Resolve model key to full model name for stream API."
  [model-key]
  (case (keyword model-key)
    (:claude-sonnet :claude-3.5-sonnet :claude-sonnet-20241022) "anthropic/claude-3.5-sonnet"
    (:gemini-flash :gemini-2.0-flash) "google/gemini-2.0-flash-001"
    (:gemini-pro :gemini-2.5-pro) "google/gemini-2.5-pro-exp-03-25:free"
    :deepseek-chat "deepseek-chat"
    :deepseek-reasoner "deepseek-reasoner"
    (name model-key)))

;; ══════════════════════ SSE Parser ══════════════════════

(defn- parse-sse-line
  "Parse one SSE data: line into a chunk map.
   Returns {:delta \"text\" :finish \"stop\"} or nil for empty/skip lines."
  [^String line]
  (when (and line (.startsWith line "data:"))
    (let [json-str (.substring line 5)]
      (if (= json-str " [DONE]")
        {:finish "done"}
        (try
          (let [data (json/parse-string json-str false)
                choice (first (get data "choices"))
                delta (get choice "delta")
                content (get delta "content")
                finish-reason (get choice "finish_reason")]
            (cond-> {}
              content (assoc :delta content)
              finish-reason (assoc :finish finish-reason)))
          (catch Exception _
            nil))))))

;; ══════════════════════ Streaming HTTP ══════════════════════

(defn- http-stream-lines
  "Send streaming POST request, return a channel of raw SSE lines.
   Uses Java HttpClient async + ofLines BodyHandler."
  [url body-str headers timeout-ms]
  (let [out (chan 64)
        client (HttpClient/newBuilder)
        _ (.connectTimeout client (Duration/ofMillis timeout-ms))
        client (.build client)
        builder (doto (HttpRequest/newBuilder (URI/create url))
                  (.timeout (Duration/ofMillis timeout-ms))
                  (.header "Content-Type" "application/json"))
        _ (doseq [[k v] headers] (.header builder k v))
        _ (.POST builder (HttpRequest$BodyPublishers/ofString body-str))
        req (.build builder)
        future (.sendAsync client req (HttpResponse$BodyHandlers/ofLines))]
    ;; Feed lines into channel as they arrive
    (go
      (try
        (let [resp (.get future 120 TimeUnit/SECONDS)
              lines (.collect (.body resp) (Collectors/toList))]
          (doseq [^String line lines]
            (when (and line (.startsWith line "data:"))
              (>! out line))))
        (catch Exception e
          (log/error e :stream-error (.getMessage e))
          (>! out (str "data: {\"error\":\"" (.getMessage e) "\"}")))
        (finally
          (close! out))))
    out))

;; ══════════════════════ Public API ══════════════════════

(defn llm-stream
  "Stream LLM response as delta chunks.

   Args:
     messages  — vector of message maps [{:role \"user\" :content \"...\"}]
     tools     — optional vector of tool schemas
     :model    — model key (:deepseek-chat, :claude-sonnet, etc.)
     :provider — :deepseek (default) or :openrouter
     :api-key  — override auto-detected API key
     :max-tokens — max tokens (default 2048)
     :on-chunk — optional callback (fn [chunk]) called for each delta
     :on-done  — optional callback called when stream completes

   Returns core.async channel. Each value is:
     {:delta \"text chunk\"}  — partial response text
     {:finish \"stop\"}        — stream finished naturally
     {:error \"msg\"}           — error occurred
     {:done :closed}           — channel closed (always last)"
  [messages & {:keys [tools model provider api-key max-tokens on-chunk on-done]
               :or {model :deepseek-chat provider :deepseek max-tokens 4096}}]
  (let [out (chan 64)
        key (or api-key (read-api-key provider))
        pc (get provider-config provider)
        url (:url pc)
        headers ((:headers-fn pc) key)
        payload (cond-> {"model" (resolve-model model)
                         "messages" messages
                         "max_tokens" max-tokens
                         "stream" true}
                  (seq tools) (assoc "tools" tools "tool_choice" "auto"))
        line-ch (http-stream-lines url (json/generate-string payload) headers 180000)]
    (go
      (try
        (loop []
          (when-let [line (<! line-ch)]
            (let [chunk (parse-sse-line line)]
              (when chunk
                (when (and on-chunk (:delta chunk))
                  (try (on-chunk chunk) (catch Exception _)))
                (>! out chunk)
                ;; If we got a finish, break out (but let DONE pass through)
                (when (and (:finish chunk) (= "done" (:finish chunk)))
                  (>! out {:done :completed}))))
            (recur)))
        (catch Exception e
          (log/error e :stream-read-error)
          (>! out {:error (.getMessage e)}))
        (finally
          (>! out {:done :closed})
          (close! out)
          (when on-done (try (on-done) (catch Exception _))))))
    out))

;; ══════════════════════ Convenience ══════════════════════

(defn stream-to-string
  "Collect stream into a single string, blocking.
   Useful for REPL testing."
  [messages & opts]
  (let [ch (apply llm-stream messages opts)
        sb (StringBuilder.)]
    (loop []
      (let [chunk (a/<!! ch)]
        (when chunk
          (when (:delta chunk) (.append sb (:delta chunk)))
          (when (not (:done chunk)) (recur)))))
    (str sb)))

;; ══════════════════════ REPL ══════════════════════

(comment
  (require '[clojure.core.async :as a])

  ;; Simple stream
  (def ch (llm-stream [{:role "user" :content "Say hello in 3 words"}]
                      :model :deepseek-chat :provider :deepseek :max-tokens 20))
  (loop [chunk (a/<!! ch)]
    (when chunk
      (when (:delta chunk) (print (:delta chunk)) (flush))
      (when (not (:done chunk)) (recur (a/<!! ch)))))

  ;; With callback
  (def ch2 (llm-stream [{:role "user" :content "Count from 1 to 10"}]
                       :model :deepseek-chat :max-tokens 100
                       :on-chunk #(print (:delta %))))

  ;; Blocking collection
  (stream-to-string [{:role "user" :content "What is 2+2? Answer with just the number."}]
                    :model :deepseek-chat :max-tokens 10))
