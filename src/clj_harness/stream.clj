(ns clj-harness.stream
  "SSE streaming — non-blocking LLM streaming on dedicated Thread.
   Does NOT use core.async go-blocks for I/O — runs HTTP on raw Thread
   and pushes parsed deltas into a core.async channel.

   stream-agent is a standalone agent loop with streaming — call it instead
   of clj-harness.core/handle-message for real-time output."
  (:require
   [cheshire.core :as json]
   [clojure.core.async :refer [chan close! >!! <!!]]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [clj-harness.heap :as heap]
   [clj-harness.infra :as infra]
   [clj-harness.llm :as llm])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpClient$Version HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.time Duration]
   [java.io BufferedReader InputStreamReader]))

(defn- http-stream-lines
  "Performs an HTTP POST with JSON body and streams the response line-by-line
   into a core.async channel. Runs on a raw Thread (not a go-block) to avoid
   blocking the async thread pool."
  [url headers body]
  (let [ch (chan 256)]
    (.start
     (Thread.
      (reify Runnable
        (run [_]
          (try
            (let [client (-> (HttpClient/newBuilder)
                             (.connectTimeout (Duration/ofSeconds 30))
                             (.build))
                  req-builder (-> (HttpRequest/newBuilder)
                                  (.uri (URI. url))
                                  (.header "Content-Type" "application/json")
                                  (.version HttpClient$Version/HTTP_1_1)
                                  (.timeout (Duration/ofSeconds 120))
                                  (.POST (HttpRequest$BodyPublishers/ofString (json/generate-string body))))
                  req (.build (reduce (fn [r [k v]] (.header r k v)) req-builder headers))
                  resp (.send client req (HttpResponse$BodyHandlers/ofInputStream))
                  status (.statusCode resp)]
              (if (= status 200)
                (let [reader (BufferedReader. (InputStreamReader. (.body resp)))
                      lines (line-seq reader)]
                  (doseq [line lines]
                    (when (str/starts-with? line "data: ")
                      (let [data (subs line 6)]
                        (if (= "[DONE]" data)
                          (>!! ch {:done true})
                          (try
                            (let [parsed (json/parse-string data true)]
                              (>!! ch parsed))
                            (catch Exception e
                              (log/warn :stream-parse-error (.getMessage e))))))))
                  (>!! ch {:done "eof"}))
                (do
                  (log/error :stream-http-error status)
                  (>!! ch {:done (str "http-error-" status)}))))
            (catch Exception e
              (log/error e :stream-error)
              (>!! ch {:done "error"}))
            (finally
              (close! ch)))))
      (str "http-stream-" (hash url))))
    ch))

(defn- provider-stream-config
  "Returns {:url ... :api-key ... :model ...} for a given provider."
  [provider-key]
  (let [cfg (case provider-key
              :deepseek {:url "https://api.deepseek.com/v1/chat/completions"
                         :api-key (infra/read-api-key :deepseek)}
              :openrouter {:url "https://openrouter.ai/api/v1/chat/completions"
                           :api-key (infra/read-api-key :openrouter)})]
    cfg))

(defn llm-stream
  "Stream LLM response via SSE. Returns a core.async channel.

   Messages arrive as maps: {:delta \"text\"} {:tool-calls [...]} {:finish \"stop\"} {:done true}

   Parameters (keyword args):
     :model    — model string (e.g. \"deepseek-v4-pro\")
     :messages — vector of message maps
     :tools    — optional tool definitions (OpenAI format)
     :provider — :deepseek (default) or :openrouter
     :max-tokens — default 4096"
  [& {:keys [model messages tools provider max-tokens]
      :or {provider :deepseek max-tokens 4096}}]
  (let [cfg (provider-stream-config provider)
        headers (merge {"Authorization" (str "Bearer " (:api-key cfg))}
                       (case provider
                         :openrouter {"HTTP-Referer" "http://localhost"}
                         {}))
        body (cond-> {"model" model "messages" messages
                      "max_tokens" max-tokens "stream" true}
               (seq tools) (assoc "tools" tools "tool_choice" "auto"))]
    (log/info :stream-start :provider provider :model model
              :msgs (count messages) :tools (count tools))
    (http-stream-lines (:url cfg) headers body)))

;; ══════════════════════ STREAMING AGENT ══════════════════════

(defn- consume-stream
  "Consume a llm-stream channel: parse OpenAI SSE chunks, accumulate
   content, tool_calls, and call stream-cb.
   Returns {:content ... :tool-calls ... :finish ...}.
   Blocks until stream completes."
  [ch stream-cb]
  (let [content-sb (StringBuilder.)
        tc-chunks (atom {})
        finish-reason (atom nil)]
    (loop []
      (let [msg (<!! ch)]
        (when msg
          ;; Parse OpenAI chunk format: {:choices [{:delta {:content "..." :tool_calls [...]}}]}
          (let [choices (:choices msg)
                delta (when (seq choices) (:delta (first choices)))
                finish (when (seq choices) (:finish_reason (first choices)))]
            (when delta
              (when (:content delta)
                (.append content-sb (:content delta))
                (try (stream-cb (:content delta)) (catch Exception _)))
              (when (:tool_calls delta)
                (doseq [tc (:tool_calls delta)]
                  (let [idx (get tc :index 0)
                        args (get-in tc [:function :arguments])]
                    (swap! tc-chunks
                           (fn [chunks]
                             (let [chunk (or (get chunks idx) {})
                                   ;; Preserve :function :name, append :arguments
                                   func-name (or (get-in tc [:function :name])
                                                 (get-in chunk [:function :name]))
                                   cur-args (get-in chunk [:function :arguments] "")
                                   new-args (str cur-args (or args ""))
                                   merged (merge chunk (dissoc tc :function))
                                   updated (assoc merged :function {:name func-name :arguments new-args})]
                               (assoc chunks idx updated)))))))
              (when finish (reset! finish-reason finish))))
          (if (:done msg) nil (recur)))))
    (let [chunks @tc-chunks
          accumulated-tc (when (pos? (count chunks))
                           (vec (for [i (sort (keys chunks))]
                                  (get chunks i))))]
      {:content (.toString content-sb)
       :tool-calls (when (seq accumulated-tc) accumulated-tc)
       :finish (or @finish-reason "stop")})))

(defn stream-agent
  "Run agent with streaming LLM calls. Handles tool execution loop.

   Parameters:
     :model         — model key (e.g. :deepseek-v4-pro)
     :messages      — vector of message maps
     :tool-map      — map of tool-name → {:execute (fn [args] ...)}
     :tool-schemas  — OpenAI-format tool definitions
     :stream-cb     — (fn [text-fragment]) called for each content delta
     :provider      — :deepseek (default) or :openrouter
     :max-turns     — max tool-calling iterations (default 10)
     :max-tokens    — max tokens per LLM call (default 4096)
     :heap          — optional clj-harness.heap atom for tool result storage
                       When provided, large tool outputs (>2K chars) are stored
                       in heap and replaced with compact summary + heap-id.
                       fetch_result tool is automatically added to tool-schemas.

   Returns accumulated response string."
  [& {:keys [model messages tool-map tool-schemas stream-cb provider max-turns max-tokens heap]
      :or {provider :deepseek max-turns 10 max-tokens 4096}}]
  (if-not stream-cb
    ;; Non-streaming fallback: use regular LLM
    (let [resp (llm/llm model messages tool-schemas :provider provider :max-tokens max-tokens)
          choice (first (get resp "choices"))
          msg (get choice "message")]
      (or (get msg "content") "No response."))

    ;; Streaming path
    (let [;; Auto-add fetch_result tool if heap is active
          all-tool-schemas (if heap
                             (conj (or tool-schemas [])
                                   {"type" "function"
                                    "function" {"name" "fetch_result"
                                                "description" "Get full details from a previously stored tool result. Use this when you need to see specific items from a large search result that was stored in the heap."
                                                "parameters" {"type" "object"
                                                              "properties" {"heap_id" {"type" "string"
                                                                                       "description" "The heap ID reference from a previous tool result (e.g. heap:abc123)"}
                                                                            "query" {"type" "string"
                                                                                     "description" "Optional: filter results matching this query (e.g. 'Samsung' or 'under 20000')"}}
                                                              "required" ["heap_id"]}}})
                             (or tool-schemas []))
          all-tool-map (if heap
                         (assoc (or tool-map {})
                                "fetch_result"
                                {:execute (fn [args]
                                            (let [hid (get args "heap_id")
                                                  q (get args "query")]
                                              (if q
                                                (heap/fetch-with-query heap hid q)
                                                (or (heap/fetch heap hid)
                                                    (str "Heap entry " hid " not found or expired.")))))
                                 :name "fetch_result"
                                 :description "Get full details from a previously stored tool result"})
                         (or tool-map {}))]
      (loop [msgs messages turn 0]
        (if (>= turn max-turns)
          "⚠️ Reached max turns. Try a more specific query."
          (let [resp (consume-stream
                      (llm-stream :model (llm/resolve-model model)
                                  :messages msgs :tools all-tool-schemas
                                  :provider provider :max-tokens max-tokens)
                      stream-cb)
                content (:content resp)
                calls (:tool-calls resp)
                _ (log/info :stream-turn turn :content-len (count content) :calls-len (count calls))
                _ (when (seq calls)
                    (log/info :stream-tool-call (-> calls first :function :name)))]
            (if (seq calls)
              ;; Execute tools, continue loop
              (let [tool-results
                    (mapv (fn [tc]
                            (let [tn (get-in tc [:function :name])
                                  args-str (get-in tc [:function :arguments])
                                  args (try (if (string? args-str)
                                              (json/parse-string args-str false)
                                              args-str)
                                            (catch Exception _ {}))
                                  tool (get all-tool-map tn)
                                  result (if tool
                                           (try ((:execute tool) args)
                                                (catch Exception e
                                                  (str "Tool error: " (.getMessage e))))
                                           (str "Unknown tool: " tn))
                                  result-str (str result)
                                  ;; Heap storage: if heap active and result > 2K, store externally
                                  heap-ref (when heap
                                             (heap/store! heap tn result-str))
                                  ;; Format tool result for LLM
                                  fmt-result (if heap-ref
                                               (str (heap/extract-key-items result-str)
                                                    "\n\n📦 Stored in heap: " (:heap-id heap-ref)
                                                    " (" (:size heap-ref) " chars)."
                                                    " Use fetch_result to get full details.")
                                               ;; No heap: truncate to 8K as before
                                               (let [max-out 8000]
                                                 (if (> (count result-str) max-out)
                                                   (str (subs result-str 0 max-out)
                                                        "\n...(truncated)")
                                                   result-str)))]
                              {"role" "tool"
                               "tool_call_id" (:id tc)
                               "content" fmt-result}))
                          calls)
                    ;; Assistant message: nil content if only tool_calls
                    asst-msg (cond-> {"role" "assistant" "tool_calls"
                                      (mapv (fn [tc]
                                              {"id" (:id tc)
                                               "type" "function"
                                               "function" {"name" (get-in tc [:function :name])
                                                           "arguments" (get-in tc [:function :arguments] "")}})
                                            calls)}
                               (not (str/blank? content))
                               (assoc "content" content))]
                (recur (into (conj msgs asst-msg) tool-results)
                       (inc turn)))

              ;; No tool calls — final response
              content)))))))

(comment
  ;; Usage example:
  (def ch (llm-stream
           :model "deepseek-v4-pro"
           :messages [{"role" "user" "content" "Say hello in Russian"}]
           :provider :deepseek))

  (loop [msg (<!! ch)]
    (when msg
      (when (:delta msg) (print (:delta msg)) (flush))
      (when-not (:done msg) (recur (<!! ch)))))

  ;; With stream-agent:
  (def buf (StringBuilder.))
  (stream-agent
   :model :deepseek-v4-pro
   :messages [{"role" "user" "content" "Say hello"}]
   :tool-map {}
   :tool-schemas []
   :stream-cb (fn [delta] (.append buf delta))
   :provider :deepseek
   :max-turns 2))
