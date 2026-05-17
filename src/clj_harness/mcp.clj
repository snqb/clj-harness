(ns clj-harness.mcp
  "MCPvisor client — tool discovery, execution, and schema conversion.

   Dependencies: clj-harness.infra (http-post, cfg).

   Usage:
     (mcp-call :get_today_date)
     (mcp-call :search_tours {:city 80})
     (list-mcp-tools)
     (mcp-tool->openai-schema tool-def)"
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]
   [clj-harness.infra :refer [http-post cfg]]))

;; ══════════════════════ TOOL CACHE ══════════════════════

(defonce ^:private tool-cache (atom nil))

;; ══════════════════════ MCP CALLS ══════════════════════

(defn mcp-call
  "Call MCPvisor tool via JSON-RPC.
   (mcp-call :get_today_date)          ;; no args
   (mcp-call :search_tours {:city 80}) ;; with args"
  ([tool-name] (mcp-call tool-name {}))
  ([tool-name args]
   (let [r (http-post (str (cfg :mcp-url) "/")
                      (json/generate-string
                       {"jsonrpc" "2.0" "id" (rand-int 99999) "method" "tools/call"
                        "params" {"name" (name tool-name) "arguments" args}})
                      :timeout-ms 30000)]
     (if-let [err (get-in r ["error" "message"])]
       (throw (ex-info (str "MCP error: " err) {:tool tool-name :args args}))
       (->> (get-in r ["result" "content"])
            (filter #(= "text" (get % "type")))
            (map #(get % "text"))
            (str/join "\n"))))))

(defn list-mcp-tools
  "Discover available MCP tools. Results cached after first call.
   Returns vector of MCP tool definitions with string keys (name, description, inputSchema)."
  []
  (when-not @tool-cache
    (let [r (http-post (str (cfg :mcp-url) "/")
                       (json/generate-string
                        {"jsonrpc" "2.0" "id" 1 "method" "tools/list" "params" {}}))]
      (reset! tool-cache (get-in r ["result" "tools"]))))
  @tool-cache)

;; ══════════════════════ SCHEMA CONVERSION ══════════════════════

(defn mcp-tool->openai-schema
  "Convert MCP tool definition to OpenAI function-calling schema.
   Strips 'default' key from parameters (unsupported by some providers)."
  [tool-def]
  (let [schema (or (get tool-def "inputSchema") {"type" "object" "properties" {}})]
    {"type" "function"
     "function" {"name" (get tool-def "name")
                 "description" (get tool-def "description" "")
                 "parameters" (dissoc schema "default")}}))
