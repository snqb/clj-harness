(ns clj-harness.infra
  "Shared infrastructure: config, secrets, HTTP client.
   Bottom of the dependency chain — required by core, mcp, and others."
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [aero.core :as aero])
  (:import
   [java.net URI]
   [java.net.http HttpClient HttpClient$Version HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
   [java.time Duration]))

;; ══════════════════════ CONFIG ══════════════════════

(def config
  "Aero config from resources/config.edn on classpath."
  (aero/read-config (io/resource "config.edn")))

(defn cfg
  "Read nested config key. (cfg :agent :max-turns) => 10"
  [& path]
  (get-in config (vec path)))

;; ══════════════════════ SECRETS ══════════════════════

(defn read-api-key
  "Read API key: env var or pass store.
   (read-api-key :openrouter)  ;; checks OPENROUTER_API_KEY or pass openrouter/token
   (read-api-key :deepseek)    ;; checks DEEPSEEK_API_KEY or pass deepseek-api/token"
  ([provider]
   (case provider
     :openrouter (or (System/getenv "OPENROUTER_API_KEY")
                     (try (-> (shell/sh "pass" "show" "openrouter/token" :out :string) :out str/trim)
                          (catch Exception _ nil)))
     :deepseek   (or (System/getenv "DEEPSEEK_API_KEY")
                     (try (-> (shell/sh "pass" "show" "deepseek-api/token" :out :string) :out str/trim)
                          (catch Exception _ nil)))
     (or (System/getenv "OPENROUTER_API_KEY")
         (try (-> (shell/sh "pass" "show" "openrouter/token" :out :string) :out str/trim)
              (catch Exception _ nil))))))

;; ══════════════════════ HTTP ══════════════════════

(defn http-post
  "Raw Java HttpClient (HTTP/1.1). Works everywhere — MCPvisor, OpenRouter, Telegram.
   Returns parsed JSON with string keys.
   
   (http-post \"https://api.example.com/chat\" body-str 
              :headers {\"Authorization\" \"Bearer sk-...\"}
              :timeout-ms 60000)"
  [url body-str & {:keys [headers timeout-ms] :or {timeout-ms 60000}}]
  (let [client (-> (HttpClient/newBuilder)
                   (.version HttpClient$Version/HTTP_1_1)
                   (.connectTimeout (Duration/ofMillis timeout-ms))
                   .build)
        builder (doto (HttpRequest/newBuilder (URI/create url))
                  (.timeout (Duration/ofMillis timeout-ms))
                  (.header "Content-Type" "application/json"))
        _ (doseq [[k v] (merge {"Content-Type" "application/json"} headers)]
            (.header builder k v))
        _ (.POST builder (HttpRequest$BodyPublishers/ofString body-str))
        req (.build builder)
        resp (.send client req (HttpResponse$BodyHandlers/ofString))]
    (if (>= (.statusCode resp) 400)
      (throw (ex-info (str "HTTP " (.statusCode resp))
                      {:status (.statusCode resp) :body (.body resp) :url url}))
      (json/parse-string (str/trim (.body resp)) false))))
