(ns clj-harness.dashboard
  "Embedded observability panel — zero new dependencies.
   Uses JDK's built-in com.sun.net.httpserver to serve a single-page
   web dashboard on a configurable port.

   Start with (start! port), stop with (stop! server).
   Wire into create-bot via :dashboard {:port 8089}."
  (:require
   [cheshire.core :as json]
   [clj-harness.observe :as observe]
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import
   [com.sun.net.httpserver HttpServer HttpExchange HttpHandler]
   [java.net InetSocketAddress]
   [java.util.concurrent Executors]))

;; ══════════════════════ STATE ══════════════════════

(defonce ^:private server-atom (atom nil))

(declare stop!)

;; ══════════════════════ STATIC HTML ══════════════════════

(def ^:private dashboard-html
  "Lazy-loaded dashboard page from resources."
  (delay
    (if-let [r (io/resource "clj_harness/dashboard.html")]
      (slurp r)
      (str "<!DOCTYPE html><html><body><h1>dashboard.html not found</h1>"
           "<p>Place it in resources/clj_harness/dashboard.html</p></body></html>"))))

;; ══════════════════════ HANDLERS ══════════════════════

(defn- send-json [^HttpExchange ex status data]
  (let [body (json/generate-string data)
        bytes (.getBytes body "UTF-8")]
    (.getResponseHeaders ex)
    (doto (.getResponseHeaders ex)
      (.set "Content-Type" "application/json; charset=utf-8")
      (.set "Access-Control-Allow-Origin" "*"))
    (.sendResponseHeaders ex (int status) (long (alength bytes)))
    (with-open [os (.getResponseBody ex)]
      (.write os bytes))))

(defn- handle-stats [^HttpExchange ex]
  (send-json ex 200 (observe/compute-stats)))

(defn- query-param [^HttpExchange ex k]
  (when-let [q (.getQuery (.getRequestURI ex))]
    (some->> (str/split q #"&")
             (map #(str/split % #"=" 2))
             (filter #(= k (first %)))
             first second
             (java.net.URLDecoder/decode "UTF-8"))))

(defn- handle-events-json [^HttpExchange ex]
  (let [n (try
            (Integer/parseInt (or (query-param ex "n") "100"))
            (catch Exception _ 100))]
    (send-json ex 200 (observe/recent n))))

(defn- handle-sse [^HttpExchange ex]
  (let [poll (observe/sse-subscriber)]
    (.getResponseHeaders ex)
    (doto (.getResponseHeaders ex)
      (.set "Content-Type" "text/event-stream; charset=utf-8")
      (.set "Cache-Control" "no-cache")
      (.set "Connection" "keep-alive")
      (.set "Access-Control-Allow-Origin" "*"))
    (.sendResponseHeaders ex 200 0)
    (with-open [os (.getResponseBody ex)]
      (try
        ;; Send initial batch
        (doseq [e (observe/recent 50)]
          (let [line (str "data: " (json/generate-string e) "\n\n")]
            (.write os (.getBytes line "UTF-8"))))
        (.flush os)
        ;; Poll loop — push new events every second
        (while true
          (let [new-events (poll)]
            (when (seq new-events)
              (doseq [e new-events]
                (let [line (str "data: " (json/generate-string e) "\n\n")]
                  (.write os (.getBytes line "UTF-8"))))
              (.flush os)))
          (Thread/sleep 1000))
        (catch Exception _
          ;; Client disconnected — expected, clean exit
          )))))

(defn- handle-root [^HttpExchange ex]
  (let [html @dashboard-html
        bytes (.getBytes html "UTF-8")]
    (.getResponseHeaders ex)
    (doto (.getResponseHeaders ex)
      (.set "Content-Type" "text/html; charset=utf-8"))
    (.sendResponseHeaders ex 200 (long (alength bytes)))
    (with-open [os (.getResponseBody ex)]
      (.write os bytes))))

(defn- router
  "Top-level handler that routes by path."
  [^HttpExchange ex]
  (let [path (.getPath (.getRequestURI ex))]
    (cond
      (= path "/api/stats")   (handle-stats ex)
      (= path "/api/events")  (handle-events-json ex)
      (= path "/api/stream")  (handle-sse ex)
      :else                   (handle-root ex))))

;; ══════════════════════ PUBLIC API ══════════════════════

(defn start!
  "Start the observability dashboard on the given port.
   Returns the HttpServer instance.

   Options:
     :port  — port to listen on (default 8089)
     :host  — host to bind (default \"0.0.0.0\")"
  [& {:keys [port host] :or {port 8089 host "0.0.0.0"}}]
  (when @server-atom
    (stop!))
  (let [addr (InetSocketAddress. host port)
        server (HttpServer/create addr 0)]
    (.createContext server "/" (reify HttpHandler
                                 (handle [_ ex]
                                   (router ex))))
    (.setExecutor server (Executors/newFixedThreadPool 4))
    (.start server)
    (reset! server-atom server)
    (println (str "[dashboard] http://" host ":" port))
    server))

(defn stop!
  "Stop the dashboard server if running."
  []
  (when-let [s @server-atom]
    (.stop s 1)
    (reset! server-atom nil)
    (println "[dashboard] stopped")))
