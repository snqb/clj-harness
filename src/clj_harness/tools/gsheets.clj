(ns clj-harness.tools.gsheets
  "Google Sheets tool for booking/scheduling. Writes rows to a Google Sheet.
   Uses gcloud Application Default Credentials for auth.

   Tool: book_slot — appends a row with [date, time, name, phone, service, status]

   Setup:
   1. Create Google Cloud project, enable Sheets API
   2. Run: gcloud auth application-default login
   3. Share your Google Sheet with the authenticated account (Editor)
   4. Pass the sheet ID to this tool"
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [clojure.java.shell :as shell])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.net.http HttpClient$Version]
           [java.time Duration LocalDateTime]
           [java.time.format DateTimeFormatter]))

;; ── Append Row ──

(defn- sheets-api-call [access-token spreadsheet-id range values]
  (let [url (str "https://sheets.googleapis.com/v4/spreadsheets/" spreadsheet-id
                 "/values/" (java.net.URLEncoder/encode range "UTF-8")
                 ":append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS")
        body (json/generate-string {"values" [values]})
        client (-> (HttpClient/newBuilder)
                   (.version HttpClient$Version/HTTP_1_1)
                   (.connectTimeout (Duration/ofMillis 30000))
                   .build)
        builder (doto (HttpRequest/newBuilder (URI/create url))
                  (.timeout (Duration/ofMillis 30000))
                  (.header "Content-Type" "application/json")
                  (.header "Authorization" (str "Bearer " access-token)))
        _ (.POST builder (HttpRequest$BodyPublishers/ofString body))
        req (.build builder)
        resp (.send client req (HttpResponse$BodyHandlers/ofString))]
    (if (>= (.statusCode resp) 400)
      (throw (ex-info (str "Sheets API error " (.statusCode resp))
                      {:body (.body resp)}))
      (json/parse-string (.body resp) false))))

;; ── Google Sheets via Application Default Credentials ──

(defn- ensure-access-token
  "Get access token via gcloud CLI."
  []
  (try
    (let [res (shell/sh "gcloud" "auth" "application-default" "print-access-token")]
      (str/trim (:out res)))
    (catch Exception e
      (str "gcloud-error: " (.getMessage e)))))

;; ── Public API ──

(defn append-row!
  "Append a row to a Google Sheet.
   spreadsheet-id: from sheet URL (the long hash)
   sheet-name: tab name (default 'Sheet1')
   values: vector of values for the row
   
   Returns: updated range string"
  [spreadsheet-id sheet-name values]
  (let [token (ensure-access-token)
        range (str sheet-name "!A:Z")
        result (sheets-api-call token spreadsheet-id range values)]
    (get-in result ["updates" "updatedRange"])))

(defn read-last-row
  "Read the last row from a sheet."
  [spreadsheet-id sheet-name]
  (let [token (ensure-access-token)
        url (str "https://sheets.googleapis.com/v4/spreadsheets/" spreadsheet-id
                 "/values/" sheet-name "!A:Z")
        client (-> (HttpClient/newBuilder)
                   (.version HttpClient$Version/HTTP_1_1)
                   (.connectTimeout (Duration/ofMillis 15000))
                   .build)
        builder (doto (HttpRequest/newBuilder (URI/create url))
                  (.timeout (Duration/ofMillis 15000))
                  (.header "Authorization" (str "Bearer " token)))
        req (.build (.GET builder))
        resp (.send client req (HttpResponse$BodyHandlers/ofString))]
    (if (>= (.statusCode resp) 400)
      (throw (ex-info (str "Sheets read error " (.statusCode resp)) {:body (.body resp)}))
      (let [data (json/parse-string (.body resp) false)
            values (get data "values" [])]
        (last values)))))

;; ── Booking-specific helpers ──

(defn book-slot!
  "Book a time slot. Appends row: [date, time, client-name, phone, service, status, created-at]
   Returns success message."
  [spreadsheet-id sheet-name {:strs [date time name phone service]
                              :or {service "-"}}]
  (let [now (.format (LocalDateTime/now) (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm"))
        row [date time name phone service "новый" now]]
    (append-row! spreadsheet-id sheet-name row)
    (str "✅ Запись добавлена: " date " " time " — " name " (" phone ")\n"
         "Статус: новый | Лист: " sheet-name)))

;; ── Tool factory for agent harness ──

(defn booking-tool
  "Create a book_slot tool for use in agent.
   Config: {:spreadsheet-id \"...\" :sheet-name \"Записи\"}"
  [{:keys [spreadsheet-id sheet-name] :or {sheet-name "Записи"}}]
  {:name "book_slot"
   :schema {"type" "object"
            "properties" {"date" {"type" "string" "description" "Date in DD.MM.YYYY format"}
                          "time" {"type" "string" "description" "Time in HH:MM format"}
                          "name" {"type" "string" "description" "Client name"}
                          "phone" {"type" "string" "description" "Phone number"}
                          "service" {"type" "string" "description" "Service booked"}}
            "required" ["date" "time" "name" "phone"]}
   :execute (fn [args]
              (try
                (book-slot! spreadsheet-id sheet-name args)
                (catch Exception e
                  (str "❌ Ошибка записи: " (.getMessage e)))))})

(comment
  ;; Usage:
  (def tool (booking-tool {:spreadsheet-id "1BxiMVs0XRA5nFMjKvBdBZjgmUUqptlbs74OgvE2upms"
                           :sheet-name "Записи"}))

  ((:execute tool) {"date" "14.05.2026" "time" "15:00" "name" "Айбек" "phone" "+996700123456" "service" "Стрижка"})
  ;; → "✅ Запись добавлена: 14.05.2026 15:00 — Айбек (+996700123456)"
  )
