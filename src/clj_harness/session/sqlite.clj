(ns clj-harness.session.sqlite
  "SQLite-backed session persistence for clj-harness bots.

  Sessions are stored as JSON blobs keyed by (bot-id, user-id).
  Auto-loads on create-bot, auto-saves via on-save hook.

  Usage:
    (require '[clj-harness.core :as h]
             '[clj-harness.session.sqlite :as sess])

    (def bot (h/create-bot
               {:name \"mybot\"
                :prompt \"...\"
                :persistence (sess/create \"/tmp/mybot.db\")}))

    ;; Sessions survive restarts."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :refer [as-unqualified-lower-maps]]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]))

;; ══════════════════════ SCHEMA ══════════════════════

(def ^:private schema-ddl
  "CREATE TABLE IF NOT EXISTS sessions (
     bot_id TEXT NOT NULL,
     user_id TEXT NOT NULL,
     messages_json TEXT NOT NULL DEFAULT '[]',
     updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
     PRIMARY KEY (bot_id, user_id)
   )")

(defn- ensure-schema! [ds]
  (jdbc/execute! ds [schema-ddl]))

;; ══════════════════════ PUBLIC ══════════════════════

(defn create
  "Create a persistence config for clj-harness.
   db-path: path to SQLite file (created if missing).
   Returns a map suitable for :persistence option in create-bot.

   The returned map implements:
     :load   — (fn [bot-id user-id]) → messages vector
     :save   — (fn [bot-id user-id messages])"
  [db-path]
  (let [ds (jdbc/get-datasource {:dbtype "sqlite" :dbname db-path})]
    (ensure-schema! ds)
    (log/info :session-db-ready :path db-path)
    {:type :sqlite
     :ds   ds
     :load (fn [bot-id user-id]
             (try
               (let [row (jdbc/execute-one! ds
                                            ["SELECT messages_json FROM sessions WHERE bot_id = ? AND user_id = ?"
                                             bot-id user-id]
                                            {:builder-fn as-unqualified-lower-maps})]
                 (if row
                   (json/parse-string (:messages_json row) false)
                   []))
               (catch Exception e
                 (log/warn e :session-load-error :bot-id bot-id :user-id user-id)
                 [])))
     :save (fn [bot-id user-id messages]
             (try
               (let [json-str (json/generate-string messages)]
                 (jdbc/execute! ds
                                ["INSERT INTO sessions (bot_id, user_id, messages_json, updated_at)
                     VALUES (?, ?, ?, unixepoch())
                     ON CONFLICT(bot_id, user_id)
                     DO UPDATE SET messages_json = ?, updated_at = unixepoch()"
                                 bot-id user-id json-str json-str]))
               (catch Exception e
                 (log/error e :session-save-error :bot-id bot-id :user-id user-id))))}))
