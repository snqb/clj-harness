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
     ;; Legacy: message-array persistence (still works, wraps in state map)
     :load (fn [bot-id user-id]
             (try
               (let [row (jdbc/execute-one! ds
                                            ["SELECT messages_json FROM sessions WHERE bot_id = ? AND user_id = ?"
                                             bot-id user-id]
                                            {:builder-fn as-unqualified-lower-maps})]
                 (if row
                   (let [parsed (json/parse-string (:messages_json row) false)]
                     ;; Handle both old format (array) and new format (map)
                     (if (sequential? parsed)
                       {"messages" (vec (take-last 20 parsed)) "data" {}}
                       parsed))
                   []))
               (catch Exception e
                 (log/warn e :session-load-error :bot-id bot-id :user-id user-id)
                 [])))
     :save (fn [bot-id user-id messages]
             (try
               ;; Store as new state-map format: {:messages [...] :data {}}
               ;; Existing data map is preserved on load since the load now
               ;; returns the full map when in new format
               (let [json-str (json/generate-string {"messages" messages "data" {}})]
                 (jdbc/execute! ds
                                ["INSERT INTO sessions (bot_id, user_id, messages_json, updated_at)
                     VALUES (?, ?, ?, unixepoch())
                     ON CONFLICT(bot_id, user_id)
                     DO UPDATE SET messages_json = ?, updated_at = unixepoch()"
                                 bot-id user-id json-str json-str]))
               (catch Exception e
                 (log/error e :session-save-error :bot-id bot-id :user-id user-id))))
     ;; Snapshot: full-state persistence (messages + data map)
     :snapshot-save (fn [bot-id user-id state]
                      (try
                        (let [json-str (json/generate-string state)]
                          (jdbc/execute! ds
                                         ["INSERT INTO sessions (bot_id, user_id, messages_json, updated_at)
                     VALUES (?, ?, ?, unixepoch())
                     ON CONFLICT(bot_id, user_id)
                     DO UPDATE SET messages_json = ?, updated_at = unixepoch()"
                                          bot-id user-id json-str json-str]))
                        (catch Exception e
                          (log/error e :session-snapshot-save-error :bot-id bot-id :user-id user-id))))
     :snapshot-load (fn [bot-id user-id]
                      (try
                        (let [row (jdbc/execute-one! ds
                                                     ["SELECT messages_json FROM sessions WHERE bot_id = ? AND user_id = ?"
                                                      bot-id user-id]
                                                     {:builder-fn as-unqualified-lower-maps})]
                          (when row
                            (let [parsed (json/parse-string (:messages_json row) false)]
                              (if (sequential? parsed)
                                ;; Old format: just messages
                                {"messages" (vec (take-last 20 parsed)) "data" {}}
                                ;; New format: full state map
                                parsed))))
                        (catch Exception e
                          (log/warn e :session-snapshot-load-error :bot-id bot-id :user-id user-id)
                          nil)))}))
