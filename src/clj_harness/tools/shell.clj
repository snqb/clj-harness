(ns clj-harness.tools.shell
  "Shell tool — create LLM-callable tools from shell commands.

   Uses {{key}} template substitution to inject LLM-provided arguments
   into shell commands. Supports string and integer argument types.

   (shell-tool \"lalafo_search\" \"Search Lalafo\"
     \"python3 client.py --q='{{query}}' --max={{max}}\"
     {:query :string :max :number})"
  (:require
   [clojure.string :as str]
   [clojure.java.shell :as shell]))

(defn shell-tool
  "Create a tool definition that executes a shell command.

   Args:
     name        — tool name (keyword or string)
     description — tool description for LLM context
     command     — shell command with {{key}} placeholders
     arg-spec    — {:key1 :string, :key2 :number} — argument types

   Returns a tool map with :name, :schema, :execute.
   Ready to pass to create-bot's :tools vector."
  [tool-name description command arg-spec]
  {:name tool-name
   :description description
   :schema {"type" "object"
            "properties" (into {}
                               (map (fn [[k v]]
                                      [(name k) {"type" (if (= v :number) "integer" "string")
                                                 "description" (name k)}])
                                    arg-spec))
            "required" (vec (map name (keys arg-spec)))}
   :execute (fn [args]
              (let [cmd (reduce-kv (fn [s k v]
                                     (str/replace s (str "{{" (name k) "}}") (str v)))
                                   command args)]
                (try (let [r (shell/sh "bash" "-c" cmd :out :string :err :string)]
                       (if (= 0 (:exit r))
                         (str/trim (:out r))
                         (str "Error: " (str/trim (:err r)))))
                     (catch Exception e (str "Shell error: " (.getMessage e))))))})
