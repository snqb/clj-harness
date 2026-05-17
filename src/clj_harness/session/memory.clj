(ns clj-harness.session.memory
  "In-memory session management — atom-based per-user state.

   Each session is an atom wrapping a map: {\"messages\" [] \"summary\" nil \"data\" {}}.
   Messages use string keys (\"role\" \"content\") to match LLM JSON conventions.

   Complements clj-harness.session.sqlite — this is the runtime state,
   SQLite is the durable store. A session can be loaded from SQLite and
   the atom becomes the live working copy.")

;; ══════════════════════ SESSION ATOM ══════════════════════

(defn make-session
  "Create a new session atom with empty message history.
   Returns an atom — thread-safe, swap!-able, deref-able."
  []
  (atom {"messages" [] "summary" nil "data" {}}))

;; ══════════════════════ MESSAGE OPERATIONS ══════════════════════

(defn session-add!
  "Add a message to session history. Mutates the atom via swap!.
   Thread-safe — multiple concurrent additions commute correctly.

   (session-add! session \"user\" \"Hello\")
   (session-add! session \"assistant\" \"Hi there!\")"
  [session role content]
  (swap! session update "messages" conj {"role" role "content" content}))

(defn session-messages
  "Get messages from session, prepending summary if present.
   Returns a sequence suitable for passing to the LLM.

   (count (session-messages session))"
  [session]
  (let [{:strs [messages summary]} @session]
    (if summary
      (cons {"role" "system" "content" (str "[Earlier]\n" summary)} messages)
      messages)))

;; ══════════════════════ ARBITRARY DATA ══════════════════════

(defn session-data
  "Get arbitrary data stored in session. Use for per-user state that isn't messages.

   (session-data session)  ;; => {:theme \"dark\" :last-search \"Paris\"}"
  [session]
  (get @session "data" {}))

(defn session-update-data!
  "Update session data via function. Merges the result of (f current-data args...).

   (session-update-data! session assoc \"theme\" \"dark\")
   (session-update-data! session merge {:city \"Paris\"})"
  [session f & args]
  (apply swap! session update "data" f args))
