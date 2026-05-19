(ns clj-harness.skills
  "Skills system for clj-harness. Load design/style/prompt skills from EDN resources.
   Skill = {:id :airbnb :kind :design-system :prompt \"...\" :category \"...\" :description \"...\"}
   Skills are merged into agent system prompt at creation time."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; ── Skill Loading ──

(defn- read-edn
  "Read EDN forms from an io/resource URL. Returns vector of forms."
  [res]
  (try
    (with-open [r (-> res io/reader clojure.lang.LineNumberingPushbackReader.)]
      (loop [acc []]
        (let [v (try (edn/read {:eof ::eof} r) (catch Exception _ ::eof))]
          (if (= ::eof v)
            acc
            (recur (conj acc v))))))
    (catch Exception e
      (println (str "[skills] Error reading " res ": " (.getMessage e)))
      [])))

(def ^:private design-systems-file
  "skills/by-kind/design-system/all.edn")

(defn load-all-design-systems
  "Load all 99 design systems from bundled EDN."
  []
  (if-let [path (io/resource design-systems-file)]
    (read-edn path)
    (do (println "[skills] WARNING: design-systems.edn not found on classpath")
        [])))

(defn load-all-skills
  "Load all 100 skills from bundled EDN."
  []
  (if-let [path (io/resource "skills/by-kind/skill/all.edn")]
    (read-edn path)
    (do (println "[skills] WARNING: skill/all.edn not found on classpath")
        [])))

(defn load-by-ids
  "Load specific skills by :id keyword. Returns vector of skill maps."
  [ids]
  (let [all (into [] (concat (load-all-design-systems) (load-all-skills)))
        id-set (set ids)]
    (filterv #(contains? id-set (:id %)) all)))

(defn load-by-category
  "Load skills matching a category string (fuzzy, case-insensitive)."
  [category]
  (let [all (into [] (concat (load-all-design-systems) (load-all-skills)))
        cat-lower (str/lower-case category)]
    (filterv #(str/includes? (str/lower-case (or (:category %) "")) cat-lower) all)))

(defn load-by-kind
  "Load all skills of a given :kind."
  [kind]
  (let [all (into [] (concat (load-all-design-systems) (load-all-skills)))]
    (filterv #(= kind (:kind %)) all)))

;; ── Prompt Merging ──

(defn design-system-prompt
  "Convert a design system skill map into a prompt fragment for the agent."
  [{:keys [id description category prompt]}]
  (str "## Design System: " (name id) "\n"
       "> " (or description "") "\n"
       "> Category: " (or category "") "\n\n"
       prompt))

(defn skill-prompt
  "Convert a skill map into a prompt fragment."
  [{:keys [id description prompt]}]
  (str "## Skill: " (name id) "\n"
       (when description (str "> " description "\n"))
       "\n" prompt))

(defn merge-into-system-prompt
  "Merge loaded skills into the agent's system prompt.
   skills: vector of skill maps.
   base-prompt: the agent's base system prompt string.
   max-tokens: roughly limit total prompt size (default 60K chars)."
  [skills base-prompt & {:keys [max-chars] :or {max-chars 60000}}]
  (let [fragments (mapv (fn [s]
                          (if (= :design-system (:kind s))
                            (design-system-prompt s)
                            (skill-prompt s)))
                        skills)
        header ";; ── LOADED SKILLS ──\n\n"
        body (str/join "\n\n---\n\n" fragments)
        ;; Truncate if too long — keep most recent/last skills
        total (+ (count header) (count body) (count base-prompt) 50)]
    (if (< total max-chars)
      (str header body "\n\n;; ── BASE PROMPT ──\n\n" base-prompt)
      ;; Truncate: keep base prompt intact, trim skill fragments
      (let [budget (- max-chars (count base-prompt) 100)
            truncated (if (< budget (count header))
                        ""
                        (let [avail (- budget (count header))]
                          (if (< avail (count body))
                            (str header (subs body 0 avail) "\n\n[... truncated ...]")
                            (str header body))))]
        (str truncated "\n\n;; ── BASE PROMPT ──\n\n" base-prompt)))))

;; ── Convenience ──

(defn list-available
  "List all available skill IDs, optionally filtered by kind."
  ([] (list-available nil))
  ([kind]
   (let [all (if kind
               (load-by-kind kind)
               (into [] (concat (load-all-design-systems) (load-all-skills))))]
     (mapv (fn [s] {:id (:id s) :kind (:kind s) :category (:category s) :desc (:description s)}) all))))

;; ── Cache (for performance) ──

(defonce ^:private skill-cache (atom nil))

(defn- all-skills-cached []
  (or @skill-cache
      (let [loaded (into [] (concat (load-all-design-systems) (load-all-skills)))]
        (reset! skill-cache loaded)
        loaded)))

(defn warm-cache! []
  (reset! skill-cache nil)
  (let [n (count (all-skills-cached))]
    (println (str "[skills] Cached " n " skills"))
    n))
