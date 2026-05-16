(ns clj-harness.tools.business-schema
  "Opt-in tool: injects domain-specific required fields into agent context.
  
  Usage:
    (require '[clj-harness.tools.business-schema :as bs])
    (def bot (create-bot {:tools (into my-tools [(bs/tool :pet-hotel :coffee-shop)]) ...}))
  
  Agent calls business_schema(\"pet hotel\") → gets required fields checklist.
  The schema tells the agent what it MUST include for that business type."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def ^:private schemas
  (delay (edn/read-string (slurp (io/resource "business-schemas.edn")))))

(def ^:private all-types
  (delay (->> @schemas keys (sort) (mapv name))))

(defn tool
  "Create a business-schema tool. Optionally restrict to specific business types.
   (tool)           ;; all 30+ types available
   (tool :pet-hotel :coffee-shop :yoga-studio)  ;; only these types"
  [& restrict-to]
  {:name "business_schema"
   :description (str "Get required fields, recommendations, and anti-patterns for a business type. "
                     "Call this BEFORE creating a site to know what content is mandatory. "
                     "Available types: pet-hotel, hotel, restaurant, coffee-shop, wedding-venue, "
                     "beauty-salon, barbershop, yoga-studio, fitness-club, car-wash, car-service, "
                     "photo-studio, law-firm, medical-clinic, kindergarten, clothing-store, "
                     "flower-shop, pharmacy, bookstore, saas-product, mobile-app, fintech, "
                     "cryptocurrency-exchange, photographer-portfolio, design-agency, music-studio, "
                     "real-estate-agency, coworking-space, travel-agency, landing-page, "
                     "personal-website, event-page, local-business")
   :schema {"type" "object"
            "properties" {"type" {"type" "string"
                                  "description" "Business type: 'pet hotel', 'coffee shop', 'yoga studio', etc."}}
            "required" ["type"]}
   :execute (fn [args]
              (let [type-str (get args "type")
                    q (-> type-str str/trim str/lower-case
                         (str/replace #"[^a-z0-9-]" "-")
                         (str/replace #"-+" "-")
                         keyword)
                    sm @schemas
                    schema (or (get sm q)
                              (some (fn [[tk v]] (when (str/includes? (name tk) (name q)) v)) sm)
                              (get sm :local-business))
                    type-key (or (some (fn [[k v]] (when (identical? v schema) k)) sm) :local-business)]
                (if schema
                  (let [required (get schema :required [])
                        recommended (get schema :recommended [])
                        anti (get schema :anti_patterns [])
                        example (get schema :example "")]
                    (str "Business type: " (name type-key) "\n"
                         "MUST include (required):\n"
                         (str/join "\n" (map #(str "  • " (str/capitalize (name %))) required)) "\n"
                         "\nShould include (recommended):\n"
                         (str/join "\n" (map #(str "  • " (str/capitalize (name %))) recommended)) "\n"
                         "\nAvoid (anti-patterns):\n"
                         (str/join "\n" (map #(str "  ✗ " %) anti)) "\n"
                         (when (not (str/blank? example))
                           (str "\nExample format:\n  " example "\n"))
                         "\nMake sure ALL required fields are present."))
                  (str "Unknown business type: " type-str ". Try: " (str/join ", " (take 15 @all-types)) "..."))))})
