(ns clj-harness.telegram.format-test
  (:require [clj-harness.telegram.format :as fmt]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]))

(def sample-table
  "Before\n\n| Theme | Mood |\n| --- | --- |\n| Floral | Soft |\n| Dark | Glam |\n\nAfter")

(deftest rewrites-markdown-tables-to-bullets
  (testing "table data is preserved without raw Markdown table rows"
    (let [rewritten (fmt/rewrite-markdown-tables sample-table)]
      (is (str/includes? rewritten "Before"))
      (is (str/includes? rewritten "After"))
      (is (str/includes? rewritten "• Theme: Floral, Mood: Soft"))
      (is (str/includes? rewritten "• Theme: Dark, Mood: Glam"))
      (is (not (str/includes? rewritten "| --- | --- |"))))))

(deftest md->html-never-emits-markdown-table-blocks
  (let [html (fmt/md->html sample-table)]
    (is (str/includes? html "• Theme: Floral, Mood: Soft"))
    (is (not (str/includes? html "| Theme | Mood |")))
    (is (not (str/includes? html "| --- | --- |")))))

(deftest plain-pipe-text-is-not-a-table
  (let [text "Use A | B when comparing choices."]
    (is (= text (fmt/rewrite-markdown-tables text)))))

(deftest streaming-preview-also-removes-tables
  (let [preview (fmt/strip-md sample-table)]
    (is (str/includes? preview "• Theme: Floral, Mood: Soft"))
    (is (not (str/includes? preview "| --- | --- |")))))
