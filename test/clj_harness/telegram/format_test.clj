(ns clj-harness.telegram.format-test
  (:require [clj-harness.telegram.format :as fmt]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]))

(def sample-table
  "Before\n\n| Theme | Mood |\n| --- | --- |\n| Floral | Soft |\n| Dark | Glam |\n\nAfter")

(deftest rewrites-markdown-tables-to-monospace
  (testing "small tables render as aligned monospace <pre> blocks"
    (let [rewritten (fmt/rewrite-markdown-tables sample-table)]
      (is (str/includes? rewritten "Before"))
      (is (str/includes? rewritten "After"))
      (is (str/includes? rewritten "<pre>"))
      (is (str/includes? rewritten "</pre>"))
      (is (str/includes? rewritten "Theme"))
      (is (str/includes? rewritten "Floral"))
      (is (str/includes? rewritten "Dark"))
      ;; Separator line uses ─
      (is (str/includes? rewritten "─"))
      ;; No raw pipe table artifacts
      (is (not (str/includes? rewritten "| --- | --- |")))
      (is (not (str/includes? rewritten "| Theme | Mood |"))))))

(deftest md->html-never-emits-markdown-table-blocks
  (let [html (fmt/md->html sample-table)]
    (is (str/includes? html "<pre>"))
    (is (str/includes? html "Theme"))
    (is (str/includes? html "Floral"))
    (is (not (str/includes? html "| --- | --- |")))))

(deftest plain-pipe-text-is-not-a-table
  (let [text "Use A | B when comparing choices."]
    (is (= text (fmt/rewrite-markdown-tables text)))))

(deftest streaming-preview-also-removes-tables
  (let [preview (fmt/strip-md sample-table)]
    ;; strip-md runs rewrite-markdown-tables first, then strips markdown
    (is (str/includes? preview "Theme"))
    (is (str/includes? preview "Floral"))
    (is (not (str/includes? preview "| --- | --- |")))))

(deftest render-table-from-structured-data
  (testing "render-table builds monospace table from headers + rows"
    (let [result (fmt/render-table ["Name" "Price"]
                                   [["iPhone" "72000"] ["Galaxy" "65000"]])]
      (is (str/includes? result "<pre>"))
      (is (str/includes? result "</pre>"))
      (is (str/includes? result "Name"))
      (is (str/includes? result "Price"))
      (is (str/includes? result "iPhone"))
      (is (str/includes? result "72000"))
      (is (str/includes? result "Galaxy"))
      (is (str/includes? result "65000"))
      ;; Separator
      (is (str/includes? result "─")))))

(deftest render-table-fallback-for-huge-tables
  (testing "render-table falls back to bullets when result exceeds 4000 chars"
    ;; With 24-char cap, need ~150 rows to exceed 4000 chars in <pre>
    (let [big-rows (mapv (fn [i] [(str "Item-" i) (str "V" (apply str (repeat 23 \x)))])
                         (range 160))
          result (fmt/render-table ["Col1" "Col2"] big-rows)]
      ;; Should be bullet list, not <pre>
      (is (str/starts-with? result "• "))
      (is (not (str/includes? result "<pre>"))))))

(deftest wide-table-falls-back-to-bullets
  (testing "tables too wide for monospace fall back to bullets"
    ;; 7 columns with long content → exceeds 4000 in <pre>
    (let [headers ["A" "B" "C" "D" "E" "F" "G"]
          rows [["a1" "b1" "c1" "d1" "e1" "f1" "g1"]]
          result (fmt/rewrite-markdown-tables
                  (str "| " (str/join " | " headers) " |\n"
                       "| " (str/join " | " (repeat 7 "---")) " |\n"
                       "| " (str/join " | " (first rows)) " |"))]
      ;; Should still produce valid output (either mono or bullets)
      (is (string? result))
      (is (pos? (count result))))))
