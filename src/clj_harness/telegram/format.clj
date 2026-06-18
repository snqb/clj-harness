(ns clj-harness.telegram.format
  "Telegram message formatting utilities.

  Primary path: LLM markdown → sendRichMessage(markdown=...) — Telegram natively
  renders tables, headings, lists, code blocks, footnotes, math, etc.

  Legacy path: LLM markdown → md->html → sendMessage(parse_mode=HTML)
  Used as fallback when Rich Messages API unavailable.

  Streaming: strip-md + streaming-preview for plain-text draft previews.
  completed-markdown-prefix buffers unfinished markdown during streaming."
  (:require [clojure.string :as str]))

;; ══════════════════════ HTML ESCAPE ══════════════════════

(defn escape-html
  "Escape HTML special chars. Safe to call on already-escaped text."
  [text]
  (-> text
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")))

;; ══════════════════════ MARKDOWN TABLES → LISTS ══════════════════════

(defn- pipe-row?
  "True when a line looks like a Markdown table row."
  [line]
  (boolean (re-matches #"\s*\|.*\|\s*" (or line ""))))

(defn- table-cells
  "Parse a Markdown table row into trimmed cells."
  [line]
  (-> line
      str/trim
      (str/replace #"^\|" "")
      (str/replace #"\|$" "")
      (str/split #"\|")
      (->> (map str/trim))))

(defn- separator-cell? [cell]
  (boolean (re-matches #":?-{3,}:?" (str/trim cell))))

(defn- separator-row?
  "True for Markdown table separator rows like | --- | :---: |."
  [line]
  (let [cells (table-cells line)]
    (and (>= (count cells) 2)
         (every? separator-cell? cells))))

(defn- markdown-table-block?
  "True when a contiguous pipe-row block is a real Markdown table.
   Requires either a separator row OR 3+ consecutive pipe rows."
  [lines]
  (and (>= (count lines) 2)
       (or (some separator-row? lines)
           (>= (count lines) 3))))

(defn- truncate-cell [s max-w]
  (if (> (count s) max-w)
    (subs s 0 (dec max-w))
    s))

(defn- pad-right [s w]
  (let [len (count s)]
    (if (>= len w)
      s
      (str s (apply str (repeat (- w len) \space))))))

(defn- table-block->monospace
  "Render a Markdown table as a Telegram <pre> monospace block.
   Columns are aligned with spaces. Max column width is capped at 24.
   Returns nil if the result would exceed 4000 chars (leave room for <pre> tags).
   If no separator row, first row is treated as header."
  [lines]
  (let [rows (mapv (comp vec table-cells) lines)
        sep-idx (first (keep-indexed (fn [idx line]
                                       (when (separator-row? line) idx))
                                     lines))
        headers (cond
                  ;; Has separator: row before it is header
                  (pos? (or sep-idx 0)) (get rows (dec sep-idx))
                  ;; No separator, 3+ rows: first row is header
                  (>= (count rows) 3) (first rows)
                  ;; 2 rows, no separator: first row is header
                  :else (first rows))
        data-rows (cond
                    ;; Has separator: rows after it
                    sep-idx (subvec rows (inc sep-idx))
                    ;; No separator: all rows except first
                    :else (subvec rows 1))
        all-rows (if (seq data-rows) data-rows rows)
        col-count (apply max (count headers) (map count all-rows))
        ;; Calculate column widths (cap at 24)
        col-widths
        (vec (for [c (range col-count)]
               (let [header-w (count (str/trim (or (nth headers c "") "")))
                     max-data (apply max 0 (map #(count (str/trim (or (nth % c "") "")))
                                                all-rows))]
                 (min 32 (max 2 max-data header-w)))))
        ;; Build a line from cells with padding
        render-row (fn [cells]
                     (str/trimr
                      (str/join "  "
                                (map (fn [c w]
                                       (pad-right (truncate-cell (str/trim (or c "")) w) w))
                                     (concat cells (repeat ""))
                                     col-widths))))
        header-line (when (seq headers) (render-row headers))
        separator (when (seq headers)
                    (str/join "  " (map #(apply str (repeat % \─)) col-widths)))
        data-lines (mapv render-row all-rows)
        body (str/join "\n" (cond-> []
                              header-line (conj header-line)
                              separator (conj separator)
                              :else (into data-lines)))
        result (str "<pre>" body "</pre>")]
    (when (<= (count result) 4000)
      result)))

(defn- row->bullet [headers row]
  (let [pairs (->> (map vector headers row)
                   (keep (fn [[header value]]
                           (let [header (str/trim (or header ""))
                                 value (str/trim (or value ""))]
                             (when-not (str/blank? value)
                               (if (str/blank? header)
                                 value
                                 (str header ": " value)))))))]
    (str "• " (str/join ", " pairs))))

(defn- table-block->bullets
  "Convert one Markdown table block into Telegram-friendly bullets."
  [lines]
  (let [rows (mapv (comp vec table-cells) lines)
        sep-idx (first (keep-indexed (fn [idx line]
                                       (when (separator-row? line) idx))
                                     lines))
        headers (if (pos? (or sep-idx 0))
                  (get rows (dec sep-idx))
                  [])
        data-rows (->> rows
                       (keep-indexed (fn [idx row]
                                       (when (and (not= idx sep-idx)
                                                  (or (nil? sep-idx) (> idx sep-idx)))
                                         row))))
        bullet-rows (if (seq data-rows)
                      data-rows
                      (remove #(every? str/blank? %) rows))]
    (str/join "\n" (map #(row->bullet headers %) bullet-rows))))

(defn- table-block->rendered
  "Convert one Markdown table block: try monospace <pre> first, fall back to bullets."
  [lines]
  (or (table-block->monospace lines)
      (table-block->bullets lines)))

(defn rewrite-markdown-tables
  "Rewrite Markdown tables into Telegram-friendly format.

  Small tables (≤5 cols, fits in 4096 chars) render as aligned monospace
  <pre> blocks. Wide or oversized tables fall back to bullet/key-value lists
  since Telegram clients don't render raw Markdown tables.
  Non-table pipe text is left untouched."
  [text]
  (letfn [(flush-block [out block]
            (cond
              (empty? block) out
              (markdown-table-block? block) (conj out (table-block->rendered block))
              :else (into out block)))]
    (let [lines (str/split-lines (or text ""))
          trailing-newline? (str/ends-with? (or text "") "\n")
          rewritten (loop [remaining lines
                           out []
                           block []]
                      (if-let [line (first remaining)]
                        (if (pipe-row? line)
                          (recur (rest remaining) out (conj block line))
                          (recur (rest remaining)
                                 (conj (flush-block out block) line)
                                 []))
                        (flush-block out block)))]
      (cond-> (str/join "\n" rewritten)
        trailing-newline? (str "\n")))))

(defn tables->bullets
  "Rewrite Markdown tables into plain-text bullet lists (no HTML).
   Used by strip-md for plain-text previews where <pre> blocks aren't
   appropriate. Non-table pipe text is left untouched."
  [text]
  (letfn [(flush-block [out block]
            (cond
              (empty? block) out
              (markdown-table-block? block) (conj out (table-block->bullets block))
              :else (into out block)))]
    (let [lines (str/split-lines (or text ""))
          trailing-newline? (str/ends-with? (or text "") "\n")
          rewritten (loop [remaining lines
                           out []
                           block []]
                      (if-let [line (first remaining)]
                        (if (pipe-row? line)
                          (recur (rest remaining) out (conj block line))
                          (recur (rest remaining)
                                 (conj (flush-block out block) line)
                                 []))
                        (flush-block out block)))]
      (cond-> (str/join "\n" rewritten)
        trailing-newline? (str "\n")))))

;; ══════════════════════ MARKDOWN → TELEGRAM HTML ══════════════════════

(def ^:private inline-code-placeholder-prefix "◊ICODE◊")
(def ^:private pre-block-placeholder-prefix "◊PRE◊")

(defn save-matches
  "Save regex matches as placeholders. Returns [text-with-placeholders saved-items]."
  [text pattern prefix]
  (let [items (atom [])]
    [(str/replace text pattern
                  (fn [match-group]
                    (let [idx (count @items)]
                      (swap! items conj (peek match-group))
                      (str prefix idx "◊"))))
     @items]))

(defn restore-matches
  "Restore saved items from placeholders, applying tag-fn to each."
  [text prefix items tag-fn]
  (reduce-kv (fn [t i item]
               (str/replace t (str prefix i "◊") (tag-fn item)))
             text (vec items)))

;; ── Main converter ──

(defn md->html
  "Convert LLM markdown to Telegram-compatible HTML.

  Also rewrites any markdown tables into bullet/key-value lists since Telegram doesn't render tables.

  Key insight: headers and inline code need HTML tags, so they must be
  saved before escaping, then restored after. Everything else (**bold**,
  [links], etc.) uses plain characters and can be converted post-escape.

  Pipeline:
    1. Save inline code `...` with placeholders
    2. Save headers # Title with placeholders
    3. Blockquotes > text → plain
    4. Escape HTML (< > &)
    5. Restore headers as <b>Title</b>
    6. **bold** → <b>
    7. __underline__ → <u>
    8. *italic* → <i>
    9. ~~strikethrough~~ → <s>
    10. [text](url) → <a href='url'>text</a>
    11. - bullet → • bullet
    12. Normalize numbered lists
    13. Restore inline code with <code>
    14. Horizontal rules --- → —"
  [^String text]
  (if (str/blank? text)
    text
    (let [;; Step 0: Rewrite markdown tables (monospace <pre> or bullets)
          clean (rewrite-markdown-tables text)
          ;; Step 0.5: Save <pre>...</pre> blocks before HTML escaping
          [t0 pre-blocks] (save-matches clean #"(?s)(<pre>.*?</pre>)"
                                         pre-block-placeholder-prefix)
          ;; Step 1: Save inline code before escaping
          [t1 inline-codes] (save-matches t0 #"`([^`]+)`"
                                          inline-code-placeholder-prefix)
          ;; Step 2: Save headers before escaping
          [t2 headers] (save-matches t1 #"(?m)^#{1,6}\s+(.+)$"
                                     "◊H◊")
          ;; Step 3: Blockquotes → plain
          t3 (str/replace t2 #"(?m)^>\s*(.*)$" "$1")
          ;; Step 4: Escape HTML (safe since headers are saved)
          t4 (escape-html t3)
          ;; Step 5: Restore headers as <b>
          t5 (restore-matches t4 "◊H◊" headers
                              (fn [h] (str "<b>" h "</b>")))
          ;; Step 6: **bold**
          t6 (str/replace t5 #"\*\*(.+?)\*\*" "<b>$1</b>")
          ;; Step 7: __underline__
          t7 (str/replace t6 #"__(.+?)__" "<u>$1</u>")
          ;; Step 8: *italic* (word-boundary aware single asterisk)
          t8 (str/replace t7 #"(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])" "<i>$1</i>")
          ;; Step 9: ~~strikethrough~~
          t9 (str/replace t8 #"~~(.+?)~~" "<s>$1</s>")
          ;; Step 10: [text](url) → <a>
          t10 (str/replace t9 #"\[([^\]]+)\]\(([^)]+)\)"
                           "<a href='$2'>$1</a>")
          ;; Step 11: Bullets
          t11 (str/replace t10 #"(?m)^[-*]\s+" "• ")
          ;; Step 12: Numbered lists (normalize whitespace after dot)
          t12 (str/replace t11 #"(?m)^(\d+)\.\s+" "$1. ")
          ;; Step 13: Restore inline code
          t13 (restore-matches t12 inline-code-placeholder-prefix inline-codes
                               (fn [code] (str "<code>" (escape-html code) "</code>")))
          ;; Step 14: Horizontal rules
          t14 (str/replace t13 #"(?m)^---+$" "<code>———</code>")
          ;; Step 15: Restore <pre> monospace table blocks
          t15 (restore-matches t14 pre-block-placeholder-prefix pre-blocks
                               (fn [block] block))]
      t15)))

;; ══════════════════════ STRIP MARKDOWN ══════════════════════

(def ^:private md-strip-patterns
  "Ordered [pattern replacement] pairs for stripping markdown."
  [[#"```[\w]*\n?([\s\S]*?)```" "$1"]           ;; code blocks
   [#"`([^`]+)`" "$1"]                              ;; inline code
   [#"(?m)^#{1,6}\s+(.+)$" "$1"]                    ;; headers
   [#"(?m)^>\s*(.*)$" "$1"]                          ;; blockquotes
   [#"\*\*(.+?)\*\*" "$1"]                          ;; bold
   [#"__(.+?)__" "$1"]                                ;; underline
   [#"(?<![a-zA-Z0-9])_([^_]+)_(?![a-zA-Z0-9])" "$1"]  ;; italic
   [#"~~(.+?)~~" "$1"]                                ;; strikethrough
   [#"\[([^\]]+)\]\([^)]+\)" "$1"]                  ;; links
   [#"(?m)^[-*]\s+" "• "]                             ;; bullets
   [#"(?m)^(\d+)\.\s+" "$1. "]])                    ;; numbered lists

(defn strip-md
  "Strip markdown syntax for readable plain-text preview.
   Used during streaming mid-edits as a safety fallback.
   With Rich Message Drafts, most markdown renders natively,
   so this is only needed for legacy editMessageText paths.
   Markdown tables are first rewritten as bullet lists so the separator
   row (| --- |) doesn't leak into the plain-text preview."
  [text]
  (reduce (fn [t [pat repl]] (str/replace t pat repl))
          (tables->bullets (or text ""))
          md-strip-patterns))

;; ══════════════════════ STREAMING PREVIEW ══════════════════════

(defn- last-match-end
  "Return the end index of the last regex match in text."
  [pattern ^String text]
  (let [matcher (re-matcher pattern text)]
    (loop [idx nil]
      (if (.find matcher)
        (recur (.end matcher))
        idx))))

(defn- open-code-fence-start
  "Return start index of an unmatched ``` fence, if text is inside one."
  [^String text]
  (let [matcher (re-matcher #"(?m)^```" text)]
    (loop [n 0
           start nil]
      (if (.find matcher)
        (recur (inc n) (.start matcher))
        (when (odd? n) start)))))

(defn completed-markdown-prefix
  "Return the largest prefix that is safe to show as a streaming preview.

   Keeps trailing partial markdown hidden until it reaches a natural boundary:
   paragraph, completed line/list item, or sentence. If a fenced code block is
   open, only returns content before that block so users never see half a fence."
  [text]
  (let [text (or text "")]
    (if (str/blank? text)
      ""
      (let [candidate (if-let [fence-start (open-code-fence-start text)]
                        (subs text 0 fence-start)
                        text)
            end-idx (or (last-match-end #"\n\s*\n+" candidate)
                        (last-match-end #"(?m)^.+\n" candidate)
                        (last-match-end #"[.!?…][\"')\]]?\s+" candidate)
                        (when (re-find #"[.!?…][\"')\]]?$" candidate)
                          (count candidate)))]
        (if end-idx
          (let [prefix (subs candidate 0 end-idx)]
            (if-let [fence-start (open-code-fence-start prefix)]
              (subs prefix 0 fence-start)
              prefix))
          "")))))

(defn streaming-preview
  "Markdown preview for in-progress streaming via Rich Message Drafts.

   Returns the largest safe prefix of accumulated markdown that won't
   show half-formed tables or code blocks. Telegram natively renders
   the markdown in the draft preview.

   For legacy editMessageText paths, wrap with strip-md."
  [text]
  (str/trimr (completed-markdown-prefix text)))

;; ══════════════════════ MONOSPACE TABLE BUILDER ══════════════════════

(defn render-table
  "Build a Telegram monospace table from headers + rows.
   Returns a <pre> HTML string with aligned columns.
   Falls back to bullet list if result exceeds 4000 chars.

   Usage:
     (render-table [\"Name\" \"Price\" \"Link\"]
                   [[\"iPhone 15\" \"72000\" \"lalafo.kg/123\"]
                    [\"Galaxy S24\" \"65000\" \"lalafo.kg/456\"]])
   → \"<pre>Name       Price  Link\n────────  ─────  ──────────────\niPhone 15  72000  lalafo.kg/123\nGalaxy S24 65000  lalafo.kg/46</pre>\""
  [headers rows]
  (let [col-count (count headers)
        col-widths
        (vec (for [c (range col-count)]
               (let [header-w (count (nth headers c))
                     max-data (apply max 0 (map #(count (str (nth % c ""))) rows))]
                 (min 32 (max 2 max-data header-w)))))
        render-row (fn [cells]
                     (str/trimr
                      (str/join "  "
                                (map (fn [c w]
                                       (pad-right (truncate-cell (str (or c "")) w) w))
                                     (concat cells (repeat ""))
                                     col-widths))))
        header-line (render-row headers)
        separator (str/join "  " (map #(apply str (repeat % \─)) col-widths))
        data-lines (map render-row rows)
        body (str/join "\n" (cons header-line (cons separator data-lines)))
        result (str "<pre>" body "</pre>")]
    (if (<= (count result) 4000)
      result
      ;; Fallback: bullet list
      (str/join "\n" (map (fn [row]
                            (str "• " (str/join ", "
                                                 (keep (fn [[h v]]
                                                         (when-not (str/blank? (str v))
                                                           (str h ": " v)))
                                                       (map vector headers row)))))
                          rows)))))

;; ══════════════════════ SPLIT ══════════════════════

(def telegram-max-length 4096)

(defn- safe-split-idx
  "Find best split index in section, preferring natural boundaries."
  [^String section]
  (or (str/last-index-of section "\n\n")
      (str/last-index-of section "\n")
      (str/last-index-of section ". ")
      (str/last-index-of section "! ")
      (str/last-index-of section "? ")
      (str/last-index-of section " ")
      (count section)))

(defn- html-safe-split
  "Split text for HTML content: close tags before split, reopen after.
   Returns [head tail]."
  [text max-length]
  (let [section (subs text 0 max-length)
        split-pt (safe-split-idx section)
        head (subs text 0 split-pt)
        tail (subs text split-pt)
        ;; Find open HTML tags in head that need closing
        open-tags (re-seq #"<(b|i|u|s|code|pre|a)[^>]*>" head)
        close-tags (re-seq #"</(b|i|u|s|code|pre|a)>" head)
        ;; Simple stack: unmatched opens
        unmatched (loop [tags (map second open-tags)
                         closes (map second close-tags)
                         stack '()]
                    (if (or (empty? tags) (empty? closes))
                      (reverse (concat tags stack))
                      (if (= (first tags) (first closes))
                        (recur (rest tags) (rest closes) stack)
                        (recur (rest tags) closes (cons (first tags) stack)))))]
    [(str (str/trimr head)
          (apply str (map #(str "</" % ">") unmatched)))
     (str (apply str (map #(str "<" % ">") unmatched))
          (str/triml tail))]))

(defn split-message
  "Split text into Telegram-safe chunks (≤ 4096 chars).
   Splits at paragraph/sentence/word boundaries.
   Handles HTML tags: closes open tags before split, reopens in next chunk."
  ([text] (split-message text telegram-max-length))
  ([text max-length]
   (if (<= (count text) max-length)
     (when-not (str/blank? text) [text])
     (let [[head tail] (html-safe-split text max-length)]
       (cons head (split-message tail max-length))))))
