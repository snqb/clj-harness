(ns clj-harness.telegram.format
  "Telegram HTML formatting for LLM-generated markdown.

  Pipeline:
    LLM markdown → md->html → split-html → parse_mode='HTML'

  Based on nanobot's _markdown_to_telegram_html() pattern
  (https://github.com/HKUDS/nanobot/pull/3355).

  Key design decisions:
  - Use Telegram HTML (not MarkdownV2) — no strict escaping, no parse failures
  - Headers (#, ##) become <b>bold</b> since Telegram has no heading entity
  - Escape HTML entities BEFORE converting markdown (safe order)
  - Split at 4096 chars respecting HTML tag boundaries
  - Strips markdown tables (| --- |) — they don't render on Telegram mobile"
  (:require [clojure.string :as str]))

;; ══════════════════════ HTML ESCAPE ══════════════════════

(defn escape-html
  "Escape HTML special chars. Safe to call on already-escaped text."
  [text]
  (-> text
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")))

;; ══════════════════════ MARKDOWN → TELEGRAM HTML ══════════════════════

(def ^:private inline-code-placeholder-prefix "◊ICODE◊")

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

  Also STRIPS any markdown tables (| --- |) since they don't render on Telegram.

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
    (let [;; Step 0: Strip markdown tables (they don't render on Telegram)
          no-tables (str/replace text #"(?m)^\|.*\|$" "")
          clean (str/replace no-tables #"(?m)^\|[\s\-:]+\|$" "")
          ;; Step 1: Save inline code before escaping
          [t1 inline-codes] (save-matches clean #"`([^`]+)`"
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
          t14 (str/replace t13 #"(?m)^---+$" "<code>———</code>")]
      t14)))

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
   Used during streaming mid-edits so users see clean text
   instead of raw markdown while the response is being generated."
  [text]
  (reduce (fn [t [pat repl]] (str/replace t pat repl))
          (or text "")
          md-strip-patterns))

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
