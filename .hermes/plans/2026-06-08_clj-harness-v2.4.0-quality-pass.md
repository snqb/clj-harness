# clj-harness v2.4.0 Quality Pass — Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Fix 3 correctness bugs, clean up code hygiene, add tests for core modules, and upgrade documentation — all non-breaking, targeting a v2.4.0 release.

**Architecture:** Incremental cleanup across 5 phases: bugs → hygiene → tests → docs → release. No API changes, no removed public vars, no behavioral changes users would notice (except bugs being fixed).

**Tech Stack:** Clojure 1.12, deps.edn, clojure.test, Cheshire, Aero, next.jdbc, SQLite. No new dependencies.

**Constraints:** Non-breaking. All `create-bot` calls, tool defs, handler signatures, public API functions remain identical. Downstream dependents (tapalakbot-v2, cljr-site-factory, cljr-wedding-factory, pb-bot) must continue working with zero code changes.

---

## Phase 1: Correctness Fixes (3 bugs)

### Task 1: Remove debug println from agent_loop.clj

**Objective:** Remove the production debug println that leaks LLM content to stdout.

**Files:**
- Modify: `src/clj_harness/agent_loop.clj:159`

**Step 1: Remove the println**

Open `src/clj_harness/agent_loop.clj` line 159. Replace:

```clojure
          (do (println "[DEBUG] :fatal reached, content:" (pr-str (when content (subs content 0 (min 50 (count content))))) "reason:" (:reason checked))
              [(assoc state :phase :done :response {:content (if step-fail? fallback (or content fallback))})
               [(fx/make-emit-event fx/event-turn-end :reason :nudge-fatal)]])
```

With:

```clojure
          [(assoc state :phase :done :response {:content (if step-fail? fallback (or content fallback))})
           [(fx/make-emit-event fx/event-turn-end :reason :nudge-fatal)]])
```

**Step 2: Verify**

```bash
cd /Users/sn/Projects/clj-harness
grep -n 'println.*DEBUG' src/clj_harness/agent_loop.clj
# Expected: no output — the println should be gone
```

**Step 3: Run smoke test**

```bash
clojure -M -e '(doseq [n (quote [clj-harness.core clj-harness.agent-loop])] (require n)) (println :ok)'
# Expected: :ok
```

**Step 4: Commit**

```bash
git add src/clj_harness/agent_loop.clj
git commit -m "fix: remove debug println in agent_loop fatal path"
```

---

### Task 2: Fix compaction sampling bug in compact.clj

**Objective:** Fix `compact.clj` line 74 which takes summary messages only from the first half of `older`, not a representative sample.

**Files:**
- Modify: `src/clj_harness/compact.clj:74`
- Test: `test/clj_harness/compact_test.clj` (new)

**Step 1: Write failing test**

Create `test/clj_harness/compact_test.clj`:

```clojure
(ns clj-harness.compact-test
  (:require [clj-harness.compact :as compact]
            [clojure.test :refer [deftest is testing]]))

(deftest summary-sample-includes-early-and-late-messages
  (testing "compaction summary pulls from both early and late messages"
    (let [early-msg {"role" "user" "content" "AAAA"}
          late-msg  {"role" "user" "content" "ZZZZ"}
          messages (into [early-msg]
                         (repeat 18 {"role" "assistant" "content" "padding"})
                         [late-msg])
          summary-called (atom nil)
          summarize-fn (fn [msgs]
                         (reset! summary-called msgs)
                         "fake summary")]
      (compact/compact-history
       messages
       {:threshold 10    ;; force compaction (token estimate will be >> 10)
        :summarize-fn summarize-fn})
      (let [called @summary-called
            content-str (apply str (map #(get % "content") called))]
        (is (some? called))
        (is (str/includes? content-str "AAAA")
            "should include early message")
        (is (str/includes? content-str "ZZZZ")
            "should include late message")))))
```

**Step 2: Run to verify failure**

```bash
cd /Users/sn/Projects/clj-harness
clojure -M:test -n clj-harness.compact-test/summary-sample-includes-early-and-late-messages
# Expected: FAIL — the current code only grabs from the first half
```

**Step 3: Fix the sampling**

In `src/clj_harness/compact.clj` line 74, replace:

```clojure
              summary-msgs (vec (take split-at (take split-at older)))
```

With:

```clojure
              summary-msgs (vec (concat
                                 (take split-at older)        ;; early messages
                                 (take-last split-at older))) ;; late messages
```

**Step 4: Run test to verify pass**

```bash
clojure -M:test -n clj-harness.compact-test
# Expected: 1 test, 1 assertion, PASS
```

**Step 5: Run quality checks**

```bash
clojure -M -e '(doseq [n (quote [clj-harness.compact])] (require n)) (println :ok)'
clojure-lsp format --filenames src/clj_harness/compact.clj
clojure-lsp diagnostics --filenames src/clj_harness/compact.clj
```

**Step 6: Commit**

```bash
git add test/clj_harness/compact_test.clj src/clj_harness/compact.clj
git commit -m "fix: compaction summary now samples both early and late messages"
```

---

### Task 3: Remove unnecessary LLM call in wrap-tools-v2

**Objective:** Remove the dummy LLM call in middleware.clj that makes an HTTP request during bot creation without using the result.

**Files:**
- Modify: `src/clj_harness/middleware.clj:183-188`

**Step 1: Replace the dummy-resp block with direct config resolution**

In `src/clj_harness/middleware.clj`, the `wrap-tools-v2` function body at lines 183-188 currently computes `dummy-resp` but never uses it. The model/provider are later resolved independently anyway (lines 196-201). The dummy call was intended to resolve defaults but the subsequent code already handles this correctly.

Replace lines 183-188:

```clojure
         ;; Resolve defaults by calling handler once with empty messages
         ;; to see what model/provider it uses
         dummy-resp (try (handler {:messages [] :tools _tool-schemas})
                         (catch Exception _ {:content "" :tool-calls nil}))
```

With just a comment:

```clojure
         ;; Model/provider resolved from ctx or config at call time (see llm-fn below)
```

(Just delete the dummy-resp let binding — it's unused.)

Also remove the trailing paren that closes its enclosing `let` on the old line 188. The new structure is:

```clojure
   (let [_tool-map (into {} (map (fn [t] [(tl/tool-name t) t]) tools))
         _tool-schemas (mapv tool->openai-schema tools)]
     (fn [{:keys [messages max-turns heap nudges events>] :as ctx}]
```

That's it — remove the dummy-resp binding line entirely. The `let` before it has `_tool-map` and `_tool-schemas` as the only bindings.

**Step 2: Verify**

```bash
cd /Users/sn/Projects/clj-harness
clojure -M -e '(doseq [n (quote [clj-harness.core clj-harness.middleware])] (require n)) (println :ok)'
# Expected: :ok (and no LLM call should fire during require — the call was at runtime inside the returned fn)
```

**Step 3: Run quality checks**

```bash
clojure-lsp format --filenames src/clj_harness/middleware.clj
clojure-lsp diagnostics --filenames src/clj_harness/middleware.clj
# Expected: the "unused binding dummy-resp" warning should now be gone
```

**Step 4: Run smoke test and jscpd**

```bash
clojure -M -e '(doseq [n (quote [clj-harness.core clj-harness.guardrails clj-harness.stream clj-harness.telegram clj-harness.observe clj-harness.dashboard])] (require n)) (println :ok)'
bunx jscpd src --threshold 3 --min-lines 5 --min-tokens 30 --reporters console
```

**Step 5: Commit**

```bash
git add src/clj_harness/middleware.clj
git commit -m "fix: remove unnecessary LLM call during bot creation in wrap-tools-v2"
```

---

## Phase 2: Code Hygiene (cleanup warnings)

### Task 4: Clean unused imports and bindings in agent_loop.clj

**Objective:** Remove the unused `clojure.tools.logging` import and unused bindings flagged by clojure-lsp.

**Files:**
- Modify: `src/clj_harness/agent_loop.clj`

**Step 1: Remove unused namespace import**

Line 43: remove `[clojure.tools.logging :as log]` from the `:require` form.

**Step 2: Fix unused binding at line 113**

In `idle-transition`, line 113 binds `nudge-opts` but never uses it. The destructuring is:

```clojure
          nudge-opts (:nudge-opts state)
```

This assignment is genuinely unused. Remove this line — the `nudge-opts` binding exists in the outer `let` but isn't needed here since `idle-transition` only uses `:messages`, `:tool-schemas`, `:model`, `:provider`, and `:turn` from state.

**Step 3: Fix unused binding at line 362**

In the `::execute-tool` handler in the `run` function, line 362:

```clojure
                  tool-call (::tool-call effect)
```

The variable is bound but never referenced (the code uses `effect` directly to access fields). Remove this line.

**Step 4: Verify**

```bash
cd /Users/sn/Projects/clj-harness
clojure-lsp diagnostics --filenames src/clj_harness/agent_loop.clj
# Expected: no more warnings about unused-namespace, unused-binding for those 2 bindings
```

**Step 5: Run smoke test**

```bash
clojure -M -e '(doseq [n (quote [clj-harness.agent-loop])] (require n)) (println :ok)'
```

**Step 6: Commit**

```bash
git add src/clj_harness/agent_loop.clj
git commit -m "chore: remove unused imports and bindings in agent_loop.clj"
```

---

### Task 5: Clean unused imports in middleware.clj

**Objective:** Remove unused `put!` refer from core.async and unused `clj-harness.effects` import.

**Files:**
- Modify: `src/clj_harness/middleware.clj`

**Step 1: Clean imports**

Line 12: change `[clojure.core.async :refer [chan sliding-buffer put! close!]]` to `[clojure.core.async :refer [chan sliding-buffer close!]]`

Line 14: remove the entire `[clj-harness.effects :as fx]` line (it's only used indirectly through `agent-loop`).

**Step 2: Verify**

```bash
cd /Users/sn/Projects/clj-harness
clojure-lsp diagnostics --filenames src/clj_harness/middleware.clj
# Expected: the unused-referred-var and unused-namespace warnings are gone
```

**Step 3: Smoke test**

```bash
clojure -M -e '(doseq [n (quote [clj-harness.core])] (require n)) (println :ok)'
```

**Step 4: Commit**

```bash
git add src/clj_harness/middleware.clj
git commit -m "chore: remove unused imports in middleware.clj"
```

---

### Task 6: Clean redundant constructs in effects.clj and telegram.clj

**Objective:** Fix the redundant `let` in effects.clj and redundant `do` in telegram.clj.

**Files:**
- Modify: `src/clj_harness/effects.clj:84`
- Modify: `src/clj_harness/telegram.clj:386`

**Step 1: Fix effects.clj redundant let**

Lines 82-90 in effects.clj — the outer `let` at line 82 has only one binding (the inner let). Simplify:

Before:
```clojure
    ::execute-tool
    (let [{:keys [tool-map tool-post-process heap abort-signal]} env
          tool-call (::tool-call effect)]
      ;; Build on-update callback from events> if available
      (let [on-update (when-let [events> (:events> env)]
                        (fn [partial]
                          (put! events>
                                {::event-type ::tool-update
                                 :tool-name (:name tool-call)
                                 :partial partial})))]
        (tl/execute-tool-call tool-map tool-post-process heap
                              tool-call abort-signal on-update)))
```

After:
```clojure
    ::execute-tool
    (let [{:keys [tool-map tool-post-process heap abort-signal]} env
          tool-call (::tool-call effect)
          on-update (when-let [events> (:events> env)]
                      (fn [partial]
                        (put! events>
                              {::event-type ::tool-update
                               :tool-name (:name tool-call)
                               :partial partial})))]
      (tl/execute-tool-call tool-map tool-post-process heap
                            tool-call abort-signal on-update))
```

**Step 2: Fix telegram.clj redundant do**

In `make-handler`, the non-streaming fallback at line 386:

```clojure
              ;; Non-streaming fallback
              (do
                (let [t0 ...
```

The `(do` on line 386 is redundant — the `if` branch already provides an implicit `do`. Remove the `do` wrapper (keep the body at the same indentation level). This means:

Before:
```clojure
              (do
                (let [t0 (System/currentTimeMillis)
                      ...
```

After:
```clojure
              (let [t0 (System/currentTimeMillis)
                    ...
```

And un-indent the closing parens accordingly.

**Step 3: Verify**

```bash
clojure-lsp format --filenames src/clj_harness/effects.clj src/clj_harness/telegram.clj
clojure-lsp diagnostics --filenames src/clj_harness/effects.clj src/clj_harness/telegram.clj
```

**Step 4: Smoke test**

```bash
clojure -M -e '(doseq [n (quote [clj-harness.core clj-harness.stream])] (require n)) (println :ok)'
```

**Step 5: Commit**

```bash
git add src/clj_harness/effects.clj src/clj_harness/telegram.clj
git commit -m "chore: remove redundant let/do constructs in effects and telegram"
```

---

## Phase 3: Test Coverage for Core Modules

### Task 7: Add deps.edn test alias and test infrastructure

**Objective:** Add a `:test` alias to deps.edn so tests can be run easily.

**Files:**
- Modify: `deps.edn`

**Step 1: Add test alias**

Append to the `:aliases` map in `deps.edn`:

```clojure
           :test {:extra-paths ["test"]
                  :extra-deps {org.clojure/test.check {:mvn/version "1.1.1"}}}
```

The full `:aliases` map should become:

```clojure
 :aliases {:run {:main-opts ["-m" "clj-harness.demo"]}
           :repl {:main-opts ["-r"]}
           :eval {:extra-paths ["dev"]
                  :main-opts ["-m" "clj-harness.eval.guardrail-bench"]}
           :test {:extra-paths ["test"]
                  :extra-deps {org.clojure/test.check {:mvn/version "1.1.1"}}}}
```

**Step 2: Verify**

```bash
cd /Users/sn/Projects/clj-harness
clojure -M:test -e '(println "test paths loaded OK")'
# Expected: test paths loaded OK
```

**Step 3: Run existing test**

```bash
clojure -M:test -m kaocha.runner test/clj_harness/telegram/format_test.clj
# Or simpler:
clojure -M:test -e '(require (quote clj-harness.telegram.format-test)) (clojure.test/run-tests)'
```

**Step 4: Commit**

```bash
git add deps.edn
git commit -m "chore: add :test alias with test.check dependency"
```

---

### Task 8: Add guardrails tests

**Objective:** Test the core guardrail functions: normalize-tool-call, validate-response, check-response, enforce-steps, and the retry/step-block limits.

**Files:**
- Create: `test/clj_harness/guardrails_test.clj`

**Step 1: Write tests**

```clojure
(ns clj-harness.guardrails-test
  (:require [clj-harness.guardrails :as gr]
            [clojure.test :refer [deftest is testing]]))

(defn openai-tc [name args]
  {"id" (str "call_" name) "function" {"name" name "arguments" (json/write-str args)}})

(defn raw-tc [name args]
  {:id (str "call_" name) :function {:name name :arguments args}})

;; ── normalize-tool-call ──

(deftest normalize-valid-openai-tool-call
  (let [tc (openai-tc "search" {"query" "test"})
        result (gr/normalize-tool-call tc)]
    (is (:ok? result))
    (is (= "search" (:name (:tool-call result))))
    (is (= {"query" "test"} (:args (:tool-call result))))))

(deftest normalize-missing-name
  (let [result (gr/normalize-tool-call {"function" {"arguments" "{}"}})]
    (is (not (:ok? result)))
    (is (str/includes? (:reason result) "missing"))))

(deftest normalize-invalid-json-args
  (let [tc {"id" "call_1" "function" {"name" "search" "arguments" "not-json{"}}
        result (gr/normalize-tool-call tc)]
    (is (not (:ok? result)))
    (is (str/includes? (:reason result) "invalid JSON"))))

(deftest normalize-tool-calls-valid
  (let [calls [(openai-tc "search" {"q" "x"}) (openai-tc "lookup" {"id" 1})]
        result (gr/normalize-tool-calls calls)]
    (is (:ok? result))
    (is (= 2 (count (:tool-calls result))))))

(deftest normalize-tool-calls-one-bad
  (let [calls [(openai-tc "search" {"q" "x"})
               {"id" "bad" "function" {"name" "" "arguments" "{}"}}]
        result (gr/normalize-tool-calls calls)]
    (is (not (:ok? result)))
    (is (str/includes? (:reason result) "missing"))))

;; ── validate-response ──

(deftest validate-text-response-passed-through
  (let [state (gr/make-state)
        config {:tool-names #{"search" "answer"}}
        resp {:content "Hello" :tool-calls []}]
    (is (= :text (:action (gr/validate-response state config resp))))))

(deftest validate-valid-tool-call-executes
  (let [state (gr/make-state)
        config {:tool-names #{"search"}}
        resp {:tool-calls [(openai-tc "search" {"q" "test"})]}]
    (is (= :execute (:action (gr/validate-response state config resp))))))

(deftest validate-unknown-tool-returns-retry
  (let [state (gr/make-state)
        config {:tool-names #{"search"}}
        resp {:tool-calls [(openai-tc "unknown" {"x" 1})]}]
    (is (= :retry (:action (gr/validate-response state config resp))))))

(deftest validate-retry-count-capped
  (let [state (gr/make-state {:retry-count 4}) ;; over max-retries (3 by default)
        config {:tool-names #{"search"}}
        resp {:tool-calls [(openai-tc "unknown" {"x" 1})]}]
    (is (= :fatal (:action (gr/validate-response state config resp))))))

(deftest validate-text-when-tool-required
  (let [state (gr/make-state)
        config {:tool-names #{"search"} :require-tool? true}
        resp {:content "I'll just answer directly"}]
    (is (= :retry (:action (gr/validate-response state config resp))))))

;; ── enforce-steps ──

(deftest enforce-steps-blocks-terminal-before-required
  (let [state (gr/make-state)
        config {:required-steps ["search"] :terminal-tools #{"answer"}}
        tool-calls [{:name "answer" :args {"text" "done"}}]]
    (let [result (gr/enforce-steps state config tool-calls)]
      (is (= :step-blocked (:action result)))
      (is (str/includes? ((:nudge result) :content) "search")))))

(deftest enforce-steps-allows-required-tool
  (let [state (gr/make-state {:completed #{"search"}})
        config {:required-steps ["search"] :terminal-tools #{"answer"}}
        tool-calls [{:name "answer" :args {"text" "done"}}]]
    (is (= :execute (:action (gr/enforce-steps state config tool-calls))))))

(deftest enforce-steps-terminal-with-no-pending
  (let [state (gr/make-state {:completed #{"search"}})
        config {:required-steps [] :terminal-tools #{"answer"}}
        tool-calls [{:name "answer" :args {"text" "done"}}]]
    (is (= :execute (:action (gr/enforce-steps state config tool-calls))))))

(deftest enforce-steps-fatal-after-max-blocks
  (let [state (gr/make-state {:step-block-count 4}) ;; over max 3
        config {:required-steps ["search"] :terminal-tools #{"answer"}}
        tool-calls [{:name "answer" :args {"text" "done"}}]]
    (is (= :fatal (:action (gr/enforce-steps state config tool-calls))))))

;; ── check-response (composition) ──

(deftest check-response-composition
  (let [state (gr/make-state)
        config {:tool-names #{"search"} :required-steps ["search"] :terminal-tools #{"answer"}}
        resp {:tool-calls [(openai-tc "answer" {"text" "done"})]}]
    (is (= :step-blocked (:action (gr/check-response state config resp))))))

;; ── record-executed ──

(deftest record-executed-tracks-success
  (let [state (gr/make-state)
        state' (gr/record-executed state ["search"])]
    (is (contains? (:completed state') "search"))
    (is (zero? (:retry-count state')))
    (is (zero? (:step-block-count state')))))

;; ── parse-args ──

(deftest parse-args-handles-all-forms
  (is (:ok? (gr/parse-args nil)))
  (is (:ok? (gr/parse-args {"a" 1})))
  (is (:ok? (gr/parse-args "{\"a\":1}")))
  (is (not (:ok? (gr/parse-args #{})))))
```

**Step 2: Run tests**

```bash
cd /Users/sn/Projects/clj-harness
clojure -M:test -e '(require (quote clj-harness.guardrails-test)) (clojure.test/run-tests (quote clj-harness.guardrails-test))'
# Expected: 14 tests, all PASS
```

**Step 3: Commit**

```bash
git add test/clj_harness/guardrails_test.clj
git commit -m "test: add guardrails module tests (14 tests covering normalize, validate, enforce)"
```

---

### Task 9: Add heap tests

**Objective:** Test heap store, fetch, GC, and LRU eviction.

**Files:**
- Create: `test/clj_harness/heap_test.clj`

**Step 1: Write tests**

```clojure
(ns clj-harness.heap-test
  (:require [clj-harness.heap :as heap]
            [clojure.test :refer [deftest is testing]]))

(deftest small-result-returns-nil
  (let [h (heap/create-heap)
        result (heap/store! h "search" "short")]
    (is (nil? result) "results under threshold should not be stored")))

(deftest large-result-stored-with-heap-id
  (let [h (heap/create-heap)
        big-result (apply str (repeat 5000 "x"))
        result (heap/store! h "search" big-result)]
    (is result)
    (is (string? (:heap-id result)))
    (is (:truncated result))
    (is (= 5000 (:size result)))))

(deftest fetch-retrieves-stored-result
  (let [h (heap/create-heap)
        big-result (apply str (repeat 3000 "ABC"))
        stored (heap/store! h "lookup" big-result)
        fetched (heap/fetch h (:heap-id stored))]
    (is (= big-result fetched))))

(deftest fetch-expired-entry-returns-nil
  (let [h (heap/create-heap)
        big-result (apply str (repeat 3000 "X"))]
    (binding [heap/*heap-ttl-ms* -1]  ;; force immediate expiry
      (let [stored (heap/store! h "search" big-result)]
        (is (nil? (heap/fetch h (:heap-id stored))))))))

(deftest fetch-missing-key-returns-nil
  (let [h (heap/create-heap)]
    (is (nil? (heap/fetch h "nonexistent")))))

(deftest fetch-with-query-filters-lines
  (let [h (heap/create-heap)
        data "apple banana\ncat dog\napple pie\nxyz"
        _ (heap/store! h "search" (apply str (repeat 2000 data)))
        ;; Wait, store! only stores > 3000. Let's just store a big blob with target content.
        _ (let [big (str (apply str (repeat 2500 "padding")) "\n\nfind-me-line\n" (apply str (repeat 600 "end")))]
            (heap/store! h "search" big))
        ;; Fetch the entry
        entries (get-in @h [:entries])
        hid (first (keys entries))]
    (is (string? hid))
    (let [result (heap/fetch-with-query h hid "find-me")]
      (is (str/includes? result "find-me-line"))
      (is (not (str/includes? result "padding"))))))

(deftest gc-removes-expired-entries
  (let [h (heap/create-heap)]
    (binding [heap/*heap-ttl-ms* -1]
      (dotimes [i 3]
        (heap/store! h "search" (apply str (repeat 3000 (str "test" i)))))
      (let [before (count (:entries @h))]
        (is (= 3 before))
        (let [removed (heap/gc! h)]
          (is (= 3 removed))
          (is (zero? (count (:entries @h)))))))))

(deftest lru-eviction-on-max-entries
  (let [h (heap/create-heap)]
    (binding [heap/*heap-max-entries* 3]
      (dotimes [i 5]
        (heap/store! h "search" (apply str (repeat 3000 (str "entry" i)))))
      (let [entries (:entries @h)
            ids (keys entries)]
        (is (= 3 (count entries)) "should evict oldest, keeping 3")
        ;; The 2 oldest (entry0, entry1) should be gone
        (is (not (contains? entries "1")) "oldest entry should be evicted")))))

(deftest clear-resets-heap
  (let [h (heap/create-heap)]
    (heap/store! h "search" (apply str (repeat 3000 "x")))
    (heap/clear! h)
    (is (empty? (:entries @h)))
    (is (= 1 (count (:order @h))) "counter should be preserved")))

(deftest stats-reports-accurately
  (let [h (heap/create-heap)]
    (heap/store! h "tool-a" (apply str (repeat 3000 "a")))
    (heap/store! h "tool-b" (apply str (repeat 4000 "b")))
    (heap/store! h "tool-a" (apply str (repeat 5000 "c")))
    (let [s (heap/stats h)]
      (is (= 3 (:entries s)))
      (is (= 12000 (:total-size s)))
      (is (= {"tool-a" 2 "tool-b" 1} (:tool-breakdown s))))))
```

**Step 2: Run tests**

```bash
cd /Users/sn/Projects/clj-harness
clojure -M:test -e '(require (quote clj-harness.heap-test)) (clojure.test/run-tests (quote clj-harness.heap-test))'
# Expected: 9 tests, all PASS
```

**Step 3: Commit**

```bash
git add test/clj_harness/heap_test.clj
git commit -m "test: add heap module tests (9 tests covering store, fetch, GC, LRU, stats)"
```

---

### Task 10: Add session memory tests

**Objective:** Test the in-memory session atom operations.

**Files:**
- Create: `test/clj_harness/session/memory_test.clj`

**Step 1: Write tests**

```clojure
(ns clj-harness.session.memory-test
  (:require [clj-harness.session.memory :as mem]
            [clojure.test :refer [deftest is testing]]))

(deftest make-session-empty-state
  (let [s (mem/make-session)]
    (is (some? s))
    (is (contains? @s "messages"))
    (is (empty? (get @s "messages")))
    (is (contains? @s "data"))
    (is (empty? (get @s "data")))))

(deftest make-session-restores-state
  (let [s (mem/make-session {"messages" [{"role" "user" "content" "hi"}]
                             "data" {"theme" "dark"}})]
    (is (= 1 (count (get @s "messages"))))
    (is (= "dark" (get-in @s ["data" "theme"])))))

(deftest session-add-appends-messages
  (let [s (mem/make-session)]
    (mem/session-add! s "user" "Hello")
    (mem/session-add! s "assistant" "Hi!")
    (let [msgs (mem/session-messages s)]
      (is (= 2 (count msgs)))
      (is (= "Hello" (get (first msgs) "content")))
      (is (= "assistant" (get (second msgs) "role"))))))

(deftest session-messages-prepends-summary
  (let [s (mem/make-session)]
    (swap! s assoc "summary" "Prior conversation about weather.")
    (mem/session-add! s "user" "More?")
    (let [msgs (mem/session-messages s)]
      (is (= 2 (count msgs)))
      (is (= "system" (get (first msgs) "role")))
      (is (str/includes? (get (first msgs) "content") "weather")))))

(deftest session-data-and-update
  (let [s (mem/make-session)]
    (is (= {} (mem/session-data s)))
    (mem/session-update-data! s assoc "lang" "ru")
    (is (= {"lang" "ru"} (mem/session-data s)))
    (mem/session-update-data! s merge {"city" "Bishkek"})
    (is (= {"lang" "ru" "city" "Bishkek"} (mem/session-data s)))))

(deftest session-add-thread-safe
  (let [s (mem/make-session)
        n 100
        threads (doall (repeatedly n
                                   #(Thread. (fn []
                                               (mem/session-add! s "user" (str "msg" (rand-int 100)))))))]
    (doseq [t threads] (.start t))
    (doseq [t threads] (.join t))
    (is (= (+ n (count (get @s "messages") 0)) (count (get @s "messages"))))))
```

**Step 2: Run tests**

```bash
cd /Users/sn/Projects/clj-harness
clojure -M:test -e '(require (quote clj-harness.session.memory-test)) (clojure.test/run-tests (quote clj-harness.session.memory-test))'
# Expected: 6 tests, all PASS
```

**Step 3: Commit**

```bash
git add test/clj_harness/session/memory_test.clj
git commit -m "test: add session memory module tests (6 tests covering create, add, data, thread-safety)"
```

---

### Task 11: Run full test suite and quality gates

**Objective:** Run all tests to confirm everything passes post-changes.

**Step 1: Run all tests**

```bash
cd /Users/sn/Projects/clj-harness
clojure -M:test -e "
(require
  '[clj-harness.telegram.format-test :as ft]
  '[clj-harness.compact-test :as ct]
  '[clj-harness.guardrails-test :as gt]
  '[clj-harness.heap-test :as ht]
  '[clj-harness.session.memory-test :as st])
(clojure.test/run-tests 'clj-harness.telegram.format-test
                        'clj-harness.compact-test
                        'clj-harness.guardrails-test
                        'clj-harness.heap-test
                        'clj-harness.session.memory-test)
"
# Expected: all tests PASS
```

**Step 2: Run quality gates**

```bash
clojure -M -e '(doseq [n (quote [clj-harness.core clj-harness.guardrails clj-harness.stream clj-harness.telegram clj-harness.observe clj-harness.dashboard])] (require n)) (println :ok)'
clojure-lsp diagnostics 2>&1 | grep -E "warning:" || echo "No new warnings"
bunx jscpd src --threshold 3 --min-lines 5 --min-tokens 30 --reporters console
```

**Step 3: Commit**

```bash
git commit -m "chore: post-cleanup quality gate pass — all tests green, no new warnings"
```

---

## Phase 4: Documentation and Skill Updates

### Task 12: Update AGENTS.md to v2.4.0

**Objective:** Bump version in AGENTS.md and add a note about the quality pass.

**Files:**
- Modify: `AGENTS.md`

**Step 1: Update version**

Change line 1 from `<!-- Updated: 2026-05-21 -->` to `<!-- Updated: 2026-06-08 -->`.

Change `Current: v2.3.0` to `Current: v2.4.0`.

**Step 2: Add changelog entry**

Add an entry at the bottom of AGENTS.md (before "Related"):

```markdown
## Release v2.4.0 (2026-06-08)

- **Fix:** Removed debug println in agent_loop fatal path
- **Fix:** Compaction summary now samples both early and late messages
- **Fix:** Removed unnecessary LLM call during bot creation in wrap-tools-v2
- **Chore:** Cleaned unused imports and bindings across agent_loop, middleware, effects, telegram
- **Test:** Added test coverage for guardrails, heap, session memory, and compaction (30+ tests)
```

**Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs: bump AGENTS.md to v2.4.0 with changelog"
```

---

### Task 13: Update clojure-harness skill

**Objective:** Sync the skill with new module map and version.

**Files:**
- Use: `skill_manage` tool on `clojure-harness`

**Step 1: Update the skill using skill_manage**

Use `skill_manage` with action `patch` on skill `clojure-harness` to update:
- The "Current: v2.2.0" text to "Current: v2.4.0"
- Add `compact.clj`, `heap.clj`, `guardrails.clj`, `agent_loop.clj`, `tool_loop.clj`, `effects.clj`, `observe.clj`, `dashboard.clj`, `skills.clj` to the file table (or replace the file table with the complete v2.x module listing)

The existing file table is:

```markdown
| File | Lines | Purpose |
|------|-------|---------|
| `core.clj` | 410 | Bot factory, middleware, LLM, MCP, sessions, compaction |
| `session/sqlite.clj` | 55 | SQLite persistence — sessions survive restarts |
| `telegram.clj` | 180 | Telegram Bot API — send, edit, typing, polling, handler |
| `telegram/format.clj` | 80 | md→html, escape, split, table stripping |
| `demo.clj` | 50 | Example bots and handlers |
```

Replace with:

```markdown
| File | Lines | Purpose |
|------|-------|---------|
| `core.clj` | 376 | Bot factory, session management, public API |
| `infra.clj` | 69 | Config, secrets, HTTP client |
| `llm.clj` | 73 | Provider dispatch, model resolution, core-agent handler |
| `mcp.clj` | 61 | MCPvisor JSON-RPC client, tool cache |
| `middleware.clj` | 228 | wrap-tools (imperative), wrap-tools-v2 (effect-driven), wrap-retry, wrap-logging |
| `agent_loop.clj` | 397 | Pure state machine — emits effect descriptions, no I/O |
| `effects.clj` | 146 | Effect interpreter — interprets effect descriptions into side effects |
| `guardrails.clj` | 240 | Tool-call validation, rescue, step enforcement, nudge system |
| `tool_loop.clj` | 175 | Shared tool-loop helpers: execute, format, fetch_result injection |
| `compact.clj` | 90 | Token estimation, LLM-based conversation summarization |
| `stream.clj` | 344 | SSE streaming LLM, stream-agent tool loop |
| `heap.clj` | 173 | Content-addressable store for large tool outputs |
| `session/memory.clj` | 57 | In-memory session atoms — thread-safe per-user state |
| `session/sqlite.clj` | 110 | SQLite persistence — sessions survive restarts |
| `observe.clj` | 118 | Ring-buffer event store for observability panel |
| `dashboard.clj` | 142 | Embedded JDK HttpServer web dashboard |
| `skills.clj` | 133 | EDN skill/design-system loader and prompt merging |
| `telegram.clj` | 414 | Telegram Bot API — send, edit, typing, polling, handler |
| `telegram/format.clj` | — | md->html, escape, split, table stripping |
| `telegram/streaming.clj` | — | Block-buffered streaming previews |
```

Also update the version note in the intro from "Current: v2.2.0" to "Current: v2.4.0".

**Step 2: Verify**

Check the skill with `skill_view(name="clojure-harness")` to confirm the updates landed.

**Step 3: Commit**

```bash
git add ~/.hermes/skills/clojure-harness/SKILL.md
git commit -m "docs: update clojure-harness skill to v2.4.0 with complete module map"
```

---

## Phase 5: Tag and Verify

### Task 14: Tag v2.4.0 and run final verification

**Objective:** Tag the release and verify downstream compatibility.

**Step 1: Tag**

```bash
cd /Users/sn/Projects/clj-harness
git tag -a v2.4.0 -m "v2.4.0: Quality pass — bug fixes, code hygiene, test coverage"
```

**Step 2: Verify downstream compatibility**

Check that tapalakbot-v2 still loads (as a representative downstream):

```bash
cd /Users/sn/Projects/tapalakbot-v2
clojure -M -e '(require (quote tapalakbot.bot)) (println :ok)'
# Expected: :ok
```

**Step 3: Push (if desired)**

```bash
git push origin v2.4.0
```

---

## Summary

| Phase | Tasks | Changes | Risk |
|-------|-------|---------|------|
| 1: Bugs | 3 tasks | Remove println, fix sampling, remove dummy LLM call | Low — targeted fixes |
| 2: Hygiene | 3 tasks | Clean imports, bindings, redundant constructs | None — cosmetic |
| 3: Tests | 5 tasks | Add ~30 tests for guardrails, heap, sessions, compaction | None — additive |
| 4: Docs | 2 tasks | Update AGENTS.md and skill | None — additive |
| 5: Release | 1 task | Tag v2.4.0 | Low — verify downstream |

**Total: 14 tasks**, all non-breaking. Zero API changes, zero removed symbols, zero behavioral changes beyond bug fixes. Downstream dependents require no code changes.
