(ns clj-harness.stream-test
  (:require [clj-harness.stream :as stream]
            [clojure.test :refer [deftest is testing]]))

(deftest notify-status-calls-callback-with-single-string
  (testing "status-cb receives the full status string as ONE argument"
    (let [received (atom nil)
          arg-count (atom nil)
          cb (fn [& args]
               (reset! arg-count (count args))
               (reset! received (first args)))]
      (stream/notify-status! cb :starting)
      ;; The bug: (apply status-cb a-string) splattered the string into
      ;; per-character args. A correct call passes exactly ONE arg.
      (is (= 1 @arg-count) "status-cb must be called with exactly one argument")
      (is (string? @received) "the argument must be the whole status string")
      (is (= "🧠 Анализирую запрос..." @received)))))

(deftest notify-status-passes-tool-name
  (testing "tool-call phase includes the real tool name"
    (let [received (atom nil)]
      (stream/notify-status! (fn [s] (reset! received s)) :tool-call :tool-name "search")
      (is (= "🔧 Выполняю search..." @received)))))

(deftest notify-status-nil-callback-is-noop
  (testing "nil status-cb does not throw"
    (is (nil? (stream/notify-status! nil :starting)))))

(deftest notify-status-swallows-callback-errors
  (testing "a throwing callback never breaks the agent loop"
    (is (nil? (stream/notify-status! (fn [_] (throw (ex-info "boom" {}))) :starting)))))

(deftest notify-status-works-with-1-arity-callback
  (testing "a strict 1-arity callback (the common case) works without splatter"
    (let [received (atom nil)
          ;; 1-arity ONLY — would throw 'Wrong number of args' under the old bug
          cb (fn [s] (reset! received s))]
      (stream/notify-status! cb :after-tool)
      (is (= "📊 Обрабатываю результаты..." @received)))))
