(ns clj-harness.telegram-polling-test
  (:require [clj-harness.telegram :as tg]
            [clojure.test :refer [deftest is testing]]))

(deftest polling-delivers-unacknowledged-startup-updates
  (doseq [initial-failure? [false true]]
    (testing (if initial-failure? "initial connection failure is recoverable"
                 "the first pending update is not discarded")
      (let [requests (atom [])
            received (atom [])
            failure (atom nil)
            delivery-attempt (if initial-failure? 2 1)
            update {"update_id" 41
                    "message" {"message_id" 17 "text" "queued search"
                               "from" {"id" 42} "chat" {"id" 42}}}
            worker (Thread.
                    ^Runnable
                    (fn []
                      (try
                        (tg/poll-loop #(swap! received conj %) :interval-ms 0)
                        (catch InterruptedException _)
                        (catch Exception error (reset! failure error)))))]
        (with-redefs [tg/get-updates
                      (fn [& options]
                        (let [attempt (count (swap! requests conj (apply hash-map options)))]
                          (cond
                            (and initial-failure? (= 1 attempt))
                            (throw (ex-info "Initial connection failure" {}))

                            (= delivery-attempt attempt) {"result" [update]}

                            :else
                            (do (.interrupt (Thread/currentThread)) {"result" []}))))]
          (.setDaemon worker true)
          (try
            (.start worker)
            (.join worker 2000)
            (is (= ["queued search"] (mapv :text @received)))
            (is (nil? @failure))
            (is (= 42 (:offset (last @requests))))
            (finally (.interrupt worker) (.join worker 2000))))))))
