(ns clj-harness.telegram-test
  (:require [clj-harness.telegram :as tg]
            [clojure.test :refer [deftest is testing]]))

(def parse-update tg/parse-update)

(deftest parse-update-routes-callbacks
  (let [parsed (parse-update
                {"callback_query"
                 {"id" "cb-1"
                  "data" "more:iphone"
                  "from" {"id" 42}
                  "message" {"message_id" 9
                             "message_thread_id" 17
                             "chat" {"id" -100123}}}})]
    (is (= {:callback-id "cb-1"
            :data "more:iphone"
            :user-id 42
            :chat-id -100123
            :msg-id 9
            :thread-id 17}
           parsed))))

(deftest parse-update-preserves-message-thread
  (testing "forum messages retain their topic identity"
    (is (= 17
           (:thread-id
            (parse-update
             {"message" {"message_id" 9
                         "message_thread_id" 17
                         "text" "iphone"
                         "from" {"id" 42}
                         "chat" {"id" -100123}}}))))))
