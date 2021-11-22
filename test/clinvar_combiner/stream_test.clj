(ns clinvar-combiner.stream-test
  (:require [clinvar-combiner.stream :as stream]
            [clojure.data :refer [diff]]
            [clojure.test :as test]))

(defn is-expected-eq-actual
  [expected actual]
  (test/is (= expected actual) (str "Expected did not match actual: "
                                    (clojure.data/diff expected actual))))

(test/deftest max-key-in-test
  (test/testing "Trivial case, nil"
    (let [expected nil
          actual (stream/max-key-in [] :testkey)]
      (is-expected-eq-actual expected actual)))

  (test/testing "Missing key"
    ; Even if key is missing
    (let [input [{:a 10}
                 {:b 20}]]
      (test/is (thrown? Exception (stream/max-key-in input :testkey))
               (str "Expected exception: " input))))

  (test/testing "One item"
    (let [input [{:testkey 10}]
          expected {:testkey 10}
          actual (stream/max-key-in input :testkey)]
      (is-expected-eq-actual expected actual)))

  (test/testing "Multiple items"
    (let [input [{:testkey 10}
                 {:testkey 30}
                 {:testkey 20}
                 {:testkey -1}]
          expected {:testkey 30}
          actual (stream/max-key-in input :testkey)]
      (is-expected-eq-actual expected actual)))
  )


(test/deftest max-offsets-for-partitions-test
  (test/testing "Multiple cases"
    (let [input [; topic1 - 0 (simple, multiple in order)
                 {:topic-name :topic1
                  :partition 0
                  :offset 10}
                 {:topic-name :topic1
                  :partition 0
                  :offset 12}
                 ; topic1 - 1 (out of order)
                 {:topic-name :topic1
                  :partition 1
                  :offset 10}
                 {:topic-name :topic1
                  :partition 1
                  :offset 6}
                 ; topic2 - 0 (different topic name)
                 {:topic-name :topic2
                  :partition 0
                  :offset 20}
                 {:topic-name :topic2
                  :partition 0
                  :offset 21}
                 ; topic3 - 0 (multiple different topic names, same offset diff partitions)
                 {:topic-name :topic3
                  :partition 0
                  :offset 30}
                 {:topic-name :topic3
                  :partition 0
                  :offset 40}
                 {:topic-name :topic3
                  :partition 1
                  :offset 40}]
          expected [{:topic-name :topic1
                     :partition 0
                     :offset 12}
                    {:topic-name :topic1
                     :partition 1
                     :offset 10}
                    {:topic-name :topic2
                     :partition 0
                     :offset 21}
                    {:topic-name :topic3
                     :partition 0
                     :offset 40}
                    {:topic-name :topic3
                     :partition 1
                     :offset 40}]
          actual (stream/max-offsets-for-partitions input)]
      (is-expected-eq-actual expected actual))))
