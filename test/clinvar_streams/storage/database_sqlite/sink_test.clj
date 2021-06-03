(ns clinvar-streams.storage.database-sqlite.sink-test
  (:require [clinvar-streams.storage.database-sqlite.sink :as sink]
            [clojure.test :as test]
            [clojure.data]
            [cheshire.core :as json]))

(test/deftest variation-list-to-compound-test
  (test/testing "Trivial case, nil"
    (let [variations nil
          expected nil
          actual (sink/variation-list-to-compound variations)]
      (test/is (= expected actual) (str "Expected did not match actual: "
                                        (clojure.data/diff expected actual)))))
  (test/testing "Trivial case, no variations"
    (let [variations []
          expected nil
          actual (sink/variation-list-to-compound variations)]
      (test/is (= expected actual) (str "Expected did not match actual: "
                                        (clojure.data/diff expected actual)))))
  (test/testing "Trivial case, nil variation"
    (let [variations [nil]
          variations2 [nil nil]
          expected nil
          actual (sink/variation-list-to-compound variations)
          actual2 (sink/variation-list-to-compound variations2)]
      (test/is (= expected actual) (str "Expected did not match actual: "
                                        (clojure.data/diff expected actual)))
      (test/is (= expected actual2) (str "Expected did not match actual: "
                                         (clojure.data/diff expected actual2)))))
  (test/testing "Trivial case, one variation"
    (let [variations [{:id "v1"}]
          expected {:id "v1"}
          actual (sink/variation-list-to-compound variations)]
      (test/is (= expected actual) (str "Expected did not match actual: "
                                        (clojure.data/diff expected actual)))))

  (test/testing "General case, two children, attribute preserved"
    (let [variations [{:id "v1"
                       :child_ids (json/generate-string ["v2" "v3"])}
                      {:id "v2" :name "variation 2"}
                      {:id "v3" :name "variation 3"}]
          expected {:id "v1"
                    :child_ids ["v2" "v3"]
                    :child_variations [{:id "v2" :name "variation 2"}
                                       {:id "v3" :name "variation 3"}]}
          actual (sink/variation-list-to-compound variations)]
      (test/is (= expected actual) (str "Expected did not match actual: "
                                        (clojure.data/diff expected actual)))))

  (test/testing "General case, three layers"
    (let [variations [{:id "v1"
                       :child_ids (json/generate-string ["v2" "v3"])}
                      {:id "v2"
                       :child_ids (json/generate-string ["v2-1"])}
                      {:id "v3"
                       :child_ids (json/generate-string ["v3-1" "v3-2"])}
                      {:id "v2-1"}
                      {:id "v3-1"}
                      {:id "v3-2"}]
          expected {:id "v1"
                    :child_ids ["v2" "v3"]
                    :child_variations [{:id "v2"
                                        :child_ids ["v2-1"]
                                        :child_variations [{:id "v2-1"}]}
                                       {:id "v3"
                                        :child_ids ["v3-1" "v3-2"]
                                        :child_variations [{:id "v3-1"}
                                                           {:id "v3-2"}]}]}
          actual (sink/variation-list-to-compound variations)]
      (test/is (= expected actual) (str "Expected did not match actual: "
                                        (into [] (clojure.data/diff expected actual))))))
  (test/testing "General case, five layers"
    (let [variations [{:id "v1"
                       :child_ids (json/generate-string ["v1-1"])}
                      {:id "v1-1"
                       :child_ids (json/generate-string ["v1-2"])}
                      {:id "v1-2"
                       :child_ids (json/generate-string ["v1-3"])}
                      {:id "v1-3"
                       :child_ids (json/generate-string ["v1-4"])}
                      {:id "v1-4"}]
          expected {:id "v1"
                    :child_ids ["v1-1"]
                    :child_variations
                    [{:id "v1-1"
                      :child_ids ["v1-2"]
                      :child_variations
                      [{:id "v1-2"
                        :child_ids ["v1-3"]
                        :child_variations
                        [{:id "v1-3"
                          :child_ids ["v1-4"]
                          :child_variations
                          [{:id "v1-4"}]}
                         ]}]}]}
          actual (sink/variation-list-to-compound variations)]
      (test/is (= expected actual) (str "Expected did not match actual: "
                                        (into [] (clojure.data/diff expected actual))))))
  )
