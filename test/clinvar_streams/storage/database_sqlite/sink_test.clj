(ns clinvar-streams.storage.database-sqlite.sink-test
  (:require [clinvar-streams.storage.database-sqlite.sink :as sink]
            [clojure.test :as test]
            [clojure.data]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

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

(test/deftest validate-variation-tree
  (test/testing "Error case, nil variation"
    (let [variation nil]
      (test/is (thrown? ExceptionInfo
                        (sink/validate-variation-tree variation))
               (str "Expected exception" variation))))
  (test/testing "Error case, empty variation"
    (let [variation {}]
      (test/is (thrown? ExceptionInfo
                        (sink/validate-variation-tree variation))
               (str "Expected exception: " variation))))
  (test/testing "Error case, missing subclass"
    (let [variation {:id "v1"}]
      (test/is (thrown? ExceptionInfo
                        (sink/validate-variation-tree variation))
               (str "Expected exception: " variation))))
  (test/testing "Error case, unrecognized subclass"
    (let [variation {:id "v1" :subclass_type "fakesubclass"}]
      (test/is (thrown? ExceptionInfo
                        (sink/validate-variation-tree variation))
               (str "Expected exception: " variation))))
  (test/testing "Error case, unrecognized nested subclass"
    (let [variation {:id "v1"
                     :subclass_type "Haplotype"
                     :child_variations
                     [{:id "v1-1"
                       :subclass_type "fakesubclass"}]}]
      (test/is (thrown? ExceptionInfo
                        (sink/validate-variation-tree variation))
               (str "Expected exception: " variation))))

  (test/testing "General case, one variation"
    (let [variation {:id "v1" :subclass_type "SimpleAllele"}
          variation2 {:id "v1" :subclass_type "Haplotype"}
          variation3 {:id "v1" :subclass_type "Genotype"}]
      (test/is (sink/validate-variation-tree variation)
               (str "Expected validation success: " variation))
      (test/is (sink/validate-variation-tree variation2)
               (str "Expected validation success: " variation2))
      (test/is (sink/validate-variation-tree variation3)
               (str "Expected validation success: " variation3))))
  (test/testing "General case, two layers"
    (let [variation {:id "v1"
                     :subclass_type "Genotype"
                     :child_variations
                     [{:id "v2"
                       :subclass_type "Haplotype"}]}
          variation2 {:id "v1"
                      :subclass_type "Haplotype"
                      :child_variations
                      [{:id "v2"
                        :subclass_type "SimpleAllele"}]}]
      (test/is (sink/validate-variation-tree variation)
               (str "Expected validation success: " variation))
      (test/is (sink/validate-variation-tree variation2)
               (str "Expected validation success: " variation2))
      ))
  (test/testing "General case, three layers"
    (let [variation {:id "v1"
                     :subclass_type "Genotype"
                     :child_variations
                     [{:id "v2"
                       :subclass_type "Haplotype"}
                      {:id "v2"
                       :subclass_type "SimpleAllele"}]}
          variation2 {:id "v1"
                      :subclass_type "Haplotype"
                      :child_variations
                      [{:id "v2"
                        :subclass_type "SimpleAllele"}]}]
      (test/is (sink/validate-variation-tree variation)
               (str "Expected validation success: " variation))
      (test/is (sink/validate-variation-tree variation2)
               (str "Expected validation success: " variation2))))

  (test/testing "General case, three layers, multiple children"
    (let [variation {:id "v1"
                     :subclass_type "Genotype"
                     :child_variations
                     [{:id "v1-1"
                       :subclass_type "Haplotype"
                       :child_variations
                       [{:id "v1-1-1"
                         :subclass_type "SimpleAllele"}
                        {:id "v1-1-2"
                         :subclass_type "SimpleAllele"}
                        {:id "v1-1-3"
                         :subclass_type "SimpleAllele"}]}
                      {:id "v1-2"
                       :subclass_type "Haplotype"
                       :child_variations
                       [{:id "v1-2-1"
                         :subclass_type "SimpleAllele"}
                        {:id "v1-2-2"
                         :subclass_type "SimpleAllele"}]}]}
          ]
      (test/is (sink/validate-variation-tree variation)
               (str "Expected validation success: " variation))))

  (test/testing "Error case, invalid Genotype child"
    (let [variation {:id "v1"
                     :subclass_type "Genotype"
                     :child_variations
                     [{:id "v1-1"
                       :subclass_type "Genotype"}]}]
      (test/is (thrown? ExceptionInfo (sink/validate-variation-tree variation))
               (str "Expected exception: " variation))))
  (test/testing "Error case, invalid Haplotype child"
    (let [variation {:id "v1"
                     :subclass_type "Haplotype"
                     :child_variations
                     [{:id "v1-1"
                       :subclass_type "Genotype"}]}
          variation2 {:id "v1"
                      :subclass_type "Haplotype"
                      :child_variations
                      [{:id "v1-1"
                        :subclass_type "Haplotype"}]}]
      (test/is (thrown? ExceptionInfo (sink/validate-variation-tree variation))
               (str "Expected exception: " variation))
      (test/is (thrown? ExceptionInfo (sink/validate-variation-tree variation2))
               (str "Expected exception: " variation2))))
  (test/testing "Error case, invalid SimpleAllele child"
    (let [variation {:id "v1"
                      :subclass_type "SimpleAllele"
                      :child_variations
                      [{:id "v1-1"}]}]
      (test/is (thrown? ExceptionInfo (sink/validate-variation-tree variation))
               (str "Expected exception: " variation))))
  )
