(ns clinvar-streams.util-test
  (:require [clinvar-streams.util :refer [select-keys-nested]]
            [clojure.test :as t :refer :all]))

(deftest test-select-keys-nested
  (testing "Testing trivial cases"
    (let [m {:a 1 :b 2 :c 3}]
      (is (= (select-keys nil [:a])
             (select-keys-nested nil [:a])))
      (is (= (select-keys {} nil)
             (select-keys-nested {} nil)))
      (is (= (select-keys m nil)
             (select-keys-nested m nil)))
      (is (= (select-keys m [])
             (select-keys-nested m [])))
      (is (= (select-keys m [nil])
             (select-keys-nested m [nil])))
      (is (= (select-keys m [:b])
             (select-keys-nested m [:b])))
      (is (= (select-keys m [:a])
             (select-keys-nested m [:a])))))
  (testing "Testing nested cases"
    (let [m {:a 1 :b {:c {:d {:e 2} :f 3} :g 4}}]
      (is (= {:a 1}
             (select-keys-nested m [:a])))
      (is (= {:a 1 :b {:c {:d {:e 2}}}}
             (select-keys-nested m [:a [:b :c :d :e]])))
      ;; Selecting d gives the same as going all the way to e
      (is (= {:a 1 :b {:c {:d {:e 2}}}}
             (select-keys-nested m [:a [:b :c :d]]))))))
