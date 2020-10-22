(ns clinvar-qc.spec.gene
  (:require [clojure.spec.alpha :as spec]
            [clinvar-qc.spec.spec-asserts :as sa]))

(spec/def ::full_name not-empty)
(spec/def ::id sa/string-is-int?)
(spec/def ::symbol not-empty)

; Optional
(spec/def ::hgnc_id not-empty)

(spec/def ::gene
  (spec/keys :req-un [::full_name
                      ::id
                      ::symbol
                      ]
             :opt-un [::hgnc_id
                      ]))
