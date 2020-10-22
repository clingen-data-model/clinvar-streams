(ns clinvar-qc.spec.gene-association
  (:require [clojure.spec.alpha :as spec]
            [clinvar-qc.spec.spec-asserts :as sa]
            [clinvar-qc.util :refer :all]))

(spec/def ::gene_id sa/string-is-int?)
(spec/def ::relationship_type not-empty)
(spec/def ::source #(in? ["calculated" "submitted"] %))
(spec/def ::variation_id sa/string-is-int?)

; Optional
(spec/def ::content not-empty)

(spec/def ::gene-association
  (spec/keys :req-un [::gene_id
                      ::relationship_type
                      ::source
                      ::variation_id
                      ]
             :opt-un [::content ; Marked optional, but none are empty
                      ]))
