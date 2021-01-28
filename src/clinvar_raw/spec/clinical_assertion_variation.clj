(ns clinvar-raw.spec.clinical-assertion-variation
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.spec-asserts :as sa]
            [clinvar-streams.util :refer :all]))

(spec/def ::child_ids #(not (nil? %))) ; can be empty array
(spec/def ::clinical_assertion_id sa/scv-number?)
(spec/def ::descendant_ids #(not (nil? %))) ; can be empty array
(spec/def ::id sa/scv-number-versioned?)
(spec/def ::subclass_type #(in? % ["SimpleAllele" "Genotype" "Haplotype"]))

; Optional
(spec/def ::content not-empty)
(spec/def ::variation_type not-empty)

(spec/def ::clinical-assertion-variation
  (spec/keys :req-un [::child_ids
                      ::clinical_assertion_id
                      ::descendant_ids
                      ::id
                      ::subclass_type ;For SimpleAllele, no child_ids or descendant_ids, for Genotype/Haplotype, must have child+descendant
                      ]
             :opt-un [::content
                      ; TODO 0.025% null
                      ::variation_type
                      ]))
