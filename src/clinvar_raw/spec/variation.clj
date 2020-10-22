(ns clinvar-raw.spec.variation
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.spec-asserts :as sa]
            [clinvar-raw.util :refer :all]))


(spec/def ::id sa/string-is-int?)
(spec/def ::name not-empty)
(spec/def ::protein_change #(not (nil? %))) ; can be empty array
(spec/def ::subclass_type #(in? % ["SimpleAllele" "Genotype" "Haplotype"]))
(spec/def ::variation_type not-empty)

; Optional
(spec/def ::allele_id not-empty)
(spec/def ::child_ids #(not (nil? %))) ; can be empty array
(spec/def ::content not-empty)
(spec/def ::descendant_ids #(not (nil? %))) ; can be empty array
(spec/def ::num_chromosomes sa/string-is-int?)
(spec/def ::num_copies sa/string-is-int?)

(spec/def ::variation
  (spec/keys :req-un [::id
                      ::name
                      ::protein_change
                      ::subclass_type ;For SimpleAllele, no child_ids or descendant_ids, for Genotype/Haplotype, must have child+descendant
                      ::variation_type
                      ]
             :opt-un [::allele_id ; TODO 0.0864% null (this is okay)
                      ::child_ids
                      ::content
                      ::descendant_ids
                      ::num_chromosomes
                      ::num_copies
                      ]))
