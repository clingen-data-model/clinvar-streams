(ns clinvar-qc.spec.trait-mapping
  (:require [clojure.spec.alpha :as spec]
            [clinvar-qc.spec.spec-asserts :as sa]
            [clinvar-qc.util :refer :all]))

(spec/def ::clinical_assertion_id sa/scv-number?)
(spec/def ::mapping_ref not-empty) ; TODO Looks like an enumeration
(spec/def ::mapping_type #(in? ["XRef" "Name"] %))
(spec/def ::mapping_value not-empty)
(spec/def ::medgen_name not-empty)
(spec/def ::trait_type not-empty) ; TODO looks like an enumeration

; Optional
(spec/def ::medgen_id not-empty)

(spec/def ::trait-mapping
  (spec/keys :req-un [::clinical_assertion_id
                      ::mapping_ref
                      ::mapping_type
                      ::mapping_value
                      ::medgen_name
                      ::trait_type
                      ]
             :opt-un [::medgen_id ; TODO 5.37% null
                      ]))
