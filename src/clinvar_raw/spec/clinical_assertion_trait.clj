(ns clinvar-raw.spec.clinical-assertion-trait
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.spec-asserts :as sa]))

; Required
(spec/def ::alternate_names #(not (nil? %))) ; can be empty array
(spec/def ::id sa/scv-number-versioned?)
(spec/def ::type not-empty)
(spec/def ::xrefs #(not (nil? %)))

; Optional
(spec/def ::content not-empty)
(spec/def ::medgen_id not-empty)
(spec/def ::name not-empty)
(spec/def ::trait_id not-empty)

(spec/def ::clinical-assertion-trait
  (spec/keys :req-un [::alternate_names
                      ::id
                      ::type
                      ::xrefs]
             :opt-un [::content
                      ::medgen_id
                      ::name
                      ; TODO 2.72% null
                      ::trait_id
                      ]))
