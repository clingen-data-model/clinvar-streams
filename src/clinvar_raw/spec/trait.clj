(ns clinvar-raw.spec.trait
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.spec-asserts :as sa]))

(spec/def ::alternate_names #(not (nil? %))) ; can be empty array
(spec/def ::alternate_symbols #(not (nil? %))) ; can be empty array
(spec/def ::attribute_content #(not (nil? %))) ; can be empty array
(spec/def ::id sa/string-is-int?)
(spec/def ::keywords #(not (nil? %))) ; can be empty array
(spec/def ::type not-empty) ; TODO this is an enumeration
(spec/def ::xrefs #(not (nil? %))) ; can be empty array

; Optional
(spec/def ::content not-empty)
(spec/def ::disease_mechanism not-empty)
(spec/def ::disease_mechanism_id sa/string-is-int?)
(spec/def ::gard_id sa/string-is-int?)
(spec/def ::gene_reviews_short not-empty)
(spec/def ::ghr_links not-empty)
(spec/def ::medgen_id not-empty)
(spec/def ::mode_of_inheritance not-empty)
(spec/def ::name not-empty)
(spec/def ::public_definition not-empty)
(spec/def ::symbol not-empty)

(spec/def ::trait
  (spec/keys :req-un [::alternate_names
                      ::alternate_symbols
                      ::attribute_content
                      ::id
                      ::keywords
                      ::type
                      ::xrefs
                      ]
             :opt-un [::content
                      ::disease_mechanism
                      ::disease_mechanism_id
                      ::gard_id
                      ::gene_reviews_short
                      ::ghr_links
                      ::medgen_id
                      ::mode_of_inheritance
                      ::name ; TODO 0.017% null
                      ::public_definition
                      ::symbol
                      ]))
