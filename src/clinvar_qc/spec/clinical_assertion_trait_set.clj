(ns clinvar-qc.spec.clinical-assertion-trait-set
  (:require [clojure.spec.alpha :as spec]))

(spec/def ::clinical_assertion_trait_ids not-empty)
(spec/def ::id #(re-matches #"SCV\d+" %))
(spec/def ::type not-empty)

; Optional
(spec/def ::content not-empty)

(spec/def ::clinical-assertion-trait-set
  (spec/keys :req-un [::clinical_assertion_trait_ids
                      ::id
                      ::type
                      ]
             :opt-un [::content
                      ]))
