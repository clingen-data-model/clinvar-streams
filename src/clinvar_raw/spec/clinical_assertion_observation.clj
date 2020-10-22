(ns clinvar-raw.spec.clinical-assertion-observation
  (:require [clojure.spec.alpha :as spec]))

; TODO 93.6% null
;(spec/def ::clinical_assertion_trait_id)
(spec/def ::content not-empty)
(spec/def ::id #(re-matches #"SCV[\d.]+" %))

; Optional
(spec/def ::clinical_assertion_trait_set_id not-empty)

(spec/def ::clinical-assertion-observation
  (spec/keys :req-un [::content
                      ::id]
             :opt-un [::clinical_assertion_trait_set_id
                      ]))
