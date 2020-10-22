(ns clinvar-qc.spec.variation-archive
  (:require [clojure.spec.alpha :as spec]
            [clinvar-qc.spec.spec-asserts :as sa]))

(spec/def ::date_created sa/string-is-yyyy-mm-dd?)
(spec/def ::date_last_updated sa/string-is-yyyy-mm-dd?)
(spec/def ::id #(re-matches #"VCV\d+" %))
(spec/def ::interp_content not-empty)
(spec/def ::interp_description not-empty)
(spec/def ::interp_type not-empty) ; TODO seems like an enum. 1 value "Clinical significance"
(spec/def ::num_submissions sa/string-is-int?)
(spec/def ::num_submitters sa/string-is-int?)
(spec/def ::record_status not-empty) ; TODO seems like an enum
(spec/def ::review_status not-empty) ; TODO seems like an enum
(spec/def ::species not-empty)
(spec/def ::variation_id sa/string-is-int?)
(spec/def ::version sa/string-is-int?)

; Optional
(spec/def ::content not-empty)
(spec/def ::interp_date_last_evaluated not-empty)
(spec/def ::interp_explanation not-empty)

(spec/def ::variation-archive
  (spec/keys :req-un [::date_created
                      ::date_last_updated
                      ::id
                      ::interp_content
                      ::interp_description
                      ::interp_type
                      ::num_submissions
                      ::num_submitters
                      ::record_status
                      ::review_status
                      ::species
                      ::variation_id
                      ::version
                      ]
             :opt-un [::content
                      ::interp_date_last_evaluated ; TODO 5.7% null
                      ::interp_explanation
                      ]))
