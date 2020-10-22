(ns clinvar-raw.spec.rcv-accession
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.spec-asserts :as sa]))

(spec/def ::date_last_evaluated sa/string-is-yyyy-mm-dd?)
(spec/def ::id #(re-matches #"RCV\d+" %))
(spec/def ::interpretation not-empty) ; TODO This is a bit more structured, enumeration
(spec/def ::review_status not-empty) ; TODO This looks like an enumeration
(spec/def ::submission_count sa/string-is-int?)
(spec/def ::title not-empty) ; TODO This is fairly structured
(spec/def ::variation_archive_id #(re-matches #"VCV\d+" %))
(spec/def ::variation_id sa/string-is-int?)
(spec/def ::version sa/string-is-int?)

; Optional
(spec/def ::content not-empty)
(spec/def ::independent_observations sa/string-is-int?)
(spec/def ::trait_set_id not-empty)

(spec/def ::rcv-accession
  (spec/keys :req-un [::date_last_evaluated ; TODO not always present
                      ::id
                      ::interpretation
                      ::review_status
                      ::submission_count
                      ::title
                      ::variation_archive_id
                      ::variation_id
                      ::version
                      ::trait_set_id ; 0.0016% null
                      ]
             :opt-un [::content
                      ::independent_observations
                      ]))
