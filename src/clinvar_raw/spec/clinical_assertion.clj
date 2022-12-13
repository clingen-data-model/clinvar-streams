(ns clinvar-raw.spec.clinical-assertion
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.spec-asserts :as sa]))

;; Required
(spec/def ::assertion_type not-empty)   ; required string
(spec/def ::clinical_assertion_observation_ids not-empty) ; non-empty array
(spec/def ::clinical_assertion_trait_set_id not-empty) ; non-empty string
(spec/def ::date_created sa/string-is-yyyy-mm-dd?) ; required yyyy-mm-dd
(spec/def ::date_last_updated sa/string-is-yyyy-mm-dd?)
(spec/def ::id #(re-matches #"SCV\d{9}" %))
(spec/def ::internal_id sa/string-is-int?)
(spec/def ::interpretation_date_last_evaluated sa/string-is-yyyy-mm-dd?) ; required yyyy-mm-dd
;; TODO some records are missing RCV
(spec/def ::rcv_accession_id #(re-matches #"RCV\d{9}" %))
(spec/def ::record_status not-empty)
(spec/def ::review_status not-empty)
(spec/def ::submission_id not-empty)
(spec/def ::submission_names #(not (nil? %))) ; can be empty array
(spec/def ::submitter_id sa/string-is-int?)
;; TODO some records missing trait_set_id
(spec/def ::trait_set_id sa/string-is-int?)
(spec/def ::variation_archive_id #(re-matches #"VCV\d{9}" %))
(spec/def ::variation_id sa/string-is-int?)
;; (spec/def ::version #(and (integer? %) (< 0 %))) ; required int
(spec/def ::version #(and (sa/string-is-int? %) (< 0 (Integer/parseInt %)))) ; positive int string

;; Optional
(spec/def ::content not-empty) ; optional
(spec/def ::interpretation_comments #(not (nil? %))) ; can be empty array
(spec/def ::interpretation_description not-empty) ; optional
(spec/def ::local_key not-empty) ; optional, if present, must be nonempty string
(spec/def ::submitted_assembly not-empty)
(spec/def ::title not-empty)

(spec/def ::clinical-assertion
  (spec/keys :req-un [::assertion_type  ; required string
                      ::clinical_assertion_observation_ids
                      ::clinical_assertion_trait_set_id
                      ::date_created      ; required yyyy-mm-dd
                      ::date_last_updated ; required yyyy-mm-dd
                      ::id                ; SCV\d{10}
                      ::internal_id       ; required int
                      ::interpretation_date_last_evaluated ; required yyyy-mm-dd
                      ::local_key                          ; optional ?
                      ::rcv_accession_id                   ; RCV\d{9}
                      ::record_status
                      ::review_status
                      ::submission_id   ; required
                      ::submission_names
                      ::submitter_id         ; required int
                      ::trait_set_id         ; required int
                      ::variation_archive_id ; VCV\d{9}
                      ::variation_id         ; required int
                      ::version]             ; required int
             :opt-un [::content                    ; optional string
                      ::interpretation_comments    ; optional string
                      ::interpretation_description ; optional string
                      ::submitted_assembly
                      ::title]))
