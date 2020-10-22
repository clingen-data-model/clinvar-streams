(ns clinvar-qc.spec.submission
  (:require [clojure.spec.alpha :as spec]
            [clinvar-qc.spec.spec-asserts :as sa]))

(spec/def ::additional_submitter_ids #(not (nil? %))) ; can be empty array
(spec/def ::id #(re-matches #"[0-9-.]+" %)) ; loose sanity validation of 3.2020-01-01, submitter_id.yyyy-mm-dd
(spec/def ::submission_date sa/string-is-yyyy-mm-dd?)
(spec/def ::submitter_id sa/string-is-int?)

(spec/def ::submission
  (spec/keys :req-un [::additional_submitter_ids
                      ::id
                      ::submission_date
                      ::submitter_id
                      ]))
