(ns clinvar-raw.spec.submitter
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.spec-asserts :as sa]))

(spec/def ::all_abbrevs #(not (nil? %))) ; can be empty array
(spec/def ::all_names not-empty) ; array must have at least one
(spec/def ::current_name not-empty)
(spec/def ::id sa/string-is-int?)
(spec/def ::org_category not-empty)

; Optional
(spec/def ::current_abbrev not-empty)

(spec/def ::submitter
  (spec/keys :req-un [::all_abbrevs
                      ::all_names
                      ::current_name
                      ::id
                      ::org_category
                      ]
             :opt-un [::current_abbrev
                      ]))
