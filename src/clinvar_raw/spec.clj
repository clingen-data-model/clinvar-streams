(ns clinvar-raw.spec
  (:require [clojure.spec.alpha :as spec]
            [clinvar-raw.spec.clinical-assertion :as clinical-assertion]
            [clinvar-raw.spec.clinical-assertion-observation :as clinical-assertion-observation]
            [clinvar-raw.spec.clinical-assertion-trait :as clinical-assertion-trait]
            [clinvar-raw.spec.clinical-assertion-trait-set :as clinical-assertion-trait-set]
            [clinvar-raw.spec.clinical-assertion-variation :as clinical-assertion-variation]
            [clinvar-raw.spec.gene :as gene]
            [clinvar-raw.spec.gene-association :as gene-association]
            [clinvar-raw.spec.rcv-accession :as rcv-accession]
            [clinvar-raw.spec.submission :as submission]
            [clinvar-raw.spec.submitter :as submitter]
            [clinvar-raw.spec.trait :as trait]
            [clinvar-raw.spec.trait-mapping :as trait-mapping]
            [clinvar-raw.spec.trait-set :as trait-set]
            [clinvar-raw.spec.variation :as variation]
            [clinvar-raw.spec.variation-archive :as variation-archive]

            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [clojure.string :as s]
            ))

(defn validate [event]
  (let [entity-type (-> event :data :content :entity_type)]
    (merge event
           (spec/explain-data (keyword (str "clinvar-raw.spec." (s/replace entity-type "_" "-"))  (s/replace entity-type "_" "-"))
                              (-> event :data :content)))))
