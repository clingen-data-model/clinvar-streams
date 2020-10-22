(ns clinvar-qc.spec
  (:require [clojure.spec.alpha :as spec]
            [clinvar-qc.spec.clinical-assertion :as clinical-assertion]
            [clinvar-qc.spec.clinical-assertion-observation :as clinical-assertion-observation]
            [clinvar-qc.spec.clinical-assertion-trait :as clinical-assertion-trait]
            [clinvar-qc.spec.clinical-assertion-trait-set :as clinical-assertion-trait-set]
            [clinvar-qc.spec.clinical-assertion-variation :as clinical-assertion-variation]
            [clinvar-qc.spec.gene :as gene]
            [clinvar-qc.spec.gene-association :as gene-association]
            [clinvar-qc.spec.rcv-accession :as rcv-accession]
            [clinvar-qc.spec.submission :as submission]
            [clinvar-qc.spec.submitter :as submitter]
            [clinvar-qc.spec.trait :as trait]
            [clinvar-qc.spec.trait-mapping :as trait-mapping]
            [clinvar-qc.spec.trait-set :as trait-set]
            [clinvar-qc.spec.variation :as variation]
            [clinvar-qc.spec.variation-archive :as variation-archive]

            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [clojure.string :as s]
            ))

(defn validate [event]
  (let [content (:content event)
        entity-type (:entity_type content)
        spec-keyword (keyword (str "clinvar-qc.spec."
                                   (s/replace entity-type "_" "-"))
                              (s/replace entity-type "_" "-"))]
    (assert (not-empty event))
    (merge event
           (spec/explain-data spec-keyword content))))
