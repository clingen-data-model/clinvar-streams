(ns clinvar-submissions.core-sepio
  (:require [clinvar-submissions.db :as db]
            [clinvar-submissions.core :as core
             :refer [key-functions rocksdb]]
            [jackdaw.streams :as j]
            [jackdaw.serdes :as j-serde]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:import [org.apache.kafka.streams KafkaStreams]
           [java.util Properties])
  (:gen-class))

(defn build-clinical-assertion
  ""
  [[^String key ^String val]]
  ; For the clinical assertion record val, combine all linked entities
  (let [clinical-assertion (json/parse-string val true)
        content (:content clinical-assertion)]
    (log/debug "original" val)
    (log/debug "building clinical assertion" (:id content))

    (let [; variation archive
          variation-archive-key ((:variation_archive key-functions) (:variation_archive_id content))
          variation-archive-msg (json/parse-string (db/get-key rocksdb variation-archive-key) true)
          variation-archive (:content variation-archive-msg)


          rcv-msg (if (not-empty (:rcv_accession_id content))
                (let [rcv-key ((:rcv_accession key-functions) (:rcv_accession_id content))]
                  (json/parse-string (db/get-key rocksdb rcv-key) true))
                (do (log/warn "clinical assertion" (:id content) "had no rcv_accession_id")
                    {}))
          rcv (:content rcv-msg)

          clinical-assertion-observation-keys (map #((:clinical_assertion_observation key-functions) %)
                                                    (:clinical_assertion_observation_ids content))
          clinical-assertion-observation-msgs (map #(json/parse-string (db/get-key rocksdb %) true)
                                                clinical-assertion-observation-keys)
          clinical-assertion-observations (map #(:content %) clinical-assertion-observation-msgs)

          ; clinical_assertion_variation
          ; only know scv id, not the clinical_assertion_variation id
          ; [id scv-id]
          clinical-assertion-variation-key-prefix ((:clinical_assertion_variation key-functions) "" (:id content))
          clinical-assertion-variation-msgs (map #(json/parse-string % true)
                                             (map #(second %) ; get-prefix returns [[k v] [k v]]
                                                  (db/get-prefix rocksdb clinical-assertion-variation-key-prefix)))
          clinical-assertion-variations (map #(:content %) clinical-assertion-variation-msgs)

          ; variation_id
          variation-msg (if (not-empty (:variation_id content))
                      (let [variation-key ((:variation key-functions) (:variation_id content))]
                        (json/parse-string (db/get-key rocksdb variation-key) true))
                      (do (log/warn "clinical assertion" (:id content) "has no variation_id")
                        {}))
          variation (:content variation-msg)

          ; submitter_id
          submitter-key ((:submitter key-functions) (:submitter_id content))
          submitter-msg (json/parse-string (db/get-key rocksdb submitter-key) true)
          submitter (:content submitter-msg)

          ; submission_id
          submission-key ((:submission key-functions) (:submission_id content))
          submission-msg (json/parse-string (db/get-key rocksdb submission-key) true)
          submission (:content submission-msg)

          ; TODO trait_set_id
          ; trait-set-key ((:trait_set key-functions) (:trait_set_id content))
          ; trait-set (json/parse-string (db/get-key rocksdb trait-set-key) true)
          trait-set-msg (if (not-empty (:trait_set_id content))
                      (let [trait-set-key ((:trait_set key-functions) (:trait_set_id content))]
                        (json/parse-string (db/get-key rocksdb trait-set-key) true))
                      {})
          trait-set (:content trait-set-msg)

          ; TODO trait
          trait-msgs (if (not-empty trait-set)
                   (let [trait-keys (map #((:trait key-functions) %) (:trait_ids trait-set))]
                     (map #(json/parse-string (db/get-key rocksdb %) true) trait-keys))
                   [])
          traits (map #(:content %) trait-msgs)

          ; TODO trait_mapping
          ; [scv-id trait-type mapping-type mapping-value mapping-ref medgen-id mapping-name]
          trait-mapping-msgs (map #(json/parse-string % true)
                              (map #(second %)
                                   (db/get-prefix rocksdb
                                                  ((:trait_mapping key-functions) (:id content) "" "" "" "" "" "")))) ; Only pass SCV id
          trait-mappings (map #(:content %) trait-mapping-msgs)

          ; TODO clinical_assertion_trait_set_id
          clinical-assertion-trait-set-key ((:clinical_assertion_trait_set key-functions) (:clinical_assertion_trait_set_id content))
          clinical-assertion-trait-set-msg (json/parse-string (db/get-key rocksdb clinical-assertion-trait-set-key) true)
          clinical-assertion-trait-set (:content clinical-assertion-trait-set-msg)

          ]

      (let [; Construct top level fields
            contributions (map (fn [submission]
                                 (-> {}
                                     (assoc :id (:id submission))
                                     (assoc :type "Contribution")
                                     (assoc :contributionMadeTo (str "ClinVar:" (:id content) "." (:version content)))
                                     (assoc :contributionMadeBy (str "ClinVarOrgId:" (:current_name submitter)))
                                     (assoc :contributorRole ["submission"])
                                     (assoc :startDate (:submission_date submission))
                                     (assoc :endDate (:date_last_updated content))
                                     ; rest of fields from submission
                                     (assoc :extensions (remove
                                                          #(contains? [:submission_date] %)
                                                          (keys submission)))))
                               [submission])
            ; TODO submitter + org
            agents (map (fn [s]
                          (-> {}
                              (assoc :id)
                              (assoc :type "Agent")
                              (assoc :label)
                              (assoc :qualifiedContribution)
                              ; rest of fields from submitter
                              (assoc :extensions)
                              ))
                        [submitter])
            conditions ([])
            scv-sepio (-> {}
                          (assoc :id (:id clinical-assertion))
                          (assoc :type ())
                          (assoc :subject ())
                          (assoc :subjectDescriptor ())
                          (assoc :object {})
                          (assoc :predicate (:interpretation_description content))
                          (assoc :description "")
                          (assoc :qualifierVariantOrigin "")
                          (assoc :qualifierInheritancePattern "")
                          (assoc :qualifiedContribution [])
                          (assoc :wasSpecifiedBy "")
                          (assoc :hasEvidenceFromSource [])
                          ; TODO rest of fields
                          (assoc :extensions {}))
            methods []
            vrs-objects []
            vrs-descriptors []

            ; Add top level fields to single map, and return
            out-rec (-> {}
                        (assoc :clinical_assertion scv-sepio)
                        (assoc :conditions conditions)
                        (assoc :contributions contributions)
                        (assoc :agents agents)
                        (assoc :methods methods)
                        (assoc :vrs-objects vrs-objects)
                        (assoc :vrs-descriptors vrs-descriptors)
                        )]
            out-rec)

      ;(let [updated-clinical-assertion
      ;      (-> clinical-assertion
      ;          ; Add fields to content, remove foreign key fields from content
      ;          (assoc-in [:content :variation_archive] variation-archive)
      ;          (update-in [:content] dissoc :variation_archive_id)
      ;
      ;          (assoc-in [:content :rcv] rcv)
      ;          (update-in [:content] dissoc :rcv_accession_id)
      ;
      ;          (assoc-in [:content :clinical_assertion_observations] clinical-assertion-observations)
      ;          (update-in [:content] dissoc :clinical_assertion_observation_ids)
      ;
      ;          (assoc-in [:content :variation] variation)
      ;          (update-in [:content] dissoc :variation_id)
      ;
      ;          (assoc-in [:content :submitter] submitter)
      ;          (update-in [:content] dissoc :submitter_id)
      ;
      ;          (assoc-in [:content :submission] submission)
      ;          (update-in [:content] dissoc :submission_id)
      ;
      ;          (assoc-in [:content :trait_set] trait-set)
      ;          (update-in [:content] dissoc :trait_set_id)
      ;
      ;          ; TODO overwrite trait_ids
      ;
      ;          (assoc-in [:content :trait_mappings] trait-mappings)
      ;
      ;          ; Reverse keyed to clinical assertion
      ;          (assoc-in [:content :clinical_assertion_variations] clinical-assertion-variations)
      ;          )
      ;      ]
      ;  (log/debug "updated" (json/generate-string updated-clinical-assertion))
      ;  ; return same key with string serialized updated clinical assertion
      ;  [key (json/generate-string updated-clinical-assertion)]
      ;  )
      ))
  )