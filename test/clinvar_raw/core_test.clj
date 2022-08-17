(ns clinvar-raw.core_test
  (:require [clojure.test :refer :all]
            [clojure.string :as s]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [clojure.data :refer [diff]]
            [clojure.core.async :as async :refer [>!! <!! poll!]]
            [clinvar-raw.core :as core]
            [clinvar-raw.stream :as stream]
            [clinvar-streams.util :as util]))

;(deftest bucket-test
;  (testing "Ensure bucket is set to correct value"
;    (is (= "broad-dsp-monster-clingen-dev-ingest-results" core/bucket))))

;(deftest validate-datetime
;  (testing "Validate datetime format"
;    ; Replace with regex validating of message datetime field?
;    (is (= 1 1))))

(def release-date "2020-04-01")

(def drop-file-records
  {:gene
   {:processed-clinvar-drop
    [{:key (str "gene_107984974_" release-date)
      :value
      {:release_date release-date
       :event_type "created"
       :content (assoc
                 (json/parse-string (slurp "test/clinvar_raw/resources/drop_files/created/good/gene.json") true)
                 :entity_type "gene"
                 :clingen_version 0)}}]}
   :variation ""
   :gene_association ""
   :variation_archive ""
   :trait ""
   :trait_set ""
   :rcv_accession ""
   :submitter ""
   :submission ""
   :clinical_assertion ""
   :clinical_assertion_variation ""
   :clinical_assertion_trait ""
   :clinical_assertion_trait_set ""
   :clinical_assertion_observation ""
   :trait_mapping ""})

(defn get-drop-file-records [entity-type]
  (let [contents (slurp (str "test/clinvar_raw/resources/drop_files/created/good/" (str entity-type ".json")))
        lines (s/split-lines contents)]
    (filter #(< 0 (.length %)) lines)))

(def clinical-assertion-event {:key "clinical_assertion_SCV000924344_2020-01-01T12:00:00Z",
                               :data
                               {:release_date "2020-01-01T12:00:00Z",
                                :type "create",
                                :content
                                {:variation_id "634922",
                                 :variation_archive_id "VCV000634922",
                                 :submitter_id "3",
                                 :date_last_updated "2019-06-24",
                                 :interpretation_comments [],
                                 :interpretation_description "risk factor",
                                 :trait_set_id "47782",
                                 :internal_id "1807657",
                                 :type "clinical_assertion",
                                 :submission_id "3.2019-06-18",
                                 :local_key
                                 "114350.0001_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9",
                                 :clinical_assertion_observation_ids ["SCV000924344.0"],
                                 :title
                                 "NUP214, ASP154GLY_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9",
                                 :assertion_type "variation to disease",
                                 :rcv_accession_id "RCV000785779",
                                 :clinical_assertion_trait_set_id "SCV000924344",
                                 :id "SCV000924344",
                                 :submission_names [],
                                 :record_status "current",
                                 :date_created "2019-06-20",
                                 :review_status "no assertion criteria provided",
                                 :interpretation_date_last_evaluated "2019-06-18",
                                 :version "1"}}})

; TODO Update to use all entity-types when spec in clinvar-raw.core is complete
(deftest test-line-to-event
  (testing "Test processing line-to-event for clinical_assertion"
    (let [line (first (get-drop-file-records "clinical_assertion"))
          entity-type "clinical_assertion"
          datetime "2020-01-01T12:00:00Z"
          event-type "create"
          ;expected-value {:key "SCV000924344_2020-01-01T12:00:00Z", :value "{\"time\":\"2020-01-01T12:00:00Z\",\"type\":\"create\",\"content\":{\"variation_id\":\"634922\",\"variation_archive_id\":\"VCV000634922\",\"submitter_id\":\"3\",\"date_last_updated\":\"2019-06-24\",\"interpretation_comments\":[],\"interpretation_description\":\"risk factor\",\"trait_set_id\":\"47782\",\"internal_id\":\"1807657\",\"type\":\"clinical_assertion\",\"submission_id\":\"3.2019-06-18\",\"local_key\":\"114350.0001_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"clinical_assertion_observation_ids\":[\"SCV000924344.0\"],\"title\":\"NUP214, ASP154GLY_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9\",\"assertion_type\":\"variation to disease\",\"rcv_accession_id\":\"RCV000785779\",\"clinical_assertion_trait_set_id\":\"SCV000924344\",\"id\":\"SCV000924344\",\"submission_names\":[],\"record_status\":\"current\",\"date_created\":\"2019-06-20\",\"review_status\":\"no assertion criteria provided\",\"interpretation_date_last_evaluated\":\"2019-06-18\",\"version\":\"1\"}}"}]
          expected-value {:key "clinical_assertion_SCV000924344_2020-01-01T12:00:00Z",
                          :value {:release_date "2020-01-01T12:00:00Z",
                                  :event_type "create",
                                  :content {:clingen_version 0
                                            :variation_id "634922",
                                            :variation_archive_id "VCV000634922",
                                            :submitter_id "3",
                                            :date_last_updated "2019-06-24",
                                            :interpretation_comments [],
                                            :interpretation_description "risk factor",
                                            :trait_set_id "47782",
                                            :internal_id "1807657",
                                            :entity_type "clinical_assertion",
                                            :submission_id "3.2019-06-18",
                                            :local_key "114350.0001_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9",
                                            :clinical_assertion_observation_ids ["SCV000924344.0"],
                                            :title "NUP214, ASP154GLY_ENCEPHALOPATHY, ACUTE, INFECTION-INDUCED, SUSCEPTIBILITY TO, 9",
                                            :assertion_type "variation to disease",
                                            :rcv_accession_id "RCV000785779",
                                            :clinical_assertion_trait_set_id "SCV000924344",
                                            :id "SCV000924344",
                                            :submission_names [],
                                            :record_status "current",
                                            :date_created "2019-06-20",
                                            :review_status "no assertion criteria provided",
                                            :interpretation_date_last_evaluated "2019-06-18",
                                            :version "1"}}}]
      (let [actual-value (stream/line-map-to-event (json/parse-string line true) entity-type datetime event-type)]
        (is (= expected-value actual-value)
            (str "Expected did not match actual: "
                 (into [] (diff expected-value actual-value))))))))

(deftest test-filter-files
  (let [entity-types (map #(name %) (keys drop-file-records))
        file-list (map #(str "2020-04-01/" (name %) "/created/00000000") entity-types)]
    (testing "Test filtering file list based on entity-types"
      (doseq [entity-type entity-types]
        (let [filtered (stream/filter-files entity-type file-list)
              path-seg (str "/" entity-type "/")]
          (is (util/match-every? path-seg filtered)
              (str "All entries should contain " path-seg))
          (is (= 1 (count filtered))
              "Filtered list should have only 1 element"))))
    (testing "Testing filter-files on non-existent entity-types"
      (is (= [] (stream/filter-files "fake-entity" file-list))))
    (testing "Testing filter-files on other path segments"
      (is (= [] (stream/filter-files "2020-04-01" file-list)))
      (is (util/unordered-eq? file-list (stream/filter-files "created" file-list)))
      (is (= [] (stream/filter-files "00000000" file-list))))))

#_(deftest test-process-clinvar-drop-file
    (testing "Testing filter-files on non-existent entity-types"
    ; Returns line-to-event for each line in drop file
      (let [;entity-types (map #(name %) (keys drop-file-records))
            entity-types ["gene"]]
      ; For each file, open a reader and run process-clinvar-drop-file on it
      ; check return seq value literals
        (doseq [entity-type entity-types]
          (with-open [r (io/reader (str "test/clinvar_raw/resources/drop_files/created/good/" entity-type ".json"))]
            (let [expected-value (:processed-clinvar-drop ((keyword entity-type) drop-file-records))]
              (stream/process-clinvar-drop-file {:reader r
                                                 :entity-type entity-type
                                                 :release_date release-date
                                                 :event-type "created"
                                                 :filter-field nil})
              (Thread/sleep 100)                              ; TODO put spin
              (let [actual-value (<!! (async/into [] (async/take (count expected-value) stream/producer-channel)))]
                (println actual-value)
                (is (= expected-value actual-value)
                    (str "Expected did not match actual: "
                         (into [] (diff expected-value actual-value)))))))))))
