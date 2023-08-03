(ns clinvar-raw.single-release
  "Under clinvar-raw, there are two modes of processing:
    clinvar-raw:
        where clinvar-raw waits for DSP release messages on an input stream
        processes all of the files listed in that message and outputs raw data
        to an output stream then goes back to listening for the next message
        on the input DSP notification topic.
    single-release:
        the concept of single release is that Larry has the ability
        to create a single starting point release of files in a GCPbucket.
        The files are all \"created\" files for all of the ClinVar records
        as of a single date. This one time processing of the files found in the
        GCPBucket (no input topic) will write the single release to the output topic
        configured for the process."
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as config]
            [clinvar-raw.stream :as stream]
            [clinvar-streams.config :as streams-config]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as spec]
            [clojure.walk :as walk]
            [jackdaw.client :as jc]
            [taoensso.timbre :as log]))

;; spec validation of command line interface options
(def sample-opts {:DX_CV_RAW_OUTPUT_TOPIC "terry-test"
                  :STORAGE_PROTOCOL "gs://"
                  :RELEASE_DATE "2022-02-08"
                  :RELEASE_GCP_BUCKET "clinvar-releases"
                  :RELEASE_DIRECTORY "2023_02_08"})
(spec/def :cli-opts/RELEASE_DATE string?)
(spec/def :cli-opts/RELEASE_GCP_BUCKET string?)
(spec/def :cli-opts/RELEASE_DIRECTORY string?)
(spec/def :cli-opts/DX_CV_RAW_OUTPUT_TOPIC string?)
(spec/def :cli-opts/STORAGE_PROTOCOL string?)
(spec/def :cli-opts/opts (spec/keys :req-un [:cli-opts/DX_CV_RAW_OUTPUT_TOPIC
                                             :cli-opts/STORAGE_PROTOCOL
                                             :cli-opts/RELEASE_DATE
                                             :cli-opts/RELEASE_GCP_BUCKET
                                             :cli-opts/RELEASE_DIRECTORY]))

(def env-config
  (merge streams-config/env-config
         (-> (System/getenv)
             (select-keys
              (map name [:DX_CV_RAW_OUTPUT_TOPIC
                         :STORAGE_PROTOCOL
                         :RELEASE_DATE
                         :RELEASE_GCP_BUCKET
                         :RELEASE_DIRECTORY]))
             walk/keywordize-keys)))

(defn start-single-release
  "One time processing of a single, specifically formated message that
  specifies a gcp bucket and specific release directory containing a set of release files to
  process once, and output to an output topic that has been created by hand."
  [opts kafka-opts]
  (when (not (spec/valid? :cli-opts/opts opts))
    (throw (ex-info "opts not valid"
                    {:spec/explain (spec/explain :cli-opts/opts opts)})))
  (let [output-topic (:DX_CV_RAW_OUTPUT_TOPIC opts)
        release-info {:storage_protocol (:STORAGE_PROTOCOL opts)
                      :release_date (:RELEASE_DATE opts)
                      :bucket (:RELEASE_GCP_BUCKET opts)
                      :release_directory (:RELEASE_DIRECTORY opts)}]
    (with-open [producer (jc/producer kafka-opts)]
      (doseq [output-message
              ;; process-clinvar-drop returns [{:key ... :value ...}]
              (->> (stream/process-clinvar-drop release-info))]
        (assert (map? output-message) {:msg "Expected map" :output-message output-message})
        (stream/send-update-to-exchange producer output-topic output-message)))))

(defn cli-mode []
  (let [opts (-> env-config
                 (assoc
                  :DX_CV_RAW_OUTPUT_TOPIC "terry-test"
                  :STORAGE_PROTOCOL "gs://"
                  :RELEASE_DATE "2023-04-24"
                  :RELEASE_DIRECTORY "2023-04-24/vcep_vars"
                  :RELEASE_GCP_BUCKET "clinvar-releases"))
        kafka-config (-> (config/kafka-config opts)
                         (assoc "group.id" "local-dev"))]
    (log/info :msg "*****KAFKA-CONFIG" :kafka-config kafka-config)
    (log/info :msg "****CONFIG" :env-config opts :system-getenv (System/getenv))
    (start-single-release opts kafka-config)))

(defn start-with-env []
  (let [opts env-config
        kafka-opts (-> (config/kafka-config opts)
                       (dissoc "group.id"))]
    (start-single-release opts kafka-opts)))

(defn re-run []
  (start-single-release {:DX_CV_RAW_OUTPUT_TOPIC "clinvar-raw-2023-04-10-rerun"
                         :STORAGE_PROTOCOL "gs://"
                         :RELEASE_DATE "2023-04-10"
                         :RELEASE_GCP_BUCKET "clinvar-releases"
                         :RELEASE_DIRECTORY "2023-04-10"}
                        (dissoc (config/kafka-config config/env-config)
                                "group.id")))

(comment
  (->> (stream/process-clinvar-drop {:release_date "2023-04-24"
                                     :bucket "clinvar-releases"
                                     :release_directory "2023-04-24/vcep_vars"
                                     :storage_protocol "gs://"})
       (take 3))

  (start-single-release {:DX_CV_RAW_OUTPUT_TOPIC "kyle-test"
                         :STORAGE_PROTOCOL "gs://"
                         :RELEASE_DATE "2023-04-10"
                         :RELEASE_GCP_BUCKET "clinvar-releases"
                         :RELEASE_DIRECTORY "2023-04-10"}
                        (dissoc (config/kafka-config config/env-config)
                                "group.id"))

  ;; on cloud vm


  ())
