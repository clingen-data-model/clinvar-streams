(ns clinvar-raw.core
  (:require [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [jackdaw.client.log :as jl]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [cheshire.core :as json]
            [clojure.string :as s]
            [clojure.core :as core]
            [clojure.spec.alpha :as spec]
            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]]
            [clojure.core.async :as async :refer [>!! <!!]]
            [clinvar-raw.config :as cfg]
            [clinvar-raw.util :as util]
            )
  (:import [com.google.cloud.storage Storage StorageOptions BlobId Blob]
           com.google.cloud.storage.Blob$BlobSourceOption
           java.nio.channels.Channels
           java.io.BufferedReader
           (java.util Date TimeZone Map)
           (java.text SimpleDateFormat)
           (java.lang Thread))
  (:gen-class))

(def messages-to-consume (atom []))

(def order-of-processing [{:type "gene"}
                          {:type "variation" :filter {:field :subclass_type :value "SimpleAllele"}}
                          {:type "variation" :filter {:field :subclass_type :value "Haplotype"}}
                          {:type "variation" :filter {:field :subclass_type :value "Genotype"}}
                          {:type "gene_association"}
                          {:type "trait"}
                          {:type "trait_set"}
                          {:type "submitter"}
                          {:type "submission"}
                          {:type "clinical_assertion_trait"}
                          {:type "clinical_assertion_trait_set"}
                          {:type "clinical_assertion_observation"}
                          {:type "clinical_assertion"}
                          {:type "clinical_assertion_variation" :filter {:field :subclass_type :value "SimpleAllele"}}
                          {:type "clinical_assertion_variation" :filter {:field :subclass_type :value "Haplotype"}}
                          {:type "clinical_assertion_variation" :filter {:field :subclass_type :value "Genotype"}}
                          {:type "trait_mapping"}
                          {:type "rcv_accession"}
                          {:type "variation_archive"}
                          ])


(def delete-order-of-processing (reverse order-of-processing))

(def event-procedures [{:event-type :create :order order-of-processing :filter-string "created"}
                       {:event-type :update :order order-of-processing :filter-string "updated"}
                       {:event-type :delete :order delete-order-of-processing :filter-string "deleted"}])

(def gc-storage (.getService (StorageOptions/getDefaultInstance)))

(def send-update-to-exchange-counter (atom (bigint 0)))

(defn send-update-to-exchange
  "Sends a message to the producer on `topic`, with the message key `key`, and payload `data`
  `data` can be a string or json-serializable object like a map"
  [producer topic {:keys [key value]}]

  (debugf "Sending message with key %s to topic %s" key topic)
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic}
                                          key
                                          (if (string? value) value (json/generate-string value))))
  (swap! send-update-to-exchange-counter inc))

(defn google-storage-line-reader [bucket filename]
  "Returns a reader to the storage object. Caller must open (with-open)"
  (debugf "Opening gs://%s/%s" bucket filename)
  (let [blob-id (BlobId/of bucket filename)
        blob (.get gc-storage blob-id)]
    (debugf "Obtaining reader to blob %s/%s" bucket filename)
    (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.)))

(defn line-map-to-event [line-map entity-type release_date event-type]
  "Parses a single line of a drop file, transforms into an event object map"
  (let [content (assoc line-map :entity_type entity-type)
        key (str entity-type "_" (:id content) "_" release_date)
        event {:release_date release_date :type event-type :content content}]
    {:key key :value event}))

(defn process-drop-record
  "Process a single line from a dsp clinvar drop file"
  [record entity-type datetime event-type]
  (-> record
      (line-map-to-event entity-type datetime event-type)))

(def producer-channel (async/chan 1000))

(defn create-sentinel-message
  "sentinel-type should be one of :start, :end"
  [release-tag sentinel-type]
  (assert (util/in? sentinel-type [:start :end]))
  {:key   (str "release_sentinel_" release-tag)
   :value {:type          "release_sentinel"
           :sentinel_type (name sentinel-type)
           :release_date  release-tag
           :rules         []
           :source        "clinvar"
           :reason        (str "ClinVar Upstream Release " release-tag),
           :notes         nil                               ; TODO
           ; notes could point to ClinVar release notes, or a clingen release notes page
           }})

(defn process-clinvar-drop-file
  "Return a seq of parsed json messages"
  [{:keys [reader entity-type release_date event-type file-read-limit filter-field]
    :or   {file-read-limit ##Inf}}]
  (debugf "Processing first %s lines of dropped file for entity-type %s and event-type %s"
          file-read-limit entity-type event-type)
  (with-open [open-reader reader]
    ; For each line, parse, convert to event object, attach validation fields
    (doseq [line (take file-read-limit (line-seq open-reader))]
      (let [record (json/parse-string line true)]
        (if (or (nil? filter-field) (= (get record (:field filter-field)) (:value filter-field)))
          (>!! producer-channel (process-drop-record record entity-type release_date event-type))
          )))))

(defn filter-files
  "Filters a collection of file strings containing a path segment which matches `filter-string`"
  [filter-string files]
  (filter #(re-find (re-pattern (str "/" filter-string "/")) %) files))

(defn construct-reader
  "Attempts to construct a reader to a sequence of file URL fragments.
  Example: (construct-reader /home foo bar) will return a reader to '/home/foo/bar'
           (construct-reader gs:// bucket dir file) will return a reader to gs://bucket/dir/file"
  [protocol & path-segs]
  (cond (= "gs://" protocol) (apply google-storage-line-reader path-segs)
        (= "file://" protocol) (io/reader (apply io/file path-segs))
        :default (io/reader (apply io/file path-segs))
        ))

(defn process-clinvar-drop
  "Parses and processes the clinvar drop notification from dsp, returns a seq of output messages"
  [msg & {:keys [storage-protocol]
          :or   {storage-protocol "gs://"}}]
  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest
  (debugf "File listing message: %s" msg)
  (let [parsed-drop-record (if (string? msg) (json/parse-string msg true) msg)
        release-date (:release_date parsed-drop-record)]
    ; Output start sentinel
    (>!! producer-channel (create-sentinel-message release-date :start))

    ;; need to verify all entries in manifest are processed else warning and logging on unknown files.
    ;; 2. process the folder structure in order of tables for create-update and then reverse for deletes
    (doseq [procedure event-procedures]
      (let [bucket (:bucket parsed-drop-record)
            files (filter-files (:filter-string procedure) (:files parsed-drop-record))]
        (doseq [record-type (:order procedure)]
          (doseq [file (filter-files (:type record-type) files)]
            ;(let [file-reader (google-storage-line-reader bucket file)]
            (let [file-reader (construct-reader storage-protocol bucket file)]
              (process-clinvar-drop-file {:reader       file-reader
                                          :entity-type  (:type record-type)
                                          :release_date release-date
                                          :event-type   (:event-type procedure)
                                          :filter-field (:filter record-type)}))))))
    ; Output end sentinel
    (>!! producer-channel (create-sentinel-message release-date :end))))

(def listening-for-drop (atom true))

; Thread 3 - to output topic
(defn send-producer-messages
  "This function should be launched in a thread. It reads from the async channel `producer-channel`
  and writes those messages to the output kafka topic.

  If there are any validation problems during the processing of messages they are logged here"
  [opts kafka-config]
  (with-open [producer (jc/producer kafka-config)]
    (let [closed (atom false)]
      (while (not @closed)
        (let [msg (<!! producer-channel)]
          (if (not= msg nil)
            (send-update-to-exchange producer (:kafka-producer-topic opts) msg)
            (do (info "Shutting down send-producer-messages")
                (reset! closed true))))))))

(defn process-drop-messages
  "Process all outstanding messages from messages-to-consume queue"
  [opts]
  (debug "Starting process-drop-messages")
  (while @listening-for-drop
    (when-let [msg (first @messages-to-consume)]
      (infof "Got message on messages-to-consume queue: %s" msg)
      (if (contains? opts :storage-protocol)
        (process-clinvar-drop (:value msg) :storage-protocol (:storage-protocol opts))
        (process-clinvar-drop (:value msg)))
      (infof "Finished processing clinvar drop")
      (swap! messages-to-consume #(into [] (rest %))))
    (Thread/sleep 100)))

(def last-received-clinvar-drop (atom {}))

(defn listen-for-clinvar-drop
  "Listens to consumer topic for dsp clinvar drop notifications in `manifest` file list format.
  Produces output messages on producer topic."
  [opts kafka-config]
  (debug "Listening for clinvar drop")
  (with-open [consumer (jc/consumer kafka-config)]
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
    (if (:kafka-reset-consumer-offset opts)
      (do (info "Resetting to start of input topic")
          (jc/seek-to-beginning-eager consumer)))
    (debug "Subscribed to consumer topic " (:kafka-consumer-topic opts))
    (while @listening-for-drop
      (let [msgs (jc/poll consumer 100)]
        (doseq [m msgs]
          (tracef "Received message: %s" m)
          (swap! messages-to-consume conj m)
          (reset! last-received-clinvar-drop m))
        (Thread/sleep 100)))))

(defn -main
  [& args]
  (.start (Thread. (partial process-drop-messages (cfg/app-config))))
  (.start (Thread. (partial send-producer-messages
                            (cfg/app-config)
                            (cfg/kafka-config (cfg/app-config)))))

  (listen-for-clinvar-drop
    (cfg/app-config)
    (cfg/kafka-config (cfg/app-config))))
