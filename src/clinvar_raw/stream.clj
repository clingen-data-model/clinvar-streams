(ns clinvar-raw.stream
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as cfg]
            [clinvar-raw.ingest :as ingest]
            [clojure.java.io :as io]
            [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [taoensso.timbre :as log])
  (:import (com.google.cloud.storage BlobId StorageOptions)
           com.google.cloud.storage.Blob$BlobSourceOption
           java.io.BufferedReader
           java.nio.channels.Channels
           (java.time Duration)))

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
                          {:type "variation_archive"}])

(def delete-order-of-processing (reverse order-of-processing))

(def event-procedures [{:event-type :create :order order-of-processing :filter-string "created"}
                       {:event-type :update :order order-of-processing :filter-string "updated"}
                       {:event-type :delete :order delete-order-of-processing :filter-string "deleted"}])

(def gc-storage (.getService (StorageOptions/getDefaultInstance)))

(def send-update-to-exchange-counter (atom (bigint 0)))

(defn send-update-to-exchange
  "Sends a message to the producer on `topic`, with the message key `key`, and payload `value`
  `value` can be a string or json-serializable object like a map"
  [producer topic {:keys [key value]}]
  (log/tracef "Sending message with key %s to topic %s" key topic)
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic}
                                          key
                                          (if (string? value) value (json/generate-string value))))
  (swap! send-update-to-exchange-counter inc))

(defn google-storage-line-reader
  "Returns an open reader to the storage object in `bucket` with path `filename`."
  [bucket filename]
  (log/debugf "Opening gs://%s/%s" bucket filename)
  (let [blob-id (BlobId/of bucket filename)
        blob (.get gc-storage blob-id)]
    (log/debugf "Obtaining reader to blob %s/%s" bucket filename)
    (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.)))

(defn line-map-to-event
  "Parses a single line of a drop file, transforms into an event object map
   `line-map` is one parsed JSON object read in from a clinvar diff file.
   `entity-type` is a type like 'variation', 'gene', etc, from clinvar.
   `release_date` is the date string to attach to the event message.
       Should be the date the object appeared in clinvar.
   `event-type` is one of: create, update, delete."
  [line-map entity-type release_date event-type]

  (let [content (-> line-map
                    (assoc :entity_type entity-type)
                    (assoc :clingen_version 0))
        key (str entity-type "_" (:id content) "_" release_date)
        event {:release_date release_date
               :event_type event-type
               :content content}]
    {:key key :value event}))

(defn create-sentinel-message
  "sentinel-type should be one of :start, :end"
  [release-tag sentinel-type]
  {:key (str "release_sentinel_" release-tag)
   :value {:event_type "create"
           :release_date release-tag
           :content {:entity_type "release_sentinel"
                     :clingen_version 0
                     :sentinel_type (name sentinel-type)
                     ; For releases from upstream, the release tag is clinvar-<releasedate>
                     :release_tag (str "clinvar-" release-tag)
                     :rules []
                     :source "clinvar"
                     :reason (str "ClinVar Upstream Release " release-tag),
                     :notes nil                             ; TODO
                     ; notes could point to ClinVar release notes (ftp release notes move), or a clingen release notes page
                     }}})

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
        :else (io/reader (apply io/file path-segs))))

(def listening-for-drop (atom true))

(defn read-newline-json
  [{:keys [reader file-read-limit]
    :or {file-read-limit ##Inf}}]
  (->>
   (line-seq reader)
   (take file-read-limit)
   (map #(json/parse-string % true))
   (filter #(not (nil? %)))))

(defn filter-by-field
  "Filters values seq by whether field-key maps to field-value.
   If field-key is nil, apply no filtration."
  [field-key field-value values]
  (filter (fn [val]
            (or (nil? field-key)
                (= field-value (get val field-key))))
          values))

(defn process-clinvar-drop
  "Parses and processes the clinvar drop notification from upstream DSP service.
   Calls callback-fn on each resulting output message."
  [msg callback-fn {:keys [storage-protocol]
                    :or {storage-protocol "gs://"}}]
  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest
  (log/info {:fn :process-clinvar-drop :msg "Processing drop message" :drop-message msg})
  (let [parsed-drop-record (if (string? msg) (json/parse-string msg true) msg)
        release-date (:release_date parsed-drop-record)]
    ; Output start sentinel
    (callback-fn (create-sentinel-message release-date :start))

    ;; TODO verify all entries in manifest are processed else warning and logging on unknown files.
    ;; 2. process the folder structure in order of tables for create-update and then reverse for deletes
    ;; An event procedure per: add, update, delete
    ;; Don't care about return, just want to execute it all
    (doseq [procedure event-procedures]
      (log/info {:msg "Starting to process procedure" :procedure procedure})
      (let [bucket (:bucket parsed-drop-record)
            files (filter-files (:filter-string procedure) (:files parsed-drop-record))]
        (log/info {:msg "Processing files for procedure" :bucket bucket :files files})
        (doseq [record-type (:order procedure)]
          (doseq [file-path (filter-files (:type record-type) files)]
            (log/info "Opening file: " (str bucket "/" file-path))
            (with-open [file-reader (construct-reader storage-protocol
                                                      bucket
                                                      file-path)]
              (log/info "Iterating over file: " (str bucket "/" file-path))
              (doseq [output-message
                      (->> (read-newline-json {:reader file-reader})
                           (filter-by-field (-> record-type :filter :field)
                                            (-> record-type :filter :value))
                           (map #(line-map-to-event %
                                                    (:type record-type)
                                                    release-date
                                                    (:event-type procedure))))]
                (callback-fn output-message)))))))

    ; Output end sentinel
    (callback-fn (create-sentinel-message release-date :end))))


(defn start
  ([]
   (let [opts (cfg/app-config)
         kafka-opts (cfg/kafka-config opts)]
     (start opts kafka-opts)))
  ([opts kafka-opts]
   (let [output-topic (:kafka-producer-topic opts)
         input-counter (atom (bigint 0))
         output-counter (atom (bigint 0))
         create-to-update-counter (atom (bigint 0))]
     (with-open [producer (jc/producer kafka-opts)
                 consumer (jc/consumer kafka-opts)]
       (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
       (when (:kafka-reset-consumer-offset opts)
         (log/info "Resetting to start of input topic")
         (jc/seek-to-beginning-eager consumer))
       (log/info "Subscribed to consumer topic " (:kafka-consumer-topic opts))
       (while @listening-for-drop
         (let [msgs (jc/poll consumer (Duration/ofSeconds 5))]
           (when (not (empty? msgs))
             (log/info "Received poll batch of size: " (count msgs)))
           (doseq [m msgs]
             (log/infof "Received drop message: %s" m)
             (letfn [(callback-fn [output-m]
                       (swap! input-counter inc)
                       ;; Check the message for false positive updates
                       (let [mdup? (ingest/duplicate? m)]
                             ;; If its not a duplicate or it is a duplicate but the
                             ;; return value is :create-to-update, persist the value of m
                         (when (= :create-to-update mdup?)
                           (swap! create-to-update-counter inc))
                         ;; Store the most recent always, even if duplicate.
                         ;; Could be useful for analysis or doing further deep diffs.
                         (ingest/store-new! m)
                         (when (not mdup?)
                           (send-update-to-exchange producer output-topic output-m)
                           (swap! output-counter inc))))]
               (process-clinvar-drop (:value m)
                                     callback-fn
                                     (select-keys opts [:storage-protocol]))))))))))
