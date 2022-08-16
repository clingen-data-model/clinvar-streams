(ns clinvar-raw.stream
  (:require [taoensso.timbre :as log]
            [jackdaw.data :as jd]
            [jackdaw.client :as jc]
            [clojure.java.io :as io]
            [clojure.walk :refer [postwalk]]
            [cheshire.core :as json]
            [clinvar-raw.config :as cfg])
  (:import (com.google.cloud.storage Storage StorageOptions BlobId Blob)
           com.google.cloud.storage.Blob$BlobSourceOption
           java.nio.channels.Channels
           java.io.BufferedReader))

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
  "Sends a message to the producer on `topic`, with the message key `key`, and payload `data`
  `data` can be a string or json-serializable object like a map"
  [producer topic {:keys [key value]}]
  (log/infof "Sending message with key %s to topic %s" key topic)
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic}
                                          key
                                          (if (string? value) value (json/generate-string value))))
  (swap! send-update-to-exchange-counter inc))

(defn google-storage-line-reader
  "Returns a reader to the storage object. Caller must open (with-open)"
  [bucket filename]
  (log/debugf "Opening gs://%s/%s" bucket filename)
  (let [blob-id (BlobId/of bucket filename)
        blob (.get gc-storage blob-id)]
    (log/debugf "Obtaining reader to blob %s/%s" bucket filename)
    (-> blob (.reader (make-array Blob$BlobSourceOption 0)) (Channels/newReader "UTF-8") BufferedReader.)))

(defn line-map-to-event
  "Parses a single line of a drop file, transforms into an event object map"
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
  ;(try (assert (util/in? sentinel-type [:start :end]))
  ;     (catch AssertionError e
  ;       (error e)))

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

(defn future-realized? [ftr sym]
  (let [r (realized? ftr)]
    (log/info {:fn :future-realized? :msg (format "(realized? %s) = %s" sym r)})
    r))

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
  "Parses and processes the clinvar drop notification from dsp, returns a seq of output messages"
  [msg {:keys [storage-protocol]
        :or {storage-protocol "gs://"}}]
  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest
  (log/debug {:fn :process-clinvar-drop :msg "Processing drop message" :drop-message msg})
  (let [parsed-drop-record (if (string? msg) (json/parse-string msg true) msg)
        release-date (:release_date parsed-drop-record)]
    (flatten
     ; Output start sentinel
     [(create-sentinel-message release-date :start)

      ;; TODO verify all entries in manifest are processed else warning and logging on unknown files.
      ;; 2. process the folder structure in order of tables for create-update and then reverse for deletes
      ;; An event procedure per: add, update, delete
      (for [procedure event-procedures]
        (do (log/info {:msg "Starting to process procedure" :procedure procedure})
            (let [bucket (:bucket parsed-drop-record)
                  files (filter-files (:filter-string procedure) (:files parsed-drop-record))]
              (log/info {:msg "Processing files for procedure" :bucket bucket :files files})
              (for [record-type (:order procedure)]
                (for [file-path (filter-files (:type record-type) files)]
                  (do (log/info "Reading file: " file-path)
                      (with-open [file-reader (construct-reader storage-protocol
                                                                bucket
                                                                file-path)]
                        (->> (read-newline-json {:reader file-reader})
                             (filter-by-field (-> record-type :filter :field)
                                              (-> record-type :filter :value))
                             (map #(line-map-to-event %
                                                      (:type record-type)
                                                      release-date
                                                      (:event-type procedure)))
                             doall))))))))

      ; Output end sentinel
      (create-sentinel-message release-date :end)])))

(defn dissoc-nil-values
  "Removes keys from maps for which the value is nil."
  [input-map]
  (apply dissoc input-map (filter #(= nil (get input-map %)) (keys input-map))))

(defn dissoc-nil-values-recur
  "Removes keys from maps for which the value is nil. Recurses with clojure.walk/postwalk"
  [input-map]
  (postwalk #(if (map? %) (dissoc-nil-values %) %) input-map))

(defn select-keys-if
  "Same as select-keys, except only selects a given key k if the value of (get m k) is not nil"
  [m keys]
  (->> (select-keys m keys)
       dissoc-nil-values))

(defn start
  ([]
   (let [opts (cfg/app-config)
         kafka-opts (cfg/kafka-config opts)]
     (start opts kafka-opts)))
  ([opts kafka-opts]
   (let [output-topic (:kafka-producer-topic opts)]
     (with-open [producer (jc/producer kafka-opts)
                 consumer (jc/consumer kafka-opts)]
       (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
       (when (:kafka-reset-consumer-offset opts)
         (log/info "Resetting to start of input topic")
         (jc/seek-to-beginning-eager consumer))
       (log/debug "Subscribed to consumer topic " (:kafka-consumer-topic opts))
       (while @listening-for-drop
         (let [msgs (jc/poll consumer 100)]
           (doseq [m msgs]
             (log/tracef "Received message: %s" m)
             (let [output-messages (process-clinvar-drop (:value m)
                                                         (select-keys opts [:storage-protocol]))]
               (log/info "Finished processing clinvar drop")
               (log/info "Sending messages to output topic")
               (doseq [output-m output-messages]
                 (send-update-to-exchange producer output-topic output-m))))))))))
