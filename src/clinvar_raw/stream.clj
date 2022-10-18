(ns clinvar-raw.stream
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as cfg]
            [clinvar-raw.ingest :as ingest]
            [clinvar-streams.storage.rocksdb :as rocksdb]
            [clinvar-streams.stream-utils :refer [get-max-offset
                                                  get-min-offset]]
            [clinvar-streams.util :refer [parse-nested-content]]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as log])
  (:import (com.google.cloud.storage BlobId StorageOptions)
           com.google.cloud.storage.Blob$BlobSourceOption
           java.io.BufferedReader
           java.nio.channels.Channels
           (java.time Duration)
           (org.apache.kafka.common TopicPartition)))

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
    (-> blob
        (.reader
         (make-array Blob$BlobSourceOption 0)
         ;; Some intermittent exceptions on reading the object when run locally
         ;; the method of automatically determining the credentials and project
         ;; to use may be different on the cluster vs locally
         #_(into-array [(Blob$BlobSourceOption/userProject "....")]))
        (Channels/newReader "UTF-8")
        BufferedReader.)))

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
                     :notes nil}}})                             ; TODO
                     ; notes could point to ClinVar release notes (ftp release notes move), or a clingen release notes page


(defn filter-files
  "Filters a collection of file strings containing a path segment which matches `filter-string`"
  [filter-string files]
  (filter #(re-find (re-pattern (str "/" filter-string "/")) %) files))

(defn construct-reader
  "Attempts to construct a reader to a sequence of file URL fragments.
  Example: (construct-reader /home foo bar) will return a reader to '/home/foo/bar'
           (construct-reader gs:// bucket dir file) will return a reader to gs://bucket/dir/file"
  [protocol & path-segs]
  (log/info :fn :construct-reader :msg "Constructing reader"
            :protocol protocol :path-segs path-segs)
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

(defn lazy-line-reader-threaded
  "READER-FN is called to return an open reader.
   Returns a lazy-seq of lines, and closes the reader when done.
   Internally uses a reader thread that will greedily try to keep an async/chan
   full so that any I/O is done ahead of the time the line is needed by the application.
   This will speed up applications where the I/O operation is sufficiently expensive and the
   time spent processing each line is high enough that the reader thread can stay ahead of the
   line processing by the application. For example a (count) on the return from this will be
   slower than the non-threaded version of this function.
   But a loop doing some work for each will be faster because each call to get the next item
   will never have to wait for I/O."
  [reader-fn]
  (let [reader (reader-fn)
        buffer-size 1000
        line-counter (atom (bigint 0))
        line-chan (async/chan buffer-size)]
    (letfn [(enqueuer []
              (doseq [line (line-seq reader)]
                (swap! line-counter #(+ % 1))
                (async/>!! line-chan line))
              (do (log/info :fn :lazy-line-reader-threaded
                            :msg "Closing reader"
                            :total-lines @line-counter)
                  (.close reader)
                  (async/close! line-chan)))
            (dequeuer []
              (for [i (range)
                    :let [line (async/<!! line-chan)]
                    :while line]
                line))]
      (lazy-seq (do (.start (Thread. enqueuer))
                    (dequeuer))))))

(defn lazy-line-reader
  "READER-FN is called to return an open reader.
   Returns a lazy-seq of lines, and closes the reader when done."
  [reader-fn]
  (let [reader (reader-fn)
        batch-size 10
        line-counter (atom (bigint 0))]
    (letfn [(get-batch [lines]
              (let [batch (take batch-size lines)]
                (if (seq batch)
                  (do (swap! line-counter #(+ % (count batch)))
                      (lazy-cat batch (get-batch (nthrest lines batch-size))))
                  (do (log/info :fn :lazy-line-reader
                                :msg "Closing reader"
                                :total-lines @line-counter)
                      (.close reader)))))]
      (lazy-seq (get-batch (line-seq reader))))))

(defn flatten-one-level [things]
  (for [coll things e coll] e))

#_(defn unchunk [s]
    (lazy-cat [(first s)] (unchunk (rest s))))

#_(defn concatenated-lazy-line-seq
    [storage-protocol & path-seg-groups]
    (when (seq path-seg-groups)
      (lazy-cat
       (let [path-segs (first path-seg-groups)
             reader-fn (partial apply
                                (partial construct-reader
                                         storage-protocol)
                                path-segs)]
         (->> (lazy-line-reader reader-fn)))
       (concatenated-lazy-line-seq storage-protocol (rest path-seg-groups)))))

(defn dedup-clinvar-raw-seq
  "Takes a kafka message seq [{:key ... :value ...} ...] and deduplicate it.
   Optionally takes a map of atoms to count inputs and outputs for monitoring purposes."
  [db messages & [{parallelize? :parallelize?
                   input-counter :input-counter
                   output-counter :output-counter
                   :or {input-counter (atom 0)
                        output-counter (atom 0)}}]]
  (letfn [(not-a-dup? [msg]
            (log/debug :fn :not-a-dup :msg msg)
            (let [output-value (:value msg)
                  is-dup? (ingest/duplicate? db output-value)]
              (swap! input-counter inc)
              (when (or (not is-dup?) (= :create-to-update is-dup?))
                (ingest/store-new! db output-value))
              (log/debug {:is-dup? is-dup?})
              (when (not is-dup?)
                (swap! output-counter inc))
              (not is-dup?)))]
    (if parallelize?
      ;; TODO return this [dup? msg] tuple seq and let caller filter
      (->> (pmap (fn [msg] [(not-a-dup? msg) msg]) messages)
           (filter #(= true (first %)))
           (map second))
      (filter not-a-dup? messages))))

(defn annotate-is-dup?
  "Annotates map MSG with :is-dup?, which is true if the message
   is a duplicate. Writes data to DB to track seen messages.
   Expects MSG to be the :value of a kafka message, i.e. the entity
   payload containing a release_date, content, event_type."
  ;; TODO SPEC
  [db msg]
  (letfn [(get-is-dup [msg]
            (log/debug :fn :not-a-dup :msg msg)
            (let [output-value (:value msg)
                  is-dup? (ingest/duplicate? db output-value)]
              (when (or (not is-dup?) (= :create-to-update is-dup?))
                (ingest/store-new! db output-value))
              is-dup?))]
    (assoc msg :is-dup? (get-is-dup msg))))

(defn generate-messages-from-diff
  "Takes a diff notification message, and returns a lazy seq of
   all the output messages in order for this diffed release."
  [parsed-diff-files-msg storage-protocol]
  (let [release-date (:release_date parsed-diff-files-msg)]
    (letfn [(process-file [{:keys [bucket path order-entry event-type]}]
              (let [reader-fn (partial construct-reader
                                       storage-protocol
                                       bucket
                                       path)]
                (->> (lazy-line-reader-threaded reader-fn)
                     (map #(json/parse-string % true))
                     (filter-by-field (-> order-entry :filter :field)
                                      (-> order-entry :filter :value))
                     (map #(line-map-to-event %
                                              (:type order-entry)
                                              release-date
                                              event-type)))))
            (process-files [files-to-process]
              (when (seq files-to-process)
                (lazy-cat (process-file (first files-to-process))
                          (process-files (rest files-to-process)))))]
      (->> (for [procedure event-procedures]
             (let [bucket (:bucket parsed-diff-files-msg)
                   files (filter-files (:filter-string procedure) (:files parsed-diff-files-msg))]
               (for [order-entry (:order procedure)
                     file-path (filter-files (:type order-entry) files)]
                 {:bucket bucket
                  :path file-path
                  :order-entry order-entry
                  :event-type (:event-type procedure)})))
           flatten-one-level
           process-files))))

(defn process-clinvar-drop-refactor
  "Constructs a lazy sequence of output messages based on an input drop file
   from the upstream DSP service.
   Caller should avoid realizing whole sequence into memory."
  ;; TODO SPEC
  [msg {:keys [storage-protocol]
        :or {storage-protocol "gs://"}}]
  ; 1. parse the drop message to determine where the files are
  ; this will return the folder and bucket and file manifest
  (log/info {:fn :process-clinvar-drop-refactor :msg "Processing drop message" :drop-message msg})
  (let [parsed-drop-record (if (string? msg) (json/parse-string msg true) msg)
        release-date (:release_date parsed-drop-record)]
    (lazy-cat
     [(create-sentinel-message release-date :start)]
     (generate-messages-from-diff parsed-drop-record storage-protocol)
     [(create-sentinel-message release-date :end)])))

(defn consumer-lazy-seq-bounded
  "Consumes a single-partition topic using kafka-config.
  Consumes messages up to the latest offset at the time the function was initially called.
  Optionally resets the consumer group to the beginning if reset-to-beginning? is true
  Closes the consumer when the end-offset is reached."
  ;; TODO check bounds on end
  [consumer end-offset]
  (letfn [(do-poll []
            (let [msgs (jc/poll consumer (Duration/ofSeconds 10))]
              (when (seq msgs)
                (log/info {:fn :consumer-lazy-seq-bounded
                           :msgs-count (count msgs)
                           :offsets (map :offset msgs)}))
              (if (some #(= end-offset (:offset %)) msgs)
                (do (.close consumer)
                    (filter #(<= (:offset %) end-offset) msgs))
                (lazy-cat msgs (do-poll)))))]
    (lazy-seq (do-poll))))

(defn consumer-lazy-seq-infinite
  "Consumes a single-partition topic using kafka-config.
  Consumes messages up to the latest offset at the time the function was initially called.
  Optionally resets the consumer group to the beginning if reset-to-beginning? is true
  Closes the consumer when the end-offset is reached."
  ([consumer]
   (consumer-lazy-seq-bounded consumer ##Inf)))

(defn consumer-lazy-seq-full-topic
  "Consumes a single-partition topic using kafka-config, but discards any group.id in order
   to consumer in anonymous mode (no consumer group).
   Consumes messages up to the latest offset at the time the function was initially called.
   Optionally resets the consumer group to the beginning if reset-to-beginning? is true"
  ([topic-name kafka-config]
   (let [min-offset (get-min-offset topic-name 0)
         ;; get-max-offset is actually the "next" offset.
         ;; Consider changing get-max-offset to subtract 1
         max-offset (dec (get-max-offset topic-name 0))
         consumer (jc/consumer (-> kafka-config
                                   (dissoc "group.id")))]
     (jc/assign-all consumer [topic-name])
     (jc/seek consumer (TopicPartition. topic-name 0) min-offset)
     (log/debug :min-offset min-offset :max-offset max-offset)
     (consumer-lazy-seq-bounded consumer
                                max-offset))))

(declare dedup-db)
(defstate dedup-db
  :start (rocksdb/open "clinvar-raw-dedup.db")
  :stop (rocksdb/close dedup-db))

(defn reset-db []
  (mount.core/stop #'dedup-db)
  (rocksdb/rocks-destroy! "clinvar-raw-dedup.db")
  (mount.core/start #'dedup-db))

(defn json-parse-key-in
  "Replaces key K in map M with the json parsed value of K in M"
  [m ks]
  (update-in m ks #(json/parse-string % true)))

(defn json-unparse-key-in
  [m ks]
  (update-in m ks #(json/generate-string %)))

(defn start [opts kafka-opts]
  (let [output-topic (:kafka-producer-topic opts)
        max-input-count Long/MAX_VALUE]
    (with-open [producer (jc/producer kafka-opts)
                consumer (jc/consumer kafka-opts)]
      (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
      (when (:kafka-reset-consumer-offset opts)
        (log/info "Resetting to start of input topic")
        (jc/seek-to-beginning-eager consumer))
      (log/info "Subscribed to consumer topic " (:kafka-consumer-topic opts))
      (doseq [m (-> consumer
                    (consumer-lazy-seq-infinite)
                    (->> (take max-input-count)))
              :while @listening-for-drop]
        (let [m-value (-> m :value
                          (json/parse-string true))
              input-counter (atom 0)
              output-counter (atom 0)
              input-type-counters (atom {})
              output-type-counters (atom {})]
          (doseq [filtered-message
                  (->> (process-clinvar-drop-refactor m-value
                                                      (select-keys opts [:storage-protocol]))
                       ;; process-clinvar-drop-refactor returns [{:key ... :value ...}]
                       (map #(json-parse-key-in % [:value :content :content]))

                       ;; Adds :is-dup? key
                       ;; Changing this to pmap will make the I/O faster, but
                       ;; only if there are enough cores available
                       (map (partial annotate-is-dup? dedup-db))

                       ;; Counts for individual entity types
                       (map (fn [m]
                              (let [entity-type (-> m :value :content :entity_type)
                                    inc-type-counter (fn [counter-map entity-type]
                                                       (assert (not (nil? entity-type))
                                                               (str "nil entity-type for message: " m))
                                                       (assoc counter-map
                                                              entity-type
                                                              (inc (get counter-map entity-type 0))))]
                                (swap! input-type-counters inc-type-counter entity-type)
                                (if (:is-dup? m)
                                  (log/debug {:message "Duplicate found" :msg  m})
                                  (swap! output-type-counters inc-type-counter entity-type))
                                m)))
                       ;; Overall count
                       (map #(do (swap! input-counter inc) %))
                       ;; Filter out duplicates and remove the :is-dup? key
                       (filter #(not (:is-dup? %)))
                       (map #(dissoc % :is-dup?)))]
            ;; TODO SPEC
            (assert (map? filtered-message) {:msg "Expected map" :filtered-message filtered-message})
            (let [output-message (json-unparse-key-in filtered-message
                                                      [:value :content :content])]
              (swap! output-counter inc)
              (send-update-to-exchange producer output-topic output-message)))
          (log/info {:msg "Deduplication counts for release"
                     :release-date (:release_date m-value)
                     :in-type-counts @input-type-counters
                     :out-type-counts @output-type-counters
                     :in-count (int @input-counter)
                     :out-count (int @output-counter)
                     :removed-ratio (when (< 0 @input-counter)
                                      (double (/ (- @input-counter @output-counter)
                                                 @input-counter)))}))))))

(defn repl-test []
  (reset-db)
  (let [opts (-> (cfg/app-config)
                 (assoc :kafka-consumer-topic "broad-dsp-clinvar")
                 (assoc :kafka-producer-topic "clinvar-raw-dedup")
                 (assoc :kafka-reset-consumer-offset true))
        kafka-config (-> (cfg/kafka-config opts)
                         (assoc  "group.id" "kyle-dev"))]
    (start opts kafka-config)))

(comment
  (reset-db)
  (let [messages (-> "events-variation-133137.txt"
                     io/reader
                     line-seq
                     (->> (map #(json/parse-string % true))
                          (map parse-nested-content)
                          (map (fn [m] {:value m}))))]
    (log/info :message-count (count messages))
    (let [deduped (into [] (dedup-clinvar-raw-seq dedup-db messages))]
      (log/info {:deduped-count (count deduped)})
      (log/info (str {:messages (into [] messages)}))
      (log/info (str {:deduped (into [] deduped)})))))


(defn start-with-env []
  (let [opts (cfg/app-config)
        kafka-opts (cfg/kafka-config opts)]
    (start opts kafka-opts)))
