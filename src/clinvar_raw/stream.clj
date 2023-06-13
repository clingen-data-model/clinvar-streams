(ns clinvar-raw.stream
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as config]
            [clinvar-raw.gcpbucket :as gcs]
            [clinvar-raw.ingest :as ingest]
            [clinvar-streams.storage.rocksdb :as rocksdb]
            [clinvar-streams.stream-utils :refer [get-max-offset
                                                  get-min-offset]]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as spec]
            [clojure.string :as str]
            [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [me.raynes.fs :as fs]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as log])
  (:import (com.google.cloud.storage Blob$BlobSourceOption BlobId StorageOptions)
           (java.io BufferedReader)
           (java.nio.channels Channels)
           (java.time Duration)
           (org.apache.kafka.common TopicPartition)))

(def order-of-processing
  [{:type "gene"}
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

(def event-procedures
  [{:event-type :create :order order-of-processing :filter-string "created"}
   {:event-type :update :order order-of-processing :filter-string "updated"}
   {:event-type :delete :order delete-order-of-processing :filter-string "deleted"}])

(def gc-storage (.getService (StorageOptions/getDefaultInstance)))

(def send-update-to-exchange-counter (atom (bigint 0)))

(defn send-update-to-exchange
  "Sends a message to the producer on `topic`, with the message key `key`, and payload `value`
  `value` can be a string or json-serializable object like a map"
  [producer topic {:keys [key value]}]
  (log/debug "Sending message with key %s to topic %s" key topic)
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
    (when (nil? blob)
      (throw (java.io.FileNotFoundException.
              (str "GCS blob not found: " bucket "/" filename))))
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
            (log/debug :fn :get-is-dup :msg msg)
            (let [output-value (:value msg)
                  is-dup? (ingest/duplicate? db output-value)]
              (when (or (not is-dup?) (= :create-to-update is-dup?))
                (ingest/store-new! db output-value))
              is-dup?))]
    (assoc msg :is-dup? (get-is-dup msg))))

(defn process-file [storage-protocol release-date event]
  (let [{:keys [bucket path order-entry event-type]} event]
    (log/info :fn :generate-messages-from-diff/process-file
              :bucket bucket
              :path path
              :order-entry order-entry
              :event-type event-type)
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
                                    event-type))))))

(defn process-files [storage-protocol release-date files-to-process]
  (when (seq files-to-process)
    (lazy-cat (process-file storage-protocol release-date (first files-to-process))
              (process-files storage-protocol release-date (rest files-to-process)))))

(defn trim-prefix
  "If S starts with PREFIX, return substring of S after PREFIX"
  [s prefix]
  (if (.startsWith s prefix)
    (subs s (.length prefix))
    s))

(defn list-files
  "For storage-protocol gs://, path segs should start with the bucket,
    since this is part of the full URL for objects.

  For storage-protocol file://, works in the same way as gs, paths returned
   are relative to the first path segment (bucket for gs) "
  [storage-protocol & path-segs]
  #_(log/info :fn :list-files :path-segs path-segs)
  (assert (< 0 (count path-segs)) "Must provide path or path segments")
  (case storage-protocol
    "gs://" (let [[bucket & path-segs] path-segs]
              (gcs/list-all-files bucket (str/join "/" path-segs)))
    "file://" (let [[root-path & relative-segs] path-segs]
                (->> (fs/list-dir (str/join "/" path-segs))
                     (mapv (fn [f]
                             (let [path (.getPath f)
                                   rel-path (trim-prefix (.getPath f) root-path)]
                               (if (.isDirectory f)
                                 ;; Expand relative path listing into this dir, anchored to same root path
                                 (list-files storage-protocol root-path rel-path)
                                 rel-path))))
                     flatten))))

(defn xor [& vals]
  (->> vals
       (map #(when % true))
       (filter identity)
       (#(= 1 (count %)))))
(spec/def ::release_date (spec/and string? seq))
(spec/def ::files (spec/coll-of string?))
(spec/def ::release_directory (spec/and string? seq))
;; Idea for :files :release_directory mutual exclusion from here:
;; https://stackoverflow.com/a/43374087/2172133
(spec/def ::parsed-diff-msg (spec/and (spec/keys :req-un [::release_date
                                                          (or ::files
                                                              ::release_directory)])
                                      #(xor (:files %) (:release_directory %))))

(spec/def ::storage-protocol #(#{"file://" "gs://"} %))

(defn generate-messages-from-diff
  "Takes a diff notification message, and returns a lazy seq of
   all the output messages in order for this diffed release."
  [parsed-diff-files-msg storage-protocol]
  (when-not (spec/valid? ::parsed-diff-msg parsed-diff-files-msg)
    (throw (ex-info "Invalid parsed-diff-files-msg"
                    {:spec/explain (spec/explain ::parsed-diff-msg parsed-diff-files-msg)})))
  (when-not (spec/valid? ::storage-protocol storage-protocol)
    (throw (ex-info "Invalid storage-protocol"
                    {:spec/explain (spec/explain ::storage-protocol storage-protocol)})))
  (log/info :fn :generate-messages-from-diff
            :parsed-diff-files-message parsed-diff-files-msg
            :storage-protocol storage-protocol)
  (let [release-date (:release_date parsed-diff-files-msg)]
    (->> (for [procedure event-procedures]
           (let [bucket (:bucket parsed-diff-files-msg)
                ;;  file-list (or (:files parsed-diff-files-msg)
                ;;                (->> (str (:release_directory parsed-diff-files-msg) "/")
                ;;                     (gcs/list-all-files bucket)))
                 file-list (or (:files parsed-diff-files-msg)
                               (list-files storage-protocol
                                           bucket
                                           (str (:release_directory parsed-diff-files-msg) "/")))
                 #_#__ (log/info :msg "file-list first 10" :file-list (->> file-list (take 10) (into [])))
                 files (filter-files (:filter-string procedure) file-list)
                 #_#__ (log/info :msg "filtered files" :files (->> files (into [])))]
             (for [order-entry (:order procedure)
                   file-path (filter-files (:type order-entry) files)]
               {:bucket bucket
                :path file-path
                :order-entry order-entry
                :event-type (:event-type procedure)})))
         flatten-one-level
         (process-files storage-protocol release-date))))

(defn process-clinvar-drop
  "Constructs a lazy sequence of output messages based on an input drop file
   from the upstream DSP service.
   Caller should avoid realizing whole sequence into memory."
    ;; TODO SPEC
  [{:keys [storage_protocol
           release_date]
    :or {storage_protocol "gs://"}
    :as release_info}]
  (when (not (map? release_info))
    (throw (ex-info "Must provide map" {:msg release_info})))
  (log/info {:fn :process-clinvar-drop :msg "Processing drop message" :drop-message release_info})
  (lazy-cat
   [(create-sentinel-message release_date :start)]
   (generate-messages-from-diff release_info storage_protocol)
   [(create-sentinel-message release_date :end)]))

(defn consumer-lazy-seq-bounded
  "Consumes a single-partition topic.
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
  "Consumes a single-partition topic using consumer-lazy-seq-bounded."
  ([consumer]
   (consumer-lazy-seq-bounded consumer ##Inf)))

(defn consumer-lazy-seq-full-topic
  "Consumes a single-partition topic using kafka-config,
   but discards any group.id in order to consumer in anonymous mode (no consumer group).
   Consumes messages up to the latest offset at the time the function was initially called.
   Optionally resets the consumer group to the beginning if reset-to-beginning? is true"
  ([topic-name kafka-config]
   (let [min-offset (get-min-offset topic-name 0)
         ;; get-max-offset is actually the "next" offset.
         ;; Consider changing get-max-offset to subtract 1
         max-offset (dec (get-max-offset topic-name 0))
         consumer (jc/consumer (dissoc kafka-config "group.id"))]
     (jc/assign-all consumer [topic-name])
     (jc/seek consumer (TopicPartition. topic-name 0) min-offset)
     (log/debug :min-offset min-offset :max-offset max-offset)
     (consumer-lazy-seq-bounded consumer max-offset))))

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

(defn start-streaming
  "Continuously process DSP drop messages from input-topic and
  process files from the storage bucket through to the output-topic
  for downstream processing."
  [opts kafka-opts]
  (let [input-topic (:DX_CV_RAW_INPUT_TOPIC opts)
        output-topic (:DX_CV_RAW_OUTPUT_TOPIC opts)
        max-input-count (:DX_CV_RAW_MAX_INPUT_COUNT opts)]
    (with-open [producer (jc/producer kafka-opts)
                consumer (jc/consumer kafka-opts)]
      (jc/subscribe consumer [{:topic-name input-topic}])
      (when (:KAFKA_RESET_CONSUMER_OFFSET opts)
        (log/info "Resetting to start of input topic")
        (jc/seek-to-beginning-eager consumer))
      (log/info "Subscribed to consumer topic ")
      (doseq [m (cond->> (consumer-lazy-seq-infinite consumer)
                  max-input-count (take max-input-count))
              :while @listening-for-drop]
        (let [m-value (-> m :value (json/parse-string true)) ;; process-drop [drop-message]
              input-counter (atom 0)
              output-counter (atom 0)
              input-type-counters (atom {})
              output-type-counters (atom {})]
          (doseq [filtered-message
                  (->> (process-clinvar-drop (assoc m-value
                                                    :storage_protocol (:STORAGE_PROTOCOL opts)))
                       ;; process-clinvar-drop returns [{:key ... :value ...}]
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
                     :removed-type-counts (into {}
                                                (letfn [(for-type [type]
                                                          (- (get @input-type-counters type 0)
                                                             (get @output-type-counters type 0)))]
                                                  (map #(vector % (for-type %))
                                                       (set (keys @input-type-counters)))))
                     :in-count (int @input-counter)
                     :out-count (int @output-counter)
                     :removed-ratio (when (< 0 @input-counter)
                                      (double (/ (- @input-counter @output-counter)
                                                 @input-counter)))}))))))

(defn repl-test []
  (reset-db)
  (let [opts (-> config/env-config
                 (assoc :DX_CV_RAW_INPUT_TOPIC "broad-dsp-clinvar")
                 (assoc :DX_CV_RAW_OUTPUT_TOPIC "clinvar-raw-dedup")
                 (assoc :KAFKA_RESET_CONSUMER_OFFSET true))
        kafka-config (-> (config/kafka-config opts)
                         (assoc  "group.id" "local-dev"))]
    (start-streaming opts kafka-config)))


(comment
  (reset-db)
  (let [messages (-> "events-variation-133137.txt"
                     io/reader
                     line-seq
                     (->> (map #(json/parse-string % true))
                          (map #(json-parse-key-in % [:content :content]))
                          (map (fn [m] {:value m}))))]
    (log/info :message-count (count messages))
    (let [deduped (into [] (dedup-clinvar-raw-seq dedup-db messages))]
      (log/info {:deduped-count (count deduped)})
      (log/info (str {:messages (into [] messages)}))
      (log/info (str {:deduped (into [] deduped)})))))

(defn start-with-env [args]
  (let [opts config/env-config
        kafka-opts (config/kafka-config opts)]
    (start-streaming opts kafka-opts)))


(comment
  "Using functions to generate entity stream locally, no kafka"
  (let [release-message {:release_date "2023-04-10",
                         :bucket "clinvar-releases",
                         :release_directory "2023-04-10"}
        release-message {:release_date "2023-04-10",
                         :bucket "clinvar-releases",
                         :release_directory
                         "/Users/kferrite/dev/genegraph/clinvar-releases/2023-04-10"}
        opts {:storage-protocol "gs://"}
        reached-variations (atom false)
        passed-variations (atom false)]
    (with-open [writer (io/writer "re-generated-variations.txt")]
      (doseq [msg (->> (process-clinvar-drop (merge release-message opts))
                       (filter #(= "variation" (-> % :value :content :entity_type))))
              :while (or (not @reached-variations)
                         (not @passed-variations))]
        (let [entity_type (-> msg :value :content :entity_type)]
          (when (= "variation" entity_type)
            (reset! reached-variations true)
            (.write writer (json/generate-string msg))
            (.write writer "\n"))
          (when (and @reached-variations
                     (not= "variation" entity_type))
            (reset! passed-variations true)))))))

(comment
  ;; lazy seq over entities in bucket
  (def msgs (->> (generate-messages-from-diff
                  {:release_date "2023-04-10"
                   :bucket "clinvar-releases"
                   :release_directory "2023-04-10"}
                  "gs://")))

  ;; for variations only, expecting 2210627 records
  (with-open [writer (io/writer "re-generated-scv.txt")]
    (doseq [msg (generate-messages-from-diff
                 {:release_date "2023-04-10"
                  :bucket "clinvar-releases"
                  :release_directory "2023-04-10"}
                 "gs://")]
      (.write writer (json/generate-string msg))
      (.write writer "\n")))


  ;; lazy seq over entitites in local directory
  (def msgs (->> (generate-messages-from-diff
                  {:release_date "2023-04-10"
                   :bucket "/Users/kferrite/dev/clinvar-releases/"
                   :release_directory "2023-04-10"}
                  "file://")))

  (first msgs)


  (time
   (->> (generate-messages-from-diff
         {:release_date "2023-04-10"
          :bucket "/Users/kferrite/dev/clinvar-releases/"
          :release_directory "2023-04-10"}
         "file://")
        #_(take (long 10000))
        count)
   ;; 2210627
   ))

(comment
  "Validate counts from local file and bucket match. Eliminate bug in lazy-line-reader*"
  (let [rel-path-list (list-files "file://" "/Users/kferrite/dev/clinvar-releases/2023-04-10/") ;; trailing slash
        rel-path-list (filter #(or (.contains % "created")
                                   (.contains % "updated")
                                   (.contains % "deleted"))
                              rel-path-list)
        local-args  [construct-reader "file://" "/Users/kferrite/dev/clinvar-releases/2023-04-10"]
        bucket-args [construct-reader "gs://" "clinvar-releases"]]
    (doseq [rel-path rel-path-list]
      (log/info :rel-path rel-path)
      (let [local-count (count (lazy-line-reader-threaded (apply partial (flatten [local-args rel-path]))))
            bucket-count (count (lazy-line-reader-threaded (apply partial (flatten [bucket-args
                                                                                    (str "2023-04-10/" rel-path)]))))]
        (assert (= local-count bucket-count)
                {:msg "Counts don't match"
                 :rel-path rel-path
                 :local-count local-count
                 :bucket-count bucket-count})))))
