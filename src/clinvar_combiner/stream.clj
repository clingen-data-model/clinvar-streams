(ns clinvar-combiner.stream
  (:require [clinvar-combiner.config :as config
             :refer [topic-metadata]]
            [jackdaw.client :as jc]
            [clinvar-combiner.combiners.clinical-assertion :as c-assertion]
            [clinvar-combiner.combiners.core :as c-core]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clinvar-streams.storage.database-sqlite.sink2 :as sink]
            [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.java.jdbc :as jdbc])
  (:import (java.time Duration)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.clients.consumer KafkaConsumer)))

(defn topic-partitions
  "Returns a seq of TopicPartitions that the consumer is subscribed to for topic-name."
  [consumer topic-name]
  (let [partition-infos (.partitionsFor consumer topic-name)]
    (map #(TopicPartition. (.topic %) (.partition %)) partition-infos)))

(defn make-consume-seq
  "Returns a function which when called returns a lazy-seq over all the messages
  available from the consumer, forever. Inserts nils on invocations when no messages
  are available at that time."
  [consumer]
  (letfn [(consume-batch [consumer]
            (try (.commitSync ^KafkaConsumer consumer)
                 (catch InterruptedException e
                   (log/error "Got InterruptedException in commitSync():" e)))
            (let [batch (try (jc/poll consumer (Duration/ofSeconds 5))
                             (catch InterruptedException e
                               (log/info "InterruptedException in poll() call:" e)
                               ;(throw e)
                               ))]
              (if (not (empty? batch))
                (lazy-cat batch (consume-batch consumer))
                (lazy-cat [nil] (consume-batch consumer)))))]
    (partial consume-batch consumer)))

;(defn make-consume-fn
;  "Given a consumer, returns a function which on each invocation
;  returns the next message in the stream, and closes when there are no more."
;  [consumer]
;  (let [consumer-seq (atom ((make-consume-seq consumer)))]
;    (letfn [(consume-fn []
;              (let [m (first @consumer-seq)]
;                ; rest is lazy
;                (reset! consumer-seq (rest @consumer-seq))
;                m))]
;      (partial consume-fn))))

(defn make-produce-fn
  "Given a producer, returns a function which produces a single string message argument
  to the output topic configured in `topic-metadata`."
  [producer]
  (letfn [(produce-fn [msg]
            (jc/produce! producer (:output topic-metadata) msg))]
    produce-fn))

(defn set-consumer-to-db-offset
  "Takes a consumer and a seq of TopicPartition objects."
  [consumer partitions]
  (doseq [partition partitions]
    (let [topic-name (.topic partition)
          partition-idx (.partition partition)
          local-offset (or (db-client/get-offset topic-name partition-idx) 0)]
      (log/info (format "Seeking to offset %s for partition (%s, %s)"
                        local-offset topic-name partition-idx))
      (jc/seek consumer partition local-offset))))

(defn mark-database-clean! []
  (let [tables-to-clean ["release_sentinels"
                         "submitter"
                         "submission"
                         "trait"
                         "trait_set"
                         "clinical_assertion_trait_set"
                         "clinical_assertion_trait"
                         "gene"
                         "variation"
                         "gene_association"
                         "variation_archive"
                         "rcv_accession"
                         "clinical_assertion"
                         "clinical_assertion_observation"
                         "clinical_assertion_variation"
                         "trait_mapping"]]
    (doseq [table-name tables-to-clean]
      (let [updated-count (jdbc/execute! @db-client/db [(format "update %s set dirty = 0 where dirty = 1" table-name)])]
        (log/infof "Marked %s records in table %s as clean" updated-count table-name)))))

(defn process-release-sentinel
  "Given a release sentinel, send messages to producer.
  If a start sentinel, just pass through.
  If an end sentinel, flush all appropriate stored records followed by the sentinel.
  Calls `produce-fn` for each message. Should accept a single String argument."
  [release-sentinel produce-fn!]

  (let [sentinel-type (get-in release-sentinel [:content :sentinel_type])]
    (case sentinel-type
      "start"
      (do (let [record-json (json/generate-string release-sentinel)]
            (log/info "Got start sentinel" record-json)
            (produce-fn! record-json)))

      "end"
      ; Flush non-SCVs
      ; sink/get-dirty returns lazy seq, avoid realizing it
      (do
        (log/info {:fn :process-release-sentinel :msg "Flushing release" :value release-sentinel})
        (doseq [record (c-core/get-dirty release-sentinel)]
          (let [record-json (json/generate-string record)]
            (log/info {:fn :process-release-sentinel :msg "Producing message" :value record-json})
            (produce-fn! record-json)))

        ; Flush SCVs
        (let []
          ;(log/infof "Received %d messages to flush" (count scvs-to-flush))
          (doseq [clinical-assertion (c-assertion/dirty-bubble-scv release-sentinel)]
            (let [built-clinical-assertion (c-assertion/build-clinical-assertion clinical-assertion)
                  built-clinical-assertion (c-assertion/post-process-built-clinical-assertion built-clinical-assertion)
                  built-clinical-assertion-json (json/generate-string built-clinical-assertion)]
              (if (nil? (:id built-clinical-assertion))
                (throw (ex-info "assertion :id cannot be nil"
                                {:cause built-clinical-assertion-json})))
              (if (nil? (:release_date built-clinical-assertion))
                (throw (ex-info "assertion :release_date cannot be nil"
                                {:cause built-clinical-assertion-json})))
              (comment (let [fpath (format "debug/SCV/%s/%s.json"
                                           (:release_date built-clinical-assertion)
                                           (:id built-clinical-assertion))]
                         (io/make-parents fpath)
                         (with-open [fwriter (io/writer fpath)]
                           (.write fwriter built-clinical-assertion-json))))
              (log/info {:fn :process-release-sentinel :msg "Producing message" :value built-clinical-assertion-json})
              ; Write message to output
              (produce-fn! built-clinical-assertion-json))))

        ; Write end release sentinel to output
        (let [record-json (json/generate-string release-sentinel)]
          (log/info {:fn :process-release-sentinel :msg "Producing message" :value record-json})
          (produce-fn! record-json))))))

(defn make-consume-seq-batch
  "Returns a lazy-seq over all the messages in a consumer, in batches received from poll().
  Items in returned seq are nil if no messages are available, or a seq of messages in a batch."
  [consumer]
  (letfn [(consume-batch [consumer]
            (let [batch (jc/poll consumer (Duration/ofSeconds 5))]
              (if (not (empty? batch))
                (lazy-cat [batch] (consume-batch consumer))
                (lazy-cat [nil] (consume-batch consumer)))))]
    (lazy-seq (consume-batch consumer))))

(defn make-consume-fn-batch
  "Returns a function that yields the next unpolled message from the consumer on each call.
  Retrieves no messages until first invoked. Commits consumer offsets after all the messages
  in a polled batch have been yielded to the caller."
  [consumer]
  (let [message-seq (atom (make-consume-seq-batch consumer))]
    (letfn [(consume-fn []
              (let [m (first @message-seq)]
                (reset! message-seq (rest @message-seq))
                m))]
      (partial consume-fn))))

(def run-streaming-mode-continue (atom true))
(def run-streaming-mode-is-running (atom false))

;(defn run-streaming-mode-oneatatime
;  "Runs the streaming mode of the combiner application. Reads messages by calling consume-fn
;  with no args, stores messages locally, and sends appropriate output messages by calling
;  produce-fn with a single message arg. Runs while run-streaming-mode-continue is truthy."
;  [consume-fn! produce-fn!]
;  (reset! run-streaming-mode-is-running true)
;  (log/info {:fn :run-streaming-mode})
;  (while @run-streaming-mode-continue
;    (log/info "Checking for next batch")
;    (let [batch (consume-fn!)]
;      (if (not (empty? batch))
;        (doseq [rec batch]
;          (do
;            ;(log/debug {:rec rec})
;            (let [k (:key rec) v (:value rec)
;                  offset (:offset rec)
;                  partition-idx (:partition rec)
;                  topic-name (:topic-name rec)
;                  parsed-value (json/parse-string v true)]
;              (sink/store-message parsed-value)
;              (if (= "release_sentinel" (get-in parsed-value [:content :entity_type]))
;                (do (process-release-sentinel parsed-value produce-fn!)
;                    ; Mark entire database as clean. Look into whether this is the best way to do this.
;                    ; If failure occurs part-way through processing one release's batch of messages, the
;                    ; part sent will be sent again. Should be okay.
;                    (if (= "end" (:sentinel_type parsed-value))
;                      (mark-database-clean!))))
;              ; Update offset in db to offset + 1 (next offset to read)
;              (db-client/update-offset topic-name partition-idx (inc offset)))))
;        (log/info {:fn :run-streaming-mode :msg "No new messages"}))))
;  (reset! run-streaming-mode-is-running false)
;  (log/info {:fn :run-streaming-mode :msg "Exiting streaming mode"}))

(defn max-key-in
  "Given a seq of maps, return the map with the maximum value of the key given.
  Maximum is determined with < operator.
  If multiple maps have the max, returns the first in the seq.
  If s is empty? returns nil"
  [s key]
  (if (not (empty? s))
    (reduce (fn
              ([m1 m2]
               (if (< (get m1 key) (get m2 key))
                 m2 m1)))
            s)))

(defn max-offsets-for-partitions
  "Given a seq of jackdaw datafied Kafka records, return the max offset for
  each topic partition represented in the seq. Returns a seq of maps of form:
  {:topic-name ... :partition ... :offset ...}"
  [message-seq]
  (->> message-seq
       (sort-by #(vector (:topic-name %)
                         (:partition %)))
       ;(fn [%] (pprint %) %)
       (partition-by #(vector (:topic-name %)
                              (:partition %)))
       ;(fn [%] (pprint %) %)
       (mapv (fn [part] {:topic-name (:topic-name (first part))
                         :partition (:partition (first part))
                         :offset (:offset (max-key-in part :offset))}))
       ;(fn [%] (pprint %) %)
       ))


(defn run-streaming-mode
  "Runs the streaming mode of the combiner application. Reads messages by calling consume-fn
  with no args, stores messages locally, and sends appropriate output messages by calling
  produce-fn with a single message arg. Runs while run-streaming-mode-continue is truthy."
  [consume-fn! produce-fn!]
  (reset! run-streaming-mode-is-running true)
  (log/info {:fn :run-streaming-mode})
  (while @run-streaming-mode-continue
    (log/info "Checking for next batch")
    (let [batch (consume-fn!)
          ; Partition into sub-batches separated by release sentinels
          batch-with-values (map #(assoc % :value (json/parse-string (:value %) true)) batch)

          sub-batches (partition-by #(= "release_sentinel"
                                        (get-in % [:value :content :entity_type]))
                                    batch-with-values)]
      (doseq [sub-batch sub-batches]
        (if (not (empty? sub-batch))
          (let [sub-batch-values (map #(:value %) sub-batch)]
            (if (= "release_sentinel"
                   (get-in (first sub-batch-values) [:content :entity_type]))
              ; Process release sentinel(s)
              (doseq [sentinel-message sub-batch-values]
                (log/info {:fn :run-streaming-mode :msg "Processing release sentinel" :release_sentinel sentinel-message})
                (process-release-sentinel sentinel-message produce-fn!)
                ; Mark entire database as clean. Look into whether this is the best way to do this.
                ; If failure occurs part-way through processing one release's batch of messages, the
                ; part sent will be sent again. Should be okay.
                (when (= "end" (get-in sentinel-message [:content :sentinel_type]))
                  (mark-database-clean!)))
              ; Process non-release-sentinel sequence of messages
              (let [_ (log/info {:sub-batch-values sub-batch-values})
                    insert-ops (flatten (map #(sink/make-message-insert-op %) sub-batch-values))
                    ; TODO Partition first up to any release sentinels that exist
                    partitioned-insert-ops (sink/merge-ops insert-ops)]
                (doseq [partitioned-insert-op partitioned-insert-ops]
                  (log/debug {:fn :run-streaming-mode :partitioned-insert-op partitioned-insert-op})
                  (with-open [conn (jdbc/get-connection @db-client/db)]
                    (let [sql (:sql partitioned-insert-op)
                          values (:parameter-values partitioned-insert-op)
                          pstmt (sink/parameterize-op-statement conn sql values)]
                      (let [update-count (.executeUpdate pstmt)]
                        (log/info "Executed update, row count: " (str update-count)))
                      )))))
            (log/info {:fn :run-streaming-mode :msg "Updating local topic offsets"})
            (let [to-int (fn [a] (if (int? a) a (Integer/parseInt a)))
                  max-offsets (max-offsets-for-partitions batch-with-values)]
              (doseq [partition-offset max-offsets]

                (let [t (:topic-name partition-offset)
                      p (:partition partition-offset)
                      o (inc (:offset partition-offset))]
                  (log/info {:fn :run-streaming-mode
                             :msg "Updating local offset for partition"
                             :topic-name t :partition p :offset o})
                  (db-client/update-offset t p o)))
              )
            )

          ;(doseq [rec batch]
          ;  (do
          ;    ;(log/debug {:rec rec})
          ;    (let [k (:key rec) v (:value rec)
          ;          offset (:offset rec)
          ;          partition-idx (:partition rec)
          ;          topic-name (:topic-name rec)
          ;          parsed-value (json/parse-string v true)]
          ;      (sink/store-message parsed-value)
          ;      (if (= "release_sentinel" (get-in parsed-value [:content :entity_type]))
          ;        (do (process-release-sentinel parsed-value produce-fn!)
          ;            ; Mark entire database as clean. Look into whether this is the best way to do this.
          ;            ; If failure occurs part-way through processing one release's batch of messages, the
          ;            ; part sent will be sent again. Should be okay.
          ;            (if (= "end" (:sentinel_type parsed-value))
          ;              (mark-database-clean!))))
          ;      ; Update offset in db to offset + 1 (next offset to read)
          ;      (db-client/update-offset topic-name partition-idx (inc offset)))))

          (log/info {:fn :run-streaming-mode :msg "No new messages"})))))
  (reset! run-streaming-mode-is-running false)
  (log/info {:fn :run-streaming-mode :msg "Exiting streaming mode"}))
