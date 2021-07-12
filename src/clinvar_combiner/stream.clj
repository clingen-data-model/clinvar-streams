(ns clinvar-combiner.stream
  (:require [clinvar-combiner.config :as config
             :refer [topic-metadata]]
            [jackdaw.client :as jc]
            [clinvar-combiner.combiners.clinical-assertion :as c-assertion]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clinvar-streams.storage.database-sqlite.sink :as sink]
            [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clojure.java.io :as io]
            [clinvar-combiner.combiners.core :as c-core]
            [clojure.java.jdbc :as jdbc])
  (:import (java.time Duration)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.clients.consumer KafkaConsumer)))

(defn topic-partitions
  "Returns a seq of TopicPartitions that the consumer is subscribed to for topic-name."
  [consumer topic-name]
  (let [partition-infos (.partitionsFor consumer topic-name)]
    (map #(TopicPartition. (.topic %) (.partition %)) partition-infos)))

(defn make-consume-seq-batch
  "Returns a lazy-seq over all the messages in a consumer, in batches received from poll()"
  [consumer]
  (letfn [(consume-batch [consumer]
            (try (.commitSync ^KafkaConsumer consumer)
                 (catch InterruptedException e
                   (log/error "Got InterruptedException in commitSync():" e)))
            (let [batch (try (jc/poll consumer (Duration/ofSeconds 5))
                             (catch InterruptedException e
                               (log/info "InterruptedException in poll() call:" e)))]
              (if (not (empty? batch))
                (lazy-cat [batch] (consume-batch consumer))
                (lazy-cat [nil] (consume-batch consumer)))
              ))]
    (partial consume-batch consumer)) ; TODO
  )

(defn make-consume-fn-batch
  [consumer]
  (let [consumer-seq (atom ((make-consume-seq-batch consumer)))]
    (letfn [(consume-fn []
              (let [m (first @consumer-seq)]
                ; rest is lazy
                (reset! consumer-seq (rest @consumer-seq))
                m))]
      (partial consume-fn))))

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
                (lazy-cat [nil] (consume-batch consumer)))
              ))]
    (partial consume-batch consumer)))

(defn make-consume-fn
  "Given a consumer, returns a function which on each invocation
  returns the next message in the stream, and closes when there are no more."
  [consumer]
  (let [consumer-seq (atom ((make-consume-seq consumer)))]
    (letfn [(consume-fn []
              (let [m (first @consumer-seq)]
                ; rest is lazy
                (reset! consumer-seq (rest @consumer-seq))
                m))]
      (partial consume-fn))))

(defn make-produce-fn
  "Given a producer, returns a function which produces a single message argument
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
        (doseq [record (c-core/get-dirty release-sentinel)]
          (let [record-json (json/generate-string record)]
            (log/info record-json)
            (produce-fn! record-json)))

        ; Flush SCVs
        (let [scvs-to-flush (c-assertion/dirty-bubble-scv release-sentinel)]
          (log/info release-sentinel)
          (log/infof "Received %d messages to flush" (count scvs-to-flush))
          (doseq [clinical-assertion scvs-to-flush]
            (let [built-clinical-assertion (c-assertion/build-clinical-assertion clinical-assertion)
                  built-clinical-assertion (c-assertion/post-process-built-clinical-assertion built-clinical-assertion)
                  built-clinical-assertion-json (json/generate-string built-clinical-assertion)]
              (if (nil? (:id built-clinical-assertion))
                (throw (ex-info "assertion :id cannot be nil"
                                {:cause built-clinical-assertion-json})))
              (if (nil? (:release_date built-clinical-assertion))
                (throw (ex-info "assertion :release_date cannot be nil"
                                {:cause built-clinical-assertion-json})))
              (let [fpath (format "debug/SCV/%s/%s.json"
                                  (:release_date built-clinical-assertion)
                                  (:id built-clinical-assertion))]
                (io/make-parents fpath)
                (with-open [fwriter (io/writer fpath)]
                  (.write fwriter built-clinical-assertion-json)))
              (log/info built-clinical-assertion-json)
              ; Write message to output
              (produce-fn! built-clinical-assertion-json))))

        ; Write end release sentinel to output
        (let [record-json (json/generate-string release-sentinel)]
          (produce-fn! record-json))))))

(def run-streaming-mode-continue (atom true))
(def run-streaming-mode-is-running (atom false))

(defn run-streaming-mode
  "Runs the streaming mode of the combiner application. Reads messages by calling consume-fn
  with no args, stores messages locally, and sends appropriate output messages by calling
  produce-fn with a single message arg. Runs while run-streaming-mode-continue is truthy."
  [consume-fn produce-fn]
  (reset! run-streaming-mode-is-running true)
  (log/info {:fn :run-streaming-mode})
  (while @run-streaming-mode-continue
    (let [rec (consume-fn)]
      (if rec
        (do (log/info rec)
            (let [k (:key rec) v (:value rec)
                  offset (:offset rec)
                  partition-idx (:partition rec)
                  topic-name (:topic-name rec)
                  parsed-value (json/parse-string v true)]
              (sink/store-message parsed-value)
              (if (= "release_sentinel" (get-in parsed-value [:content :entity_type]))
                (do (process-release-sentinel parsed-value produce-fn)
                    ; Mark entire database as clean. Look into whether this is the best way to do this.
                    ; If failure occurs part-way through processing one release's batch of messages, the
                    ; part sent will be sent again. Should be okay.
                    (mark-database-clean!)))
              ; Update offset in db to offset + 1 (next offset to read)
              (db-client/update-offset topic-name partition-idx (inc offset))))
        (log/info "No new messages"))))
  (log/info {:fn :run-streaming-mode :msg "Exiting streaming mode"})
  (reset! run-streaming-mode-is-running false))
