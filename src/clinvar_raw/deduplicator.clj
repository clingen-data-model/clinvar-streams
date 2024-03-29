(ns clinvar-raw.deduplicator
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as config]
            [clinvar-raw.ingest :as ingest]
            [clinvar-raw.stream :as stream]
            [clinvar-streams.storage.rocksdb :as rocksdb]
            [clinvar-streams.util :refer [gzip-file-reader
                                          gzip-file-writer]]
            [clojure.java.io :as io]
            [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [taoensso.timbre :as log])
  (:import (java.time Duration)))

(defn send-message
  "Sends a message to the producer on `topic`, with the message key `key`, and payload `data`
  `data` can be a string or json-serializable object like a map"
  [producer topic {:keys [key value]}]
  (log/tracef "Sending message with key %s to topic %s" key topic)
  (->> (if (string? value) value (json/generate-string value))
       (jd/->ProducerRecord {:topic-name topic} key)
       (jc/send! producer)))

(defn -main [& args]
  (let [kafka-opts (-> (config/kafka-config config/env-config)
                       (assoc "max.poll.records" "1000"))
        output-topic (:DX_CV_RAW_OUTPUT_TOPIC config/env-config)
        running? (atom true)
        input-counter (atom 0)
        output-counter (atom 0)]
    (rocksdb/close (rocksdb/open "deduplicator.db"))
    (rocksdb/rocks-destroy! "deduplicator.db")
    (with-open [consumer (jc/consumer kafka-opts)
                producer (jc/producer kafka-opts)
                db (rocksdb/open "deduplicator.db")]
      (jc/subscribe consumer [{:topic-name (:DX_CV_RAW_INPUT_TOPIC config/env-config)}])
      (when (:KAFKA_RESET_CONSUMER_OFFSET config/env-config)
        (log/info "Resetting to start of input topic")
        (jc/seek-to-beginning-eager consumer))
      (while @running?
        (let [batch (jc/poll consumer (Duration/ofSeconds 5))]
          (when (seq batch)
            (swap! input-counter #(+ % (count batch)))
            (let [deduplicated-batch (filter
                                      ;; TODO store-new
                                      #(not (ingest/duplicate? db %))
                                      batch)]
              (swap! output-counter #(+ % (count deduplicated-batch)))
              (doseq [m deduplicated-batch]
                (send-message producer output-topic m))
              (log/infof {:input-counter @input-counter
                          :output-counter @output-counter}))))))))

(defn dedup-seq
  "Do not use, does not handle dangling file handle"
  [s]
  (let [db (rocksdb/open "dedup-seq.db")]
    (letfn [(pred [e] (when (not (ingest/duplicate? db e))
                        (ingest/store-new! db e)
                        true))]
      (filter pred s))))

(defn json-parse-key-in
  "Replaces key K in map M with the json parsed value of K in M"
  [m ks]
  (update-in m ks #(json/parse-string % true)))

(defn json-unparse-key-in
  [m ks]
  (update-in m ks #(json/generate-string %)))

(defn make-reader [file-name]
  (if (.endsWith file-name ".gz")
    (gzip-file-reader file-name)
    (io/reader file-name)))

(defn make-writer [file-name]
  (if (.endsWith file-name ".gz")
    (gzip-file-writer file-name)
    (io/writer file-name)))

(defn flatten-one-level [things]
  (for [coll things e coll] e))

(defn -deduplicate-file
  "Reads event JSON lines from INPUT-FILENAME. Deduplicates and writes
   them to OUTPUT-FILENAME."
  [input-filename output-filename]
  (with-open [reader (make-reader input-filename)
              writer (make-writer output-filename)]
    (let [db-path "deduplicate-file.db"]
      (rocksdb/close (rocksdb/open db-path))
      (rocksdb/rocks-destroy! db-path)
      (let [input-counter (atom (bigint 0))
            output-counter (atom (bigint 0))
            create-to-update-counter (atom (bigint 0))
            db (rocksdb/open db-path)]
        (doseq [[out-i out-m]
                (->> (line-seq reader)
                     (map #(json/parse-string % true))
                     (map #(json-parse-key-in % [:content :content]))
                     (map #(merge {:value %}))
                     (#(stream/dedup-clinvar-raw-seq db % {:input-counter input-counter
                                                           :output-counter output-counter}))
                     (map-indexed vector))]
          (log/info :out-i out-i :release_date (:release_date out-m))
          (let [;; Put the nested content back in a string
                out-m (json-unparse-key-in out-m [:content :content])]
            (.write writer (json/generate-string out-m))
            (.write writer "\n")))
        (log/info {:input-counter @input-counter
                   :output-counter @output-counter
                   :create-to-update-counter @create-to-update-counter})))))
