(ns clinvar-raw.deduplicator
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as cfg]
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
  (jc/send! producer (jd/->ProducerRecord {:topic-name topic}
                                          key
                                          (if (string? value) value (json/generate-string value)))))

(defn -main [& args]
  (let [opts (cfg/app-config)
        kafka-opts (-> (cfg/kafka-config opts)
                       (assoc "max.poll.records" "1000"))
        output-topic (:kafka-producer-topic opts)
        running? (atom true)
        input-counter (atom 0)
        output-counter (atom 0)]
    (with-open [consumer (jc/consumer kafka-opts)
                producer (jc/producer kafka-opts)]
      (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic opts)}])
      (when (:kafka-reset-consumer-offset opts)
        (log/info "Resetting to start of input topic")
        (jc/seek-to-beginning-eager consumer))
      (while @running?
        (let [batch (jc/poll consumer (Duration/ofSeconds 5))]
          (when (seq batch)
            (swap! input-counter #(+ % (count batch)))
            (let [deduplicated-batch (filter
                                      ;; TODO store-new
                                      #(not (ingest/duplicate? %))
                                      batch)]
              (swap! output-counter #(+ % (count deduplicated-batch)))
              (doseq [m deduplicated-batch]
                (send-message producer output-topic m))
              (log/infof {:input-counter @input-counter
                          :output-counter @output-counter}))))))))

(defn dedup-seq [s]
  (let [db (rocksdb/open "dedup-seq.db")]
    (letfn [(seq-with-db [s db]
              (lazy-cat s (do (.close db) nil)))
            (pred [e] (when (not (ingest/duplicate? db e))
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

(defn reset-db! [db-name]
  (rocksdb/close db-name))

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
    (let [input-counter (atom (bigint 0))
          output-counter (atom (bigint 0))
          create-to-update-counter (atom (bigint 0))
          db-path "deduplicate-file.db"
          _ (rocksdb/close (rocksdb/open db-path))
          _ (rocksdb/rocks-destroy! db-path)
          db (rocksdb/open db-path)]

      (doseq [[out-i out-m]
              (->> (line-seq reader)
                   (map #(json/parse-string % true))
                   (map #(json-parse-key-in % [:content :content]))
                   (partition-by #(vector [(-> % :release_date)
                                           (-> % :content :entity_type)]))
                   (map (fn [batch]
                          (pmap (fn [value]
                                  (swap! input-counter inc)
                                  (let [mdup? (ingest/duplicate? db value)]
                                    (when (= :create-to-update mdup?)
                                      (swap! create-to-update-counter inc))
                                    (ingest/store-new! db value)
                                    (when (not mdup?)
                                      (swap! output-counter inc)
                                      value)))
                                batch)))
                   flatten-one-level
                   (filter #(not (nil? %)))
                   (map-indexed vector))]
        (log/info :out-i out-i :release_date (:release_date out-m))
        (let [;; Put the nested content back in a string, discard the offset/value wrapper
              out-m (json-unparse-key-in out-m [:content :content])]
          (.write writer (json/generate-string out-m))
          (.write writer "\n")))
      (log/info {:input-counter @input-counter
                 :output-counter @output-counter
                 :create-to-update-counter @create-to-update-counter}))))


(defn -deduplicate-file2
  "Reads event JSON lines from INPUT-FILENAME. Deduplicates and writes
   them to OUTPUT-FILENAME."
  [input-filename output-filename]
  (with-open [reader (make-reader input-filename)
              writer (make-writer output-filename)]
    (let [input-counter (atom (bigint 0))
          output-counter (atom (bigint 0))
          create-to-update-counter (atom (bigint 0))
          db-path "deduplicate-file.db"
          _ (rocksdb/close (rocksdb/open db-path))
          _ (rocksdb/rocks-destroy! db-path)
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
        (let [;; Put the nested content back in a string, discard the offset/value wrapper
              out-m (json-unparse-key-in out-m [:content :content])]
          (.write writer (json/generate-string out-m))
          (.write writer "\n")))
      (log/info {:input-counter @input-counter
                 :output-counter @output-counter
                 :create-to-update-counter @create-to-update-counter}))))
