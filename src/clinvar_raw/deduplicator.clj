(ns clinvar-raw.deduplicator
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as cfg]
            [clinvar-raw.ingest :as ingest]
            [clinvar-streams.util :refer [gzip-file-reader]]
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
  (letfn [(pred [e] (when (not (ingest/duplicate? e))
                      (ingest/store-new! e)
                      true))]
    (filter pred s)))

(defn json-parse-key-in
  "Replaces key K in map M with the json parsed value of K in M"
  [m ks]
  (update-in m ks #(json/parse-string % true)))

(defn json-unparse-key-in
  [m ks]
  (update-in m ks #(json/generate-string %)))

(defn -deduplicate-file
  "Reads event JSON lines from INPUT-FILENAME. Deduplicates and writes
   them to OUTPUT-FILENAME."
  [input-filename output-filename]
  (with-open [reader (gzip-file-reader input-filename)
              writer (io/writer output-filename)]
    (let [input-counter (atom (bigint 0))
          output-counter (atom (bigint 0))
          create-to-update-counter (atom (bigint 0))]
      (doseq [out-m (filter
                     #(not (nil? %))
                     (for [m (-> reader
                                 line-seq
                                 (->> (map #(json/parse-string % true))
                                      (map #(json-parse-key-in % [:value]))
                                      (map #(json-parse-key-in % [:value :content :content]))))]
                       (let [value (:value m)]
                         (do (swap! input-counter inc)
                             (let [mdup? (ingest/duplicate? value)]
                               ;; If its not a duplicate or it is a duplicate but the
                               ;; return value is :create-to-update, persist the value of m
                               (when (= :create-to-update mdup?)
                                 (swap! create-to-update-counter inc))
                               (ingest/store-new! value)
                               (if (not mdup?)
                                 (do (swap! output-counter inc)
                                     m)
                                 ;; A when statement returns nil if not, so maybe don't need this explicit nil return
                                 nil))))))]

        (let [;; Put the nested content back in a string, discard the offset/value wrapper
              out-m (json-unparse-key-in out-m [:content :content])]
          (.write writer (json/generate-string out-m))
          (.write writer "\n")))
      (log/info {:input-counter @input-counter
                 :output-counter @output-counter
                 :create-to-update-counter @create-to-update-counter}))))
