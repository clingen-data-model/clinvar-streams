(ns clinvar-raw.stream-filter
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as config]
            [clinvar-streams.util :as util]
            [clojure.java.io :as io]
            [jackdaw.client :as jc]
            [taoensso.timbre :as log])
  (:import (java.time Duration)))

#_(defn topic-lazyseq
    [{topic-name :topic-name
      terminate-at-end :terminate-at-end}])

(defn predicate-variation-id-in
  "Takes a clinvar-raw message structure and a set of variation ids.
   Returns true if the message is a variation, SCV, or VCV for one of those variation ids"
  [msg variation-ids]
  (let [id-set variation-ids ;(set (map str variation-ids))
        entity-type (-> msg :content :entity_type)]
    (or (and (= "variation" entity-type)
             (contains? id-set (-> msg :content :id str)))
        (and (= "clinical_assertion" entity-type)
             (contains? id-set (-> msg :content :variation_id str)))
        (and (= "variation_archive" entity-type)
             (contains? id-set (-> msg :content :variation_id str))))))

(defn predicate-entity-type-in
  "Takes a clinvar-raw message and ENTITY-TYPES-SET (a set). If msg :entity_type
   is one of ENTITY-TYPES-SET, return true."
  [msg entity-types-set]
  (contains? entity-types-set (-> msg :content :entity_type)))


(defn topic-download-gzip []
  (let [app-config (assoc config/env-config
                          :DX_CV_RAW_INPUT_TOPIC "clinvar-raw-dedup")
        kafka-opts (-> (config/kafka-config app-config)
                       (assoc "max.poll.records" "10000")
                       ;; 10MiB fetch
                       (assoc "max.partition.fetch.bytes" (int (* 10 1024 1024))))
        looping? (atom true)
        output-filename (str (:DX_CV_RAW_INPUT_TOPIC app-config) ".gz")
        max-empty-batches 5
        empty-batch-count (atom 0)]
    (with-open [consumer (jc/consumer kafka-opts)]
      (jc/subscribe consumer [{:topic-name (:DX_CV_RAW_INPUT_TOPIC app-config)}])
      (jc/poll consumer 0)
      (jc/seek-to-beginning-eager consumer)
      (with-open [writer (util/gzip-file-writer output-filename)]
        (while @looping?
          (let [batch (jc/poll consumer (Duration/ofSeconds 10))]
            (log/infof "Read %s records. First record offset: %s, date: %s"
                       (count batch)
                       (-> batch first :offset)
                       (-> batch first :value
                           (json/parse-string true)
                           (util/select-keys-nested [:release_date
                                                     :event_type
                                                     [:content :entity_type]])
                           json/generate-string))
            (if (empty? batch)
              (if (<= max-empty-batches @empty-batch-count)
                (reset! looping? false)
                (do (log/info "No records returned from poll, sleeping 1 minute")
                    (swap! empty-batch-count inc)
                    (Thread/sleep (* 1000 60))))
              (do (reset! empty-batch-count 0)
                  (let []
                    (log/infof "Writing %d records" (count batch))
                    (dorun (map #(do (.write writer %)
                                     (.write writer "\n"))
                                (->> batch
                                     #_(map #(select-keys % [:offset :value]))
                                     (map :value)
                                     #_(map json/generate-string)))))))))))))

(defn -main [& args]
  (let [kafka-opts (-> (config/kafka-config config/env-config)
                       (assoc "max.poll.records" "1000"))
        looping? (atom true)
        output-filename (str (:DX_CV_RAW_INPUT_TOPIC config/env-config) "-filtered.txt")
        variation-ids (-> "vcepvars-220801.txt" io/reader line-seq set)
        entity-types-include-all (set ["trait" "trait_set" "submitter"])
        max-empty-batches 10
        empty-batch-count (atom 0)]
    (with-open [consumer (jc/consumer kafka-opts)]
      (jc/subscribe consumer [{:topic-name (:DX_CV_RAW_INPUT_TOPIC config/env-config)}])
      (jc/poll consumer 0)
      (jc/seek-to-beginning-eager consumer)
      (with-open [writer (io/writer output-filename)]
        (while @looping?
          (let [batch (jc/poll consumer (Duration/ofSeconds 10))]
            (log/infof "Read %s records. First record offset: %s, date: %s"
                       (count batch)
                       (-> batch first :offset)
                       (-> batch first :value
                           (json/parse-string true)
                           (util/select-keys-nested [:release_date
                                                     :event_type
                                                     [:content :entity_type]])
                           json/generate-string))
            (if (empty? batch)
              (if (<= max-empty-batches @empty-batch-count)
                (reset! looping? false)
                (do (log/info "No records returned from poll, sleeping 1 minute")
                    (swap! empty-batch-count inc)
                    (Thread/sleep (* 1000 60))))
              (do (reset! empty-batch-count 0)
                  (let [filtered (->> batch
                                      (map :value)
                                      (map #(json/parse-string % true))
                                      (filter #(or (predicate-entity-type-in % entity-types-include-all)
                                                   (predicate-variation-id-in % variation-ids))))]
                    (log/infof "Writing %d filtered records" (count filtered))
                    (dorun (map #(do (.write writer %)
                                     (.write writer "\n"))
                                (map json/generate-string filtered))))))))))))


;; TODO write some functions to abstract reading from a KafkaConsumer
;; and reading from a file
(defn -main-file [& args]
  (let [input-filename "clinvar-raw-original.gz"
        output-filename "clinvar-raw-local-filtered-vcepvars.txt"

        variation-ids (-> "vcepvars-220801.txt" io/reader line-seq set)
        entity-types-include-all (set ["trait" "trait_set" "submitter"])]
    (with-open [reader (util/gzip-file-reader input-filename)
                writer (io/writer output-filename)]
      (doseq [line (-> reader
                       line-seq
                       (->> (map #(json/parse-string % true))
                            (map :value)
                            (map #(json/parse-string % true))
                            (filter #(or (predicate-entity-type-in % entity-types-include-all)
                                         (predicate-variation-id-in % variation-ids)))))]
        (do (.write writer (json/generate-string line))
            (.write writer "\n"))))))
