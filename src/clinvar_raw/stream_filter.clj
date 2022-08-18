(ns clinvar-raw.stream-filter
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as cfg]
            [clinvar-streams.util :refer [select-keys-nested]]
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


(defn -main [& args]
  (let [app-config (cfg/app-config)
        kafka-opts (-> (cfg/kafka-config app-config)
                       (assoc "max.poll.records" "1000"))
        consumer (jc/consumer kafka-opts)
        looping? (atom true)
        output-filename (str (:kafka-consumer-topic app-config) "-filtered.txt")
        variation-ids (-> "vcepvars-220801.txt" io/reader line-seq set)
        max-empty-batches 10
        empty-batch-count (atom 0)]
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic app-config)}])
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
                         (select-keys-nested [:release_date
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
                                    (filter #(predicate-variation-id-in % variation-ids)))]
                  (log/infof "Writing %d filtered records" (count filtered))
                  (dorun (map #(do (.write writer %)
                                   (.write writer "\n"))
                              (map json/generate-string filtered)))))))))))
