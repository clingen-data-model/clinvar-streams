(ns clinvar-raw.stream-filter
  (:require [cheshire.core :as json]
            [clinvar-raw.config :as cfg]
            [clojure.java.io :as io]
            [jackdaw.client :as jc]
            [taoensso.timbre :as log])
  (:import (java.time Duration)))

#_(defn topic-lazyseq
    [{topic-name :topic-name
      terminate-at-end :terminate-at-end}])

(defn predicate-variation-id-in
  [msg variation-ids]
  (let [id-set variation-ids ;(set (map str variation-ids))
        entity-type (-> msg :content :entity_type)]
    (or (and (= "variation" entity-type)
             (contains? id-set (-> msg :content :id str)))
        #_(and (= "clinical_assertion" entity-type)
               (contains? id-set (-> msg :content :variation_id str)))
        #_(and (= "variation_archive" entity-type)
               (contains? id-set (-> msg :content :variation_id str))))))

(defn -main [& args]
  (let [app-config (cfg/app-config)
        kafka-opts (cfg/kafka-config app-config)
        consumer (jc/consumer kafka-opts)
        looping? (atom true)
        output-filename (str (:kafka-consumer-topic app-config) "-filtered.txt")
        variation-ids (-> "vcepvars-220801.txt" io/reader line-seq set)]
    (jc/subscribe consumer [{:topic-name (:kafka-consumer-topic app-config)}])
    (jc/poll consumer 0)
    (jc/seek-to-beginning-eager consumer)
    (with-open [writer (io/writer output-filename)]
      (while @looping?
        (let [batch (jc/poll consumer (Duration/ofSeconds 5))]
          (log/infof "Read %d records. First record: %s"
                     (count batch)
                     (json/generate-string (-> batch first :value)))
          (when (empty? batch)
            (reset! looping? false))
          (let [filtered (->> batch
                              (map :value)
                              (map #(json/parse-string % true))
                              (filter #(predicate-variation-id-in % variation-ids)))]
            (log/infof "Writing %d filtered records" (count filtered))
            (dorun (map #(do (.write writer %)
                             (.write writer "\n"))
                        (map json/generate-string filtered)))))))))
