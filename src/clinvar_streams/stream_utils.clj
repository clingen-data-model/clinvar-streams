(ns clinvar-streams.stream-utils
  (:require [clinvar-streams.util :as util]
            [clinvar-streams.config :as config]
            [clinvar-streams.storage.database-sqlite.sink :as sink]
            [clinvar-streams.storage.database-sqlite.client :as db-client]
            [jackdaw.serdes :as j-serde]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.java.jdbc :as jdbc]
            [jackdaw.client :as jc]
            [jackdaw.data :as jd])
  (:import [org.apache.kafka.streams KafkaStreams]
           [java.util Properties]
           [java.time Duration]
           [org.apache.kafka.common TopicPartition]
           [java.util UUID Date])
  (:gen-class))


(defn app-config []
  {:kafka-host "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
   :kafka-user (util/get-env-required "KAFKA_USER")
   :kafka-password (util/get-env-required "KAFKA_PASSWORD")
   :kafka-group (util/get-env-required "KAFKA_GROUP")
   })




(defn get-max-offset [topic-name partition-num]
  (let [consumer (jc/consumer (config/kafka-config (assoc (app-config) :kafka-group (.toString (UUID/randomUUID)))))]
    (jc/subscribe consumer [{:topic-name topic-name}])
    (jc/seek-to-end-eager consumer)
    (jc/position consumer (TopicPartition. topic-name partition-num))))

(defn get-min-offset [topic-name partition-num]
  (let [consumer (jc/consumer (config/kafka-config (assoc (app-config) :kafka-group (.toString (UUID/randomUUID)))))]
    (jc/subscribe consumer [{:topic-name topic-name}])
    (jc/seek-to-beginning-eager consumer)
    (jc/position consumer (TopicPartition. topic-name partition-num))))

(defn get-all-messages [topic-name partition-num]
  (let [consumer (jc/consumer (config/kafka-config (assoc (app-config) :kafka-group (.toString (UUID/randomUUID)))))]
    (jc/subscribe consumer [{:topic-name topic-name}])
    (jc/seek-to-beginning-eager consumer)
    (let [min-offset (get-min-offset topic-name partition-num)
          max-offset (get-max-offset topic-name partition-num)
          num-msgs (- max-offset min-offset)]
      (printf "min: %s, max: %s\n" min-offset max-offset)
      (loop [msgs []
             c 0]
        (do
          (printf "%s/%s\n" c num-msgs)
          (if (<= num-msgs c)
            msgs
            (let [batch (jc/poll consumer (Duration/ofMillis 1000))]
              (recur (concat msgs batch)
                     (+ c (count batch)))
              )))))))
; TODO
(defn topic-exists? [topic-name]
  "TODO"
  (let [consumer ()]))

(defn -download-topic-kv
  "Downloads messages from topic and saves them newline delimited in file-name.
  Wipes prior contents of file-name."
  [topic-name file-name]

  (let [download-writer (io/writer file-name)
        running (atom true)
        max-offset (get-max-offset topic-name 0)]
    (with-open [consumer (jc/consumer (config/kafka-config config/app-config))]
      ;(jc/subscribe consumer [{:topic-name topic-name}])
      (jc/assign-all consumer [topic-name])
      (jc/seek-to-beginning-eager consumer)
      (while @running
        (let [msgs (jc/poll consumer (Duration/ofSeconds 5))]
          (if (= 0 (count msgs))
            (do (.flush download-writer)
                (reset! running false))
            (do (log/info (format "Got %d messages" (count msgs)))
                (doseq [msg msgs]
                  (.write download-writer
                          (str (json/generate-string
                                 (select-keys msg [:key :value :offset :topic-name :partition]))
                               "\n"))))))))))

(defn -upload-topic
  [topic-name file-name]
  (let []
    (log/info "Reading file" file-name)
    (with-open [file-rdr (io/reader file-name)
                ;consumer (jc/consumer (config/kafka-config config/app-config))
                producer (jc/producer (config/kafka-config config/app-config))]
      (let [file-lines (line-seq file-rdr)]
        (doseq [line file-lines]
          (let [j (json/parse-string line true)
                msg (assoc (select-keys j [:key :value :offset :topic-name :partition])
                      :topic-name topic-name)]
            (log/infof "Producing to %s key=%s value=%s" topic-name (:key msg) (:value msg))
            (jc/send! producer (jd/map->ProducerRecord (dissoc msg :offset)))
            )))
      )))
