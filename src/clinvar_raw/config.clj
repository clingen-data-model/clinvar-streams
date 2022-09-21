(ns clinvar-raw.config
  (:require [taoensso.timbre :as timbre])
  (:import java.lang.System))

(timbre/set-level! :info)
(timbre/swap-config!
 #(update % :appenders merge {:file (timbre/spit-appender {:fname "logs/clinvar-streams.log"})}))


(defn remove-nil-values [m]
  (into {} (filter #(not= nil (second %)) m)))

(defn app-config []
  {:kafka-host (or (System/getenv "KAFKA_BROKER")
                   "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092")
   :kafka-user (System/getenv "KAFKA_USER")
   :kafka-password (System/getenv "KAFKA_PASSWORD")
   :kafka-group (System/getenv "KAFKA_GROUP")
   :kafka-consumer-topic (System/getenv "DX_CV_RAW_INPUT_TOPIC")
   :kafka-producer-topic (System/getenv "DX_CV_RAW_OUTPUT_TOPIC")
   :kafka-reset-consumer-offset (Boolean/valueOf (System/getenv "KAFKA_RESET_CONSUMER_OFFSET"))})

(defn kafka-config
  "Expects :kafka-user, :kafka-password, :kafka-host, :kafka-group in opts."
  [opts]
  (remove-nil-values
   {"ssl.endpoint.identification.algorithm" "https"
    "compression.type" "gzip"
    "sasl.mechanism" "PLAIN"
    "request.timeout.ms" "20000"
    "bootstrap.servers" (:kafka-host opts)
    "retry.backoff.ms" "500"
    "security.protocol" "SASL_SSL"
    "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
    "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
    "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
    "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
    "group.id" (:kafka-group opts)
    ;; 12 hours in milliseconds
    "max.poll.interval.ms" "43200000"
    ;; Poll one record at a time. Ensures more timely offset commits.
    "max.poll.records" "1"
    "sasl.jaas.config" (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")}))

(def date-format "YYYY-MM-DD'T'HH:mm:ss:Z")
