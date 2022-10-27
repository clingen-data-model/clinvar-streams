(ns clinvar-raw.config
  (:require [taoensso.timbre :as timbre]
            [clojure.walk :as walk]))

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
   :kafka-reset-consumer-offset (Boolean/valueOf (System/getenv "KAFKA_RESET_CONSUMER_OFFSET"))
   :data-directory (System/getenv "CLINVAR_STREAMS_DATA_DIR")})

(false? (Boolean/valueOf "false"))

(-> {"KAFKA_BROKER" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"}
    (merge (System/getenv))
    (select-keys ["KAFKA_BROKER"
                  "KAFKA_USER"
                  "KAFKA_PASSWORD"
                  "KAFKA_GROUP"
                  "DX_CV_RAW_INPUT_TOPIC"
                  "DX_CV_RAW_OUTPUT_TOPIC"
                  "KAFKA_RESET_CONSUMER_OFFSET"
                  "CLINVAR_STREAMS_DATA_DIR"])
    walk/keywordize-keys)

(def appender
  "Append logs."
  (timbre/spit-appender
   {:fname (str (:data-directory (app-config)) "logs/clinvar-streams.log")}))

(timbre/set-level! :info)
(timbre/swap-config! #(update % :appenders merge {:file appender}))

;; max.poll.records must be 1 to ensure timely offset commits
(defn kafka-config
  "Expects :kafka-user, :kafka-password, :kafka-host, :kafka-group in opts."
  [{:keys [kafka-host kafka-group kafka-user kafka-password] :as opts}]
  (let [common "org.apache.kafka.common."
        serialization (str common "serialization.")
        serializer (str serialization "StringSerializer")
        deserializer (str serialization "StringDeserializer")
        jaas (str common "security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";")
        jaas-config (format jaas kafka-user kafka-password)]
    (cond->
        {"ssl.endpoint.identification.algorithm" "https"
         "compression.type" "gzip"
         "sasl.mechanism" "PLAIN"
         "request.timeout.ms" "20000"
         "bootstrap.servers" kafka-host
         "retry.backoff.ms" "500"
         "security.protocol" "SASL_SSL"
         "key.serializer" serializer
         "value.serializer" serializer
         "key.deserializer" deserializer
         "value.deserializer" deserializer
         "max.poll.interval.ms" (str (* 12 60 60 1000))
         "max.poll.records" "1"
         "sasl.jaas.config" jaas-config}
      kafka-group (assoc "group.id" kafka-group))))
