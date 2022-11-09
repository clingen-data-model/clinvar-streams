(ns clinvar-raw.config
  (:require [clojure.walk :as walk]
            [taoensso.timbre :as timbre]))

(timbre/set-level! :info)


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


;;;;;;;;;;;;;;;;;;;


(def env-config
  (let [defaults {"KAFKA_BROKER" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"}]
    (-> (merge defaults (System/getenv))
        (select-keys
         (map name [:KAFKA_BROKER
                    :KAFKA_USER
                    :KAFKA_PASSWORD
                    :KAFKA_GROUP
                    :DX_CV_RAW_INPUT_TOPIC
                    :DX_CV_RAW_OUTPUT_TOPIC
                    :DX_CV_RAW_MAX_INPUT_COUNT
                    :KAFKA_RESET_CONSUMER_OFFSET
                    :CLINVAR_STREAMS_DATA_DIR]))
        walk/keywordize-keys)))

(def appender
  "File appender for timbre."
  (timbre/spit-appender
   {:fname (str (:CLINVAR_STREAMS_DATA_DIR env-config)
                "logs/clinvar-streams.log")}))

(timbre/swap-config!
 #(update % :appenders merge {:file appender}))

;; Poll one record at a time to ensure more timely offset commits.
(defn kafka-config
  "Expects :kafka-user, :kafka-password, :kafka-host, :kafka-group in opts."
  [{:keys [kafka-group kafka-host kafka-password kafka-user] :as opts}]
  (let [common        "org.apache.kafka.common."
        serialization (str common "serialization.")
        serializer    (str serialization "StringSerializer")
        deserializer  (str serialization "StringDeserializer")
        hours12       (str (* 12 60 60 1000))
        jaas-tmpl     (str common "security.plain.PlainLoginModule"
                           " required"
                           " username=\"%s\" password=\"%s\";")
        config        (format jaas-tmpl kafka-user kafka-password)]
    (cond-> {"compression.type"                      "gzip"
             "key.deserializer"                      deserializer
             "key.serializer"                        serializer
             "max.poll.interval.ms"                  hours12
             "max.poll.records"                      "1"
             "request.timeout.ms"                    "20000"
             "retry.backoff.ms"                      "500"
             "sasl.jaas.config"                      config
             "sasl.mechanism"                        "PLAIN"
             "security.protocol"                     "SASL_SSL"
             "ssl.endpoint.identification.algorithm" "https"
             "value.deserializer"                    deserializer
             "value.serializer"                      serializer}
      kafka-host  (assoc "bootstrap.servers" kafka-host)
      kafka-group (assoc "group.id"          kafka-group))))
