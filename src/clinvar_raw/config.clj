(ns clinvar-raw.config
  (:require [taoensso.timbre :as timbre])
  (:import java.lang.System)
  (:gen-class))

(timbre/set-level! :debug)

(defn app-config
  []
  {:kafka-host                  "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
   :kafka-user                  (System/getenv "KAFKA_USER")
   :kafka-password              (System/getenv "KAFKA_PASSWORD")
   :kafka-group                 (or (System/getenv "KAFKA_GROUP") "clinvar-raw")
   :kafka-consumer-topic        "broad-dsp-clinvar"
   :kafka-producer-topic        "clinvar-raw"
   :kafka-reset-consumer-offset (Boolean/valueOf (System/getenv "KAFKA_RESET_CONSUMER_OFFSET"))
   })

(defn kafka-config
  "Expects :kafka-user, :kafka-password, :kafka-host, :kafka-group in opts."
  [opts]
  {"ssl.endpoint.identification.algorithm" "https"
   "compression.type"                      "gzip"
   "sasl.mechanism"                        "PLAIN"
   "request.timeout.ms"                    "20000"
   "bootstrap.servers"                     (:kafka-host opts)
   "retry.backoff.ms"                      "500"
   "security.protocol"                     "SASL_SSL"
   "key.serializer"                        "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                      "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"                    "org.apache.kafka.common.serialization.StringDeserializer"
   "group.id"                              (:kafka-group opts)
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                                (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")})

(def date-format "YYYY-MM-DD'T'HH:mm:ss:Z")
