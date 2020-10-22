(ns clinvar-qc.config
  (:require [taoensso.timbre :as timbre]
            [jackdaw.serdes :as j-serde])
  (:import java.lang.System)
  (:gen-class))

(timbre/set-level! :debug)

(def topic-metadata
  {:input
   {:topic-name         "clinvar-raw"
    :partition-count    1
    :replication-factor 1
    :key-serde          (j-serde/string-serde)
    :value-serde        (j-serde/string-serde)}
   :output
   {:topic-name         "clinvar-qc"
    :partition-count    1
    :replication-factor 1
    :key-serde          (j-serde/string-serde)
    :value-serde        (j-serde/string-serde)}})

(def app-config
  {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
   :kafka-user     (System/getenv "KAFKA_USER")
   :kafka-password (System/getenv "KAFKA_PASSWORD")
   :kafka-group    (or (System/getenv "KAFKA_GROUP") "clinvar-qc-dev")
   :db-password    (System/getenv "CLINVAR_DB_PASSWORD")
   :db-user        (System/getenv "CLINVAR_DB_USER")
   :db-host        (or (System/getenv "CLINVAR_DB_HOST") "localhost")
   })

(defn kafka-config
  "Expects, at a minimum, :kafka-user and :kafka-password in opts. "
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
   "application.id"                        (:kafka-group opts)
   "client.id"                             (:kafka-group opts)
   "group.id"                              (:kafka-group opts)
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                                (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")})

(def date-format "YYYY-MM-DD'T'HH:mm:ss:Z")
