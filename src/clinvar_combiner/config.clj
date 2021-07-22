(ns clinvar-combiner.config
  (:require [clinvar-streams.util :as util]
            [jackdaw.serdes :as j-serde]))

(def sqlite-db (when-not *compile-files* (util/get-env-required "SQLITE_DB")))
(def snapshot-bucket (when-not *compile-files* (util/get-env-required "DX_CV_SNAPSHOT_BUCKET")))
(def version-to-resume-from (System/getenv "DX_CV_COMBINER_SNAPSHOT_VERSION"))

(defn app-config []
  {:kafka-host "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
   :kafka-user (util/get-env-required "KAFKA_USER")
   :kafka-password (util/get-env-required "KAFKA_PASSWORD")
   :kafka-group (util/get-env-required "KAFKA_GROUP")
   })

(def topic-metadata
  {:input
   {;:topic-name "clinvar-raw"
    :topic-name "clinvar-raw-testdata_20210302"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}
   :output
   {;:topic-name         "clinvar-combined"
    :topic-name "clinvar-combined-testdata_20210302"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}})

(defn kafka-config
  "Expects, at a minimum, :kafka-user and :kafka-password in opts. "
  [opts]
  {"ssl.endpoint.identification.algorithm" "https"
   "enable.auto.commit" "false"
   "compression.type" "gzip"
   "sasl.mechanism" "PLAIN"
   "request.timeout.ms" "20000"
   "application.id" (:kafka-group opts)
   "group.id" (:kafka-group opts)
   "bootstrap.servers" (:kafka-host opts)
   "retry.backoff.ms" "500"
   "security.protocol" "SASL_SSL"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "sasl.jaas.config" (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                           (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")})
