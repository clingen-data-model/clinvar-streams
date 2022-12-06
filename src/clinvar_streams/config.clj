(ns clinvar-streams.config
  (:require [clojure.walk :as walk]
            [jackdaw.serdes :as j-serde]
            [taoensso.timbre :as timbre])
  (:import java.lang.System))


(def topic-metadata
  {:input
   {;:topic-name         "clinvar-raw"
    :topic-name         "clinvar-raw"
    :partition-count    1
    :replication-factor 1
    :key-serde          (j-serde/string-serde)
    :value-serde        (j-serde/string-serde)}
   :output
   {:topic-name         "clinvar-test"
    :partition-count    1
    :replication-factor 1
    :key-serde          (j-serde/string-serde)
    :value-serde        (j-serde/string-serde)}})

;; (defn app-config []
;;   {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
;;    :kafka-user     (System/getenv "KAFKA_USER")
;;    :kafka-password (System/getenv "KAFKA_PASSWORD")
;;    :kafka-group    (System/getenv "KAFKA_GROUP")
;;    :data-directory (System/getenv "CLINVAR_STREAMS_DATA_DIR")
;;    ;:db-password    (System/getenv "CLINVAR_DB_PASSWORD")
;;    ;:db-user        (System/getenv "CLINVAR_DB_USER")
;;    ;:db-host        (or (System/getenv "CLINVAR_DB_HOST") "localhost")
;;    })


(def env-config
  (let [defaults {"KAFKA_BROKER" "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"}]
    (-> (merge defaults (System/getenv))
        (select-keys
         (map name [:KAFKA_BROKER
                    :KAFKA_USER
                    :KAFKA_PASSWORD
                    :KAFKA_GROUP
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

;; Sets max.poll.records to only retrieve one record at a time
;; to ensure more timely offset commits.
(defn kafka-config
  "Uses :KAFKA_USER :KAFKA_PASSWORD, :KAFKA_HOST :KAFKA_GROUP in opts."
  [{:keys [KAFKA_GROUP KAFKA_BROKER KAFKA_PASSWORD KAFKA_USER] :as opts}]
  (let [common        "org.apache.kafka.common."
        serialization (str common "serialization.")
        serializer    (str serialization "StringSerializer")
        deserializer  (str serialization "StringDeserializer")
        hours12       (str (* 12 60 60 1000))
        jaas-tmpl     (str common "security.plain.PlainLoginModule"
                           " required"
                           " username=\"%s\" password=\"%s\";")
        config        (format jaas-tmpl KAFKA_USER KAFKA_PASSWORD)]
    (cond-> {"bootstrap.servers"                     KAFKA_BROKER
             "compression.type"                      "gzip"
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
      KAFKA_GROUP (assoc "group.id"          KAFKA_GROUP))))
