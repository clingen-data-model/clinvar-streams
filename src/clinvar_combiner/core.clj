(ns clinvar-combiner.core
  (:require [clinvar-streams.storage.database-sqlite.sink :as sink]
            [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-combiner.combiners.variation :as c-variation]
            [clinvar-combiner.combiners.clinical-assertion :as c-assertion]
            [clinvar-combiner.combiners.core :as c-core]
            [clinvar-streams.util :as util]
            [jackdaw.streams :as j]
            [jackdaw.serdes :as j-serde]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [jackdaw.client :as jc])
  (:import [org.apache.kafka.streams KafkaStreams]
           [java.util Properties]
           [java.time Duration]
           (org.apache.kafka.common TopicPartition))
  (:gen-class))

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


(defn select-clinical-assertion
  "Return true if the message is a clinical assertion
  otherwise return nil"
  [[key v]]
  ;  (log/debug "in select-clinical-assertion " key (get-in (json/parse-string v true) [:content :entity_type]))
  (= "clinical_assertion" (get-in (json/parse-string v true) [:content :entity_type])))

(defn is-release-sentinel
  "Return true if the message is a release sentinel
  otherwise return nil"
  [[^String key ^String v]]
  ;  (log/debug "in select-clinical-assertion " key (get-in (json/parse-string v true) [:content :entity_type]))
  (= "release_sentinel" (get-in (json/parse-string v true) [:content :entity_type])))

(defn join-safe
  [delim vec]
  (loop [v vec s ""]
    (if (empty? v) s
                   (recur (rest v)
                          (if (empty? s) (str (first v)) (str s delim (first v)))))))

(defn join-non-empty
  "Join only the non-empty terms of v with delim between each pair"
  [delim vec]
  (join-safe delim (filter #(not (empty? %)) vec)))

(def previous-entity-type (atom ""))

(defn to-db
  ""
  [[^String key ^String val]]
  ; Put each message in rocksdb
  (let [val-map (json/parse-string val true)]
    ; Log entity-type changes for monitoring stream
    ;(if (not (= @previous-entity-type (-> val-map :content :entity_type)))
    ;  (do
    ;    (log/debugf "entity-type changed from %s to %s\n" @previous-entity-type (-> val-map :content :entity_type))
    ;    (reset! previous-entity-type (-> val-map :content :entity_type))))

    ;(log/info "storing")
    (sink/store-message val-map))
  [key val])


;(defn topology [builder in-topic out-topic]
;  "Builds a topology of operations to apply to a kstream from builder.
;  Statefully applies the topology to builder, return value unused."
;  (-> (j/kstream builder in-topic)
;      ; Stash message in rocksdb
;      (j/peek to-db)
;      ; Filter to :entity_type release_sentinel
;      (j/filter is-release-sentinel)
;      (j/peek (fn [[k v]] (let [release-sentinel (json/parse-string v true)]
;                            (sink/dirty-bubble release-sentinel))))
;      ; TODO temporary filter for debugging
;      ;(j/filter (fn [[k v]] (= "SCV000335826" (-> (json/parse-string v true) :content :id))))
;      ; Transform clinical assertion using stored data
;      ;(j/map build-clinical-assertion)
;      ;(j/to out-topic)
;      )
;  )

(defn write-map-to-file
  [m filename]
  (with-open [writer (io/writer filename)]
    (doseq [[k v] m]
      (.write writer (str k "=" v "\n")))))

(defn -main
  "Construct topology and start kafka streams application"
  [& args]
  (write-map-to-file (kafka-config (app-config)) "kafka.properties")
  (log/set-level! :debug)
  (db-client/init! "clinvar.sqlite3")

  (let [consumer (jc/consumer (kafka-config (app-config)))
        producer (jc/producer (kafka-config (app-config)))
        topic-name (get-in topic-metadata [:input :topic-name])
        continue (atom true)
        poll-interval-millis 5000]
    (println (:input topic-metadata))
    (log/info "Subscribing to topic" topic-name)
    (jc/subscribe consumer [(:input topic-metadata)])

    (log/info "Seeking to beginning of input topic")
    ;(jc/seek-to-beginning-eager consumer)
    (jc/poll consumer (Duration/ofSeconds 5))
    (jc/seek consumer (TopicPartition. topic-name 0) 0)

    (log/info "Polling for messages")
    (while @continue
      (let [msgs (jc/poll consumer (Duration/ofMillis poll-interval-millis))]
        (log/info "Poll loop")
        (doseq [msg msgs]
          (log/info msg)
          (let [k (:key msg) v (:value msg)
                kv [k v]]
            (to-db kv)
            (if (is-release-sentinel kv)
              (let [release-sentinel (json/parse-string (second kv) true)
                    sentinel-type (get-in release-sentinel [:content :sentinel_type])]
                (cond (= "start" sentinel-type)
                      (do (let [record-json (json/generate-string release-sentinel)]
                            (log/info "Got start sentinel" record-json)
                            (jc/produce! producer (:output topic-metadata) record-json)))

                      (= "end" sentinel-type)
                      ; Flush non-SCVs
                      ; sink/get-dirty returns lazy seq, avoid realizing it
                      (do
                        (doseq [record (c-core/get-dirty release-sentinel)]
                          (let [record-json (json/generate-string record)]
                            (log/info record-json)
                            (jc/produce! producer (:output topic-metadata) record-json)))


                        ; Flush SCVs
                        (let [scvs-to-flush (c-assertion/dirty-bubble-scv release-sentinel)]
                          (log/info release-sentinel)
                          (log/infof "Received %d messages to flush" (count scvs-to-flush))
                          (doseq [clinical-assertion scvs-to-flush]
                            (let [built-clinical-assertion (c-assertion/build-clinical-assertion clinical-assertion)
                                  built-clinical-assertion (c-assertion/post-process-built-clinical-assertion built-clinical-assertion)
                                  built-clinical-assertion-json (json/generate-string built-clinical-assertion)]
                              (if (nil? (:id built-clinical-assertion))
                                (throw (ex-info "assertion :id cannot be nil"
                                                {:cause built-clinical-assertion-json})))
                              (if (nil? (:release_date built-clinical-assertion))
                                (throw (ex-info "assertion :release_date cannot be nil"
                                                {:cause built-clinical-assertion-json})))
                              (let [fpath (format "debug/SCV/%s/%s.json"
                                                  (:release_date built-clinical-assertion)
                                                  (:id built-clinical-assertion))]
                                (io/make-parents fpath)
                                (with-open [fwriter (io/writer fpath)]
                                  (.write fwriter built-clinical-assertion-json)))
                              (log/info built-clinical-assertion-json)
                              ; Write message to output
                              (jc/produce! producer (:output topic-metadata) built-clinical-assertion-json)
                              )))

                        ; Write end release sentinel to output
                        (let [record-json (json/generate-string release-sentinel)]
                          (jc/produce! producer (:output topic-metadata) record-json))

                        ; Temporary stop condition for testing data subset
                        ; {:event_type "create", :release_date "2019-07-01", :content {:entity_type "release_sentinel", :clingen_version 0, :sentinel_type "end", :release_tag "clinvar-2019-07-01", :rules [], :source "clinvar", :reason "ClinVar Upstream Release 2019-07-01", :notes nil}}
                        ;(if true
                        ;  (if (and (= "2019-10-01" (:release_date release-sentinel))
                        ;           ;(= "2019-07-01" (:release_date release-sentinel))
                        ;           (= "end" (get-in release-sentinel [:content :sentinel_type])))
                        ;    (do (log/info "Stopping loop")
                        ;        (reset! continue false))))

                        ; Mark entire database as clean. Look into whether this is the best way to do this.
                        ; If failure occurs part-way through processing one release's batch of messages, the
                        ; part sent will be sent again. Should be okay.
                        (let [tables-to-clean [
                                               "release_sentinels"
                                               "submitter"
                                               "submission"
                                               "trait"
                                               "trait_set"
                                               "clinical_assertion_trait_set"
                                               ;"clinical_assertion_trait_set_clinical_assertion_trait_ids"
                                               "clinical_assertion_trait"
                                               "gene"
                                               "variation"
                                               "gene_association"
                                               "variation_archive"
                                               "rcv_accession"
                                               "clinical_assertion"
                                               "clinical_assertion_observation"
                                               ;"clinical_assertion_observation_ids"
                                               "clinical_assertion_variation"
                                               ;"clinical_assertion_variation_descendant_ids"
                                               "trait_mapping"
                                               ]]
                          (doseq [table-name tables-to-clean]
                            (let [updated-count (jdbc/execute! @db-client/db [(format "update %s set dirty = 0 where dirty = 1" table-name)])]
                              (log/infof "Marked %s records in table %s as clean" updated-count table-name))))) ; end do
                      )                                     ; end if end sentinel
                ))))))))
