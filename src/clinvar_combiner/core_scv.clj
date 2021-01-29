(ns clinvar_combiner.core_scv
  (:require [clinvar-submissions.db :as db]
            [clinvar-streams.util :as util]
            [jackdaw.streams :as j]
            [jackdaw.serdes :as j-serde]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:import [org.apache.kafka.streams KafkaStreams]
           [java.util Properties])
  (:gen-class))

(def app-config {:kafka-host     "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092"
                 :kafka-user     (util/get-env-required "KAFKA_USER")
                 :kafka-password (util/get-env-required "KAFKA_PASSWORD")
                 :kafka-consumer-topic    "clinvar-scv-test"})

(def topic-metadata
  {:input
   {:topic-name "clinvar-raw"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}
   :output
   {:topic-name "clinvar-scv"
    :partition-count 1
    :replication-factor 3
    :key-serde (j-serde/string-serde)
    :value-serde (j-serde/string-serde)}})

(defn kafka-config
  "Expects, at a minimum, :kafka-user and :kafka-password in opts. "
  [opts]
  {"ssl.endpoint.identification.algorithm" "https"
   "compression.type"                      "gzip"
   "sasl.mechanism"                        "PLAIN"
   "request.timeout.ms"                    "20000"
   "application.id"                        (util/get-env-required "KAFKA_GROUP")
   "bootstrap.servers"                     (:kafka-host opts)
   "retry.backoff.ms"                      "500"
   "security.protocol"                     "SASL_SSL"
   "key.serializer"                        "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer"                      "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer"                      "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer"                    "org.apache.kafka.common.serialization.StringDeserializer"
   "sasl.jaas.config"                      (str "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                                (:kafka-user opts) "\" password=\"" (:kafka-password opts) "\";")})


(defn select-clinical-assertion
  "Return true if the message is a clinical assertion
  otherwise return nil"
  [[key v]]
  ;  (log/debug "in select-clinical-assertion " key (get-in (json/parse-string v true) [:content :entity_type]))
  (= "clinical_assertion" (get-in (json/parse-string v true) [:content :entity_type])))

(def rocksdb-path (if (not (empty? (System/getenv "CLINVAR_SUBMISSIONS_DB_DIR")))
                    (System/getenv "CLINVAR_SUBMISSIONS_DB_DIR")
                    "/tmp/clinvar-submissions-rocksdb/"))
(defonce rocksdb (db/init! rocksdb-path))

(defn jparse
  [^String s]
  (json/parse-string s true))

(defn join-safe
  [delim vec]
  (loop [v vec s ""]
    (if (empty? v) s
                   (recur (rest v)
                          (if (empty? s) (str (first v)) (str s delim (first v)))))))

(defn join-non-empty
  [delim vec]
  (join-safe delim (filter #(not (empty? %)) vec)))

(def key-functions
  ; Simple id-keyed types
  {:clinical_assertion_observation (fn [id] (str "clinical_assertion_observation-" id))
   :clinical_assertion_trait (fn [id] (str "clinical_assertion_trait-" id))
   :clinical_assertion_trait_set (fn [id] (str "clinical_assertion_trait_set-" id))
   :gene (fn [id] (str "gene-" id))
   :rcv_accession (fn [id] (str "rcv_accession-" id))
   :submission (fn [id] (str "submission-" id))
   :submitter (fn [id] (str "submitter-" id))
   :trait (fn [id] (str "trait-" id))
   :trait_set (fn [id] (str "trait_set-" id))
   :variation (fn [id] (str "variation-" id))
   :variation_archive (fn [id] (assert (not (empty? id))) (str "variation_archive-" id))

   ; Complex types

   ; SCV (id) intentionally last
   :clinical_assertion (fn [id vcv-id rcv-id] (join-non-empty "-" ["clinical_assertion" vcv-id rcv-id id]))
   ; SCV intentionally first
   :clinical_assertion_variation (fn [id scv-id] (join-non-empty "-" ["clinical_assertion_variation" scv-id id]))
   :gene_association (fn [variation-id gene-id] (join-non-empty "-" ["gene_association" variation-id gene-id]))
   ; combine all fields as the key, assume record is immutable
   :trait_mapping (fn [scv-id trait-type mapping-type mapping-value mapping-ref medgen-id mapping-name]
                    (join-non-empty "-" ["trait_mapping" scv-id trait-type mapping-type mapping-value mapping-ref medgen-id mapping-name]))
   })

(defn get-entity-key
  [entity]
  ;(println entity)
  (let [entity-content (:content entity)
        entity-type (:entity_type entity-content)]
    (case entity-type
      "clinical_assertion" ((:clinical_assertion key-functions)
                            (:id entity-content)
                            (:variation_archive_id entity-content)
                            (:rcv_accession_id entity-content))
      "clinical_assertion_variation" ((:clinical_assertion_variation key-functions)
                                      (:id entity-content)
                                      (:clinical_assertion_id entity-content))
      "gene_association" ((:gene_association key-functions) ; association table variation<->gene
                          (:variation_id entity-content)
                          (:gene_id entity-content))
      "trait_mapping" ((:trait_mapping key-functions)
                       (:clinical_assertion_id entity-content)
                       (:trait_type entity-content)
                       (:mapping_type entity-content)
                       (:mapping_value entity-content)
                       (:mapping_ref entity-content)
                       (:medgen_id entity-content)
                       (:mapping_name entity-content))

      ; Non complex identifiers, can be used based off id, which is referenced as foreign keys in other primary entities
      (str entity-type "-" (:id entity-content))
      )))

(def previous-entity-type (atom ""))

(defn to-rocksdb
  ""
  [[^String key ^String val]]
  ; Put each message in rocksdb
  (let [val-map (json/parse-string val true)
        id (get-entity-key val-map)]
    ; Log entity-type changes for monitoring stream
    (if (not (= @previous-entity-type (-> val-map :content :entity_type)))
      (do
        (log/debugf "entity-type changed from %s to %s\n" @previous-entity-type (-> val-map :content :entity_type))
        (reset! previous-entity-type (-> val-map :content :entity_type))))

    (log/trace "storing" id)
    (case (:type val-map)
      "create" (db/put-record rocksdb id val)
      "update" (db/put-record rocksdb id val)
      "delete" (db/delete-record rocksdb id)
      (ex-info "Message type not [create|update|delete]" {}))
    )
  [key val])

(defn build-clinical-assertion
  ""
  [[^String key ^String val]]
  ; For the clinical assertion record val, combine all linked entities
  (let [clinical-assertion (json/parse-string val true)
        content (:content clinical-assertion)]
    (log/debug "original" val)
    (log/debug "building clinical assertion" (:id content))

    (let [; variation archive
          variation-archive-key ((:variation_archive key-functions) (:variation_archive_id content))
          variation-archive (json/parse-string (db/get-key rocksdb variation-archive-key) true)

          rcv (if (not-empty (:rcv_accession_id content))
                (let [rcv-key ((:rcv_accession key-functions) (:rcv_accession_id content))]
                  (json/parse-string (db/get-key rocksdb rcv-key) true))
                (do (log/warn "clinical assertion" (:id content) "had no rcv_accession_id")
                    {}))

          clinical-assertion-observation-keys (map #((:clinical_assertion_observation key-functions) %)
                                                   (:clinical_assertion_observation_ids content))
          clinical-assertion-observations (map #(json/parse-string (db/get-key rocksdb %) true)
                                               clinical-assertion-observation-keys)

          ; TODO clinical_assertion_variation
          ; only know scv id, not the clinical_assertion_variation id
          ; [id scv-id]
          clinical-assertion-variation-key-prefix ((:clinical_assertion_variation key-functions) "" (:id content))
          clinical-assertion-variations (map #(json/parse-string % true)
                                             (map #(second %) ; get-prefix returns [[k v] [k v]]
                                                  (db/get-prefix rocksdb clinical-assertion-variation-key-prefix)))

          ; Variation
          ; variation_id
          variation (if (not-empty (:variation_id content))
                      (let [variation-key ((:variation key-functions) (:variation_id content))]
                        (json/parse-string (db/get-key rocksdb variation-key) true))
                      (do (log/warn "clinical assertion" (:id content) "has no variation_id")
                          {}))

          ; submitter_id
          submitter-key ((:submitter key-functions) (:submitter_id content))
          submitter (json/parse-string (db/get-key rocksdb submitter-key) true)

          ; submission_id
          submission-key ((:submission key-functions) (:submission_id content))
          submission (json/parse-string (db/get-key rocksdb submission-key) true)

          ; TODO trait_set_id
          ; trait-set-key ((:trait_set key-functions) (:trait_set_id content))
          ; trait-set (json/parse-string (db/get-key rocksdb trait-set-key) true)
          trait-set (if (not-empty (:trait_set_id content))
                      (let [trait-set-key ((:trait_set key-functions) (:trait_set_id content))]
                        (json/parse-string (db/get-key rocksdb trait-set-key) true))
                      {})

          ; TODO trait
          traits (if (not-empty trait-set)
                   (let [trait-keys (map #((:trait key-functions) %) (:trait_ids trait-set))]
                     (map #(json/parse-string (db/get-key rocksdb %) true) trait-keys))
                   {})

          ; TODO trait_mapping
          ; [scv-id trait-type mapping-type mapping-value mapping-ref medgen-id mapping-name]
          trait-mappings (map #(json/parse-string % true)
                              (map #(second %)
                                   (db/get-prefix rocksdb
                                                  ((:trait_mapping key-functions) (:id content) "" "" "" "" "" "")))) ; Only pass SCV id


          ; TODO clinical_assertion_trait_set_id
          clinical-assertion-trait-set-key ((:clinical_assertion_trait_set key-functions) (:clinical_assertion_trait_set_id content))
          clinical-assertion-trait-set (json/parse-string (db/get-key rocksdb clinical-assertion-trait-set-key) true)

          ]

      (let [updated-clinical-assertion
            (-> clinical-assertion
                ; Add fields to content, remove foreign key fields from content
                (assoc-in [:content :variation_archive] variation-archive)
                (update-in [:content] dissoc :variation_archive_id)

                (assoc-in [:content :rcv] rcv)
                (update-in [:content] dissoc :rcv_accession_id)

                (assoc-in [:content :clinical_assertion_observations] clinical-assertion-observations)
                (update-in [:content] dissoc :clinical_assertion_observation_ids)

                (assoc-in [:content :clinical_assertion_trait_set] clinical-assertion-trait-set)
                (update-in [:content] dissoc :clinical_assertion_trait_set_id)

                (assoc-in [:content :variation] variation)
                (update-in [:content] dissoc :variation_id)

                (assoc-in [:content :submitter] submitter)
                (update-in [:content] dissoc :submitter_id)

                (assoc-in [:content :submission] submission)
                (update-in [:content] dissoc :submission_id)

                (assoc-in [:content :trait_set] trait-set)
                (update-in [:content] dissoc :trait_set_id)

                ; TODO overwrite trait_ids
                (assoc-in [:content :traits] traits)
                (update-in [:content] dissoc :trait_ids)

                (assoc-in [:content :trait_mappings] trait-mappings)
                (assoc-in [:content :trait_set] trait-set)

                ; Reverse keyed to clinical assertion
                (assoc-in [:content :clinical_assertion_variations] clinical-assertion-variations)
                )
            ]
        (log/debug "updated" (json/generate-string updated-clinical-assertion))
        ; return same key with string serialized updated clinical assertion
        [key (json/generate-string updated-clinical-assertion)]
        )))
  )


(defn topology [builder in-topic out-topic]
  "Builds a topology of operations to apply to a kstream from builder.
  Statefully applies the topology to builder, return value unused."
  (-> (j/kstream builder in-topic)
      ; Stash message in rocksdb
      (j/peek to-rocksdb)
      ; Filter to :entity_type clinical_assertion
      (j/filter select-clinical-assertion)
      ; TODO temporary filter for debugging
      (j/filter (fn [[k v]] (= "SCV000335826" (-> (json/parse-string v true) :content :id))))
      ; Transform clinical assertion using stored data
      (j/map build-clinical-assertion)
      (j/to out-topic))
  )

(defn write-map-to-file
  [m filename]
  (with-open [writer (io/writer filename)]
    (doseq [[k v] m]
      (.write writer (str k "=" v "\n")))))

(defn -main
  "Construct topology and start kafka streams application"
  [& args]
  (write-map-to-file (kafka-config app-config) "kafka.properties")
  (log/set-level! :debug)

  (let [builder (j/streams-builder)]
    (topology builder (:input topic-metadata) (:output topic-metadata))
    (let [app (j/kafka-streams builder (kafka-config app-config))]
      (j/start app))))
