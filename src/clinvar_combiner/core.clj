(ns clinvar-combiner.core
  (:require [clinvar-streams.storage.database-sqlite.sink :as sink]
            [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-combiner.combiners.variation :as c-variation]
            [clinvar-combiner.combiners.clinical-assertion :as c-assertion]
            [clinvar-combiner.combiners.core :as c-core]
            [clinvar-combiner.config :as config
             :refer [app-config topic-metadata kafka-config]]
            [clinvar-combiner.stream
             :refer [make-consume-fn make-produce-fn
                     run-streaming-mode]]
            [clinvar-combiner.snapshot :as snapshot]
            [clinvar-combiner.service]
            [clinvar-streams.util :as util]
            [jackdaw.streams :as j]
            [jackdaw.serdes :as j-serde]
            [jackdaw.client :as jc]
            [cheshire.core :as json]
            [taoensso.timbre :as log]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as s]
            [clojure.spec.alpha :as spec]
            [clinvar-combiner.stream :as stream])
  (:import [org.apache.kafka.streams KafkaStreams]
           [java.util Properties]
           [java.time Duration]
           (org.apache.kafka.common TopicPartition))
  (:gen-class))

;(defn to-db
;  "Writes an entry to the database for key and value. Delegates to appropriate handling in db module."
;  [key value]
;  (sink/store-message val-map)
;  [key val])

(defn write-map-to-file
  "Writes map p to file with filename provided, in k=v format."
  [m filename]
  (with-open [writer (io/writer filename)]
    (doseq [[k v] m]
      (.write writer (str k "=" v "\n")))))

(defn seek-to-beginning [consumer topic-name]
  (log/info {:fn :seek-to-beginning :consumer consumer :topic-name topic-name})
  ;(jc/seek-to-beginning-eager consumer)
  (jc/poll consumer (Duration/ofSeconds 5))
  (let [assignment (jc/assignment consumer)]
    (log/info {:assignment assignment})
    (doseq [topic-partition assignment]
      (jc/seek consumer (TopicPartition.
                          (:topic-name topic-partition)
                          (:partition topic-partition))
               0)))
  consumer)

(defn -main-streaming
  "Configure and start kafka application run-streaming-mode"
  [& args]
  (clinvar-combiner.service/start)
  (write-map-to-file (kafka-config (app-config)) "kafka.properties")
  (log/set-level! :debug)
  (db-client/init!)

  (let [consumer (jc/consumer (kafka-config (app-config)))
        producer (jc/producer (kafka-config (app-config)))
        topic-name (get-in topic-metadata [:input :topic-name])]
    (log/info "Subscribing to topic and assigning all partitions" (:input topic-metadata))
    (let [topic-partitions (stream/topic-partitions consumer topic-name)]
      (apply (partial jc/assign consumer) topic-partitions))
    ;(jc/subscribe consumer [(:input topic-metadata)])

    (let [version-to-resume-from config/version-to-resume-from]
      (cond
        (empty? version-to-resume-from)
        (seek-to-beginning consumer topic-name),
        (= "LOCAL" version-to-resume-from)
        (stream/set-consumer-to-db-offset
          consumer
          (stream/topic-partitions consumer topic-name)),
        :else
        (snapshot/set-db-to-version! version-to-resume-from)))

    (let [consume! (make-consume-fn consumer)
          produce! (make-produce-fn producer)]
      (run-streaming-mode consume! produce!))))

;(def cli-options
;  [[nil "--mode" "Startup mode"
;    :default "streaming"
;    :required "MODE"
;    :validate [#(util/in? % ["streaming" "snapshot"])
;               "#(util/in? % [\"streaming\" \"snapshot\"]"]]
;   [nil "--resume-from" "Snapshot file version to resume from (each is tagged with its latest offset)"
;    ;:default (System/getenv "DX_CV_COMBINER_SNAPSHOT_VERSION")
;    :required "SNAPSHOTTED_OFFSET"
;    :parse-fn #(Integer/parseInt %)
;    :validate [#(<= 0 %)
;               "#(<= 0 %)"]]
;   ])


(defn validate-mode [mode]
  (spec/def ::validate-mode #(util/in? % ["snapshot" "stream"]))
  (if (spec/valid? ::validate-mode mode)
    mode (spec/explain ::validate-mode mode)))

(defn -main [& args]
  (let [mode (validate-mode (util/get-env-required "DX_CV_COMBINER_MODE"))]
    (log/info {:mode mode})
    (case mode
      "snapshot" (clinvar-combiner.snapshot/-main args)
      "stream" (-main-streaming args))))
