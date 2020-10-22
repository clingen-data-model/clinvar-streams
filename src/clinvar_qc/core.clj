(ns clinvar-qc.core
  (:require [jackdaw.client :as jc]
            [jackdaw.serdes :as j-serde]
            [jackdaw.streams :as js]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [clinvar-qc.config :as cfg]
    ;[clinvar-qc.database-psql.client :as db-client]
    ;[clinvar-qc.database-psql.sink :as db-sink]
            [clinvar-qc.database.client :as db-client]
            [clinvar-qc.database.sink :as db-sink]
            [clinvar-qc.spec :as spec]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.core.async :as async]
            )
  (:import (java.util Date)
           (java.io Writer)
           (java.util.zip GZIPInputStream)
           (java.time Duration))
  (:gen-class))

(defn reset-consumer-offset
  "Resets the consumer offset to the beginning of the topic. Is stateful on the consumer, but also returns the consumer."
  [consumer]
  (log/info "Resetting consumer to beginning of all topics")
  (jc/seek-to-beginning-eager consumer)
  consumer)

;(defn repair
;  "Performs series of transformations on input topic and sends to output topic.
;  Messages which meet criteria for modification are sent to output topic modified."
;  [consumer-config ^String consumer-topic producer-config ^String producer-topic
;   & {:keys [reset-consumer-offset] :or {reset-consumer-offset false}}]
;  (with-open [consumer (jc/consumer consumer-config)
;              producer (jc/producer producer-config)]
;    (jc/subscribe consumer [{:topic-name (:topic-name consumer-topic)}])
;
;    ))

(defn store-in-database
  [[k v]]
  ;(log/info "store-in-sqlite")
  ;(db-sink/store-message v)
  (db-sink/store-message v)
  )

(def input-counter {:val       (atom 0)
                    :interval  10000
                    :timestamp (atom (Date.))})

(defn count-msg
  [[k v]]
  (swap! (:val input-counter) inc)
  (if (= 0 @(:val input-counter))
    (reset! (:timestamp input-counter) (Date.))
    (if (= 0 (mod @(:val input-counter) (:interval input-counter)))
      (let [prev-time @(:timestamp input-counter)
            cur-time (Date.)]
        (log/infof "Read %d messages in %.6f seconds (last: %s)"
                   (:interval input-counter)
                   (double (/ (- (.getTime cur-time) (.getTime prev-time)) 1000))
                   v
                   (reset! (:timestamp input-counter) cur-time))))))

(defn topology [builder in-topic out-topic]
  "Builds a topology of operations to apply to a kstream from builder.
  Statefully applies the topology to builder, return value unused."
  (-> (js/kstream builder in-topic)
      (js/peek count-msg)
      ; Store message in sqlite database
      (js/peek store-in-database)
      ;; TODO temporary filter for debugging
      ;(js/filter (fn [[k v]] (= "SCV000335826" (-> (json/parse-string v true) :content :id))))
      ;; Transform clinical assertion using stored data
      ;(js/map build-clinical-assertion)
      ;(js/to out-topic)
      )
  )

(defn get-env-boolean
  "Returns a boolean environment variable by name. Accepts 1 or 'true' as truthy."
  [name]
  (let [val (System/getenv name)]
    (or (= "1" val)
        (.equalsIgnoreCase "true" val))))

(defn -main
  [& args]
  ;(repair (cfg/kafka-config cfg/app-config) (:kafka-consumer-topic cfg/app-config)
  ;        (cfg/kafka-config cfg/app-config) (:kafka-producer-topic cfg/app-config)
  ;        (get-env-boolean "KAFKA_RESET_CONSUMER_OFFSET"))

  (db-client/init! "database.db")

  (let [builder (js/streams-builder)]
    (topology builder (:input cfg/topic-metadata) (:output cfg/topic-metadata))
    (let [app (js/kafka-streams builder (cfg/kafka-config cfg/app-config))]
      (js/start app))))

(defn -main-file []
  (db-client/init! "database.db")
  ;(doseq [i (range 8)]
  ;  (.start (Thread. (fn []
  ;                     (while true
  ;                       (let [msg (async/<!! sink/message-queue)
  ;                             ;idle-conns (.getNumIdleConnections @db-client/datasource)
  ;                             ;total-conns (.getNumConnections @db-client/datasource)
  ;                             ]
  ;                         ;(log/infof "Storing message (%s/%s connections in use)"
  ;                         ;           (- total-conns idle-conns) total-conns)
  ;                         (sink/store-message msg)))))))

  (with-open [i-stream (GZIPInputStream.
                         (io/input-stream (str (-> cfg/topic-metadata :input :topic-name) ".topic.gz")))
              reader (io/reader i-stream)
              producer (jc/producer (cfg/kafka-config cfg/app-config))]
    (doseq [line (line-seq reader)]
      (let [terms (s/split line #" " 2)
            k (first terms)
            v (nth terms 1)]
        (count-msg [k v])
        (case (db-sink/store-message v)
          :ok (log/trace "store-message: ok")
          :data_integrity_error (do
                                  (log/error (ex-info "CLINGEN_ERROR: data_integrity_error"
                                                      {:key k :value v}))
                                  )
          (log/trace "unknown return from store-message"))
        (let [msg-validated (spec/validate (json/parse-string v true))]
          (log/info msg-validated)
          (jc/produce! producer (get-in cfg/topic-metadata [:output]) k (json/generate-string msg-validated))
          )
        ))))

(defn save-msg
  [[k v] fwriter]
  (.write fwriter ^String (format "%s %s\n" k v)))

(defn download-topology [builder in-topic fwriter]
  "Builds a topology of operations to apply to a kstream from builder.
  Statefully applies the topology to builder, return value unused."
  (letfn [(peek-fn [[k v]]
            (save-msg [k v] fwriter))]
    (-> (js/kstream builder in-topic)
        (js/peek count-msg)
        (js/peek peek-fn))
    )
  )

(def download-writer (atom {}))

(def running (atom true))
(defn -download-input-topic-nostream
  []
  (let [output-filename (str (-> cfg/topic-metadata :input :topic-name) ".topic")]
    (reset! download-writer (io/writer output-filename))
    (with-open [consumer (jc/consumer (cfg/kafka-config cfg/app-config))
                producer (jc/producer (cfg/kafka-config cfg/app-config))]
      (jc/subscribe consumer [{:topic-name (-> cfg/topic-metadata :input :topic-name)}])
      (while @running
        (let [msgs (jc/poll consumer (Duration/ofMillis 1000))]
          (doseq [msg msgs]
            (let [k (:key msg) v (:value msg)]
              (count-msg [k v])
              (save-msg [k v] @download-writer)
              ))
          )))))

(def read-chan (async/chan 1000))

(defn download-input-topic-async-reader
  [channel]
  (reset! download-writer (io/writer (str (-> cfg/topic-metadata :input :topic-name) ".topic")))
  (while @running
    (when-let [batch (async/<!! channel)]
      (doseq [msg batch]
        (let [k (:key msg) v (:value msg)]
          (count-msg [k v])
          (save-msg [k v] @download-writer)
          )
        )
      )))

(defn -download-input-topic-async
  []
  (.start (Thread. (partial download-input-topic-async-reader read-chan)))
  (let [output-filename (str (-> cfg/topic-metadata :input :topic-name) ".topic")]
    (reset! download-writer (io/writer output-filename))
    (with-open [consumer (jc/consumer (cfg/kafka-config cfg/app-config))
                producer (jc/producer (cfg/kafka-config cfg/app-config))]
      (jc/subscribe consumer [{:topic-name (-> cfg/topic-metadata :input :topic-name)}])
      (while @running
        (when-let [msgs (jc/poll consumer (Duration/ofMillis 10000))]
          (async/>!! read-chan msgs)
          ;(doseq [msg msgs]
          ;  (async/>!! read-chan msg)
          ;  )
          )))))

(defn -download-input-topic
  []

  (let [builder (js/streams-builder)
        output-filename (str (-> cfg/topic-metadata :input :topic-name) ".topic")]
    (reset! download-writer (io/writer output-filename))
    ;(with-open [fwriter (io/writer (str (-> cfg/topic-metadata :input :topic-name) ".topic"))]
    (download-topology builder (:input cfg/topic-metadata) @download-writer)
    (let [app (js/kafka-streams builder (cfg/kafka-config cfg/app-config))]
      (js/start app))))
