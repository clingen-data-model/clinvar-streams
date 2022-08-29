(ns clinvar-streams.download
  (:require [jackdaw.client :as jc]
            [clinvar-streams.config :as cfg]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [clojure.string :as s]
            [jackdaw.data :as jd])
  (:import (java.time Duration)
           (java.util Date)))

(def input-counter {:val       (atom (long 0))
                    :interval  10000
                    :timestamp (atom (Date.))})
(defn count-msg
  [[k v]]
  (if (= 0 @(:val input-counter))
    (reset! (:timestamp input-counter) (Date.)))
  (swap! (:val input-counter) inc)
  (if (= 0 (mod @(:val input-counter) (:interval input-counter)))
    (let [prev-time @(:timestamp input-counter)
          cur-time (Date.)]
      (log/infof "Read %d messages in %.6f seconds (last: %s)"
                 (:interval input-counter)
                 (double (/ (- (.getTime cur-time) (.getTime prev-time)) 1000))
                 v
                 (reset! (:timestamp input-counter) cur-time)))))

(defn save-msg
  [[k v] fwriter]
  (.write fwriter ^String (format "%s %s\n" k v)))

(def download-writer (atom {}))

(def running (atom true))
(defn -download-input-topic-nostream
  [{:keys [topic-name] :or {topic-name (-> cfg/topic-metadata :input :topic-name)}}]
  (let [output-filename (str topic-name ".topic")]
    (reset! download-writer (io/writer output-filename))
    (with-open [consumer (jc/consumer (cfg/kafka-config (cfg/app-config)))
                producer (jc/producer (cfg/kafka-config (cfg/app-config)))]
      (jc/subscribe consumer [{:topic-name topic-name}])
      (while @running
        (let [msgs (jc/poll consumer (Duration/ofMillis 1000))]
          (if (= 0 (count msgs))
            (.flush @download-writer))
          (doseq [msg msgs]
            (let [k (:key msg) v (:value msg)]
              (count-msg [k v])
              (save-msg [k v] @download-writer))))))))

(defn -upload-topic
  [{:keys [topic-name file-name]
    :or   {topic-name (-> cfg/topic-metadata :input :topic-name)}}]
  (let [file-name (if (not-empty file-name) file-name (str topic-name ".topic"))]
    (log/info "Reading file" file-name)
    (with-open [file-rdr (io/reader file-name)
                consumer (jc/consumer (cfg/kafka-config (cfg/app-config)))
                producer (jc/producer (cfg/kafka-config (cfg/app-config)))]
      (let [file-lines (line-seq file-rdr)]
        (doseq [line file-lines]
          (let [terms (s/split line #" ")
                key (first terms)
                msg (s/join " " (rest terms))]
            (log/infof "Producing to %s key=%s msg=%s" topic-name key msg)
            (jc/send! producer (jd/map->ProducerRecord {:topic-name topic-name
                                                        :key key
                                                        :value msg}))))))))
