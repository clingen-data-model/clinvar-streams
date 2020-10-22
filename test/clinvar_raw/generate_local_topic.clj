"
README: used to generate a topic of clinvar-raw messages from a folder structure
of root/release-date/type/operation/file

Usage from REPL:
(ns clinvar-raw.generate-local-topic)
(use 'clinvar-raw.generate-local-topic :reload-all)
(-main {:root-dir \"test/clinvar_raw/testset\"})
"
(ns clinvar-raw.generate-local-topic
  (:require [clinvar-raw.core :as raw-core]
            [clinvar-raw.config :as raw-config]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [jackdaw.client :as jc]
            [jackdaw.data :as jd]
            [taoensso.timbre :as log])
  (:import [java.io File])
  (:gen-class))

(defn process-directories
  ""
  [dir-list]
  (doseq [dirname dir-list]
    ))

(defn is-dir?
  [& path-segs]
  (.isDirectory (apply io/file path-segs)))

(defn trim-leading
  "If `s` is prefixed with `leading`, remove that prefix."
  [leading s]
  (if (.startsWith s leading)
    (subs s (count leading))
    s))

(defn ensure-leading
  "Naively add `leading` to `s` if not there. Does not add partial prefix of `leading`
  if a suffix is present as prefix of `s`"
  [leading s]
  (if (not (.startsWith s leading))
    (str leading s)
    s))

(defn yyyymmmdd-split
  [date]
  (if (re-matches #"\d{8}" date)
    (s/join "-"
            ; group 0 is matching string
            (rest (re-find (re-matcher #"(\d{4})(\d{2})(\d{2})" date))))
    date))

(defn generate-drop-messages
  [{:keys [root-dir root-matcher]
    :or   {root-matcher #(re-matches #"\d{8}" %)            ; YYYYMMDD
           }}]

  (for [date-dir (.listFiles (io/file root-dir))]
    (let [file-listing (filter #(not (nil? %))
                               (for [file (file-seq date-dir)]
                                 (let [relative-path (trim-leading "/" (subs (.getPath file) (count root-dir)))]
                                   ;(println "Relative path: " relative-path)
                                   (if (re-matches #"\d{8}/\w+/\w+/\d+" relative-path)
                                     relative-path))))]
      {:release_date (yyyymmmdd-split (.getName date-dir))
       :bucket       (ensure-leading "./" root-dir)
       :files        (into [] file-listing)})))

(defn process-local-drop-messages
  [drop-messages]
  (doseq [msg drop-messages]
    (raw-core/process-clinvar-drop msg :storage-protocol "file://")
    ))

(defn chan-get-available!
  [from-channel]
  (loop [ch from-channel
         out []]
    (let [m (async/poll! from-channel)]
      (if (not (nil? m))
        (recur ch (conj out m))
        out))))

(defn attach-trivial-hdr-ts
  [msgs]
  (let [ts-counter (atom (long 0))]                         ; inc will start at 1
    (map #(-> %
              (assoc :headers (or (:headers %) []))
              (assoc :timestamp (or (:timestamp %) (swap! ts-counter inc))))
         msgs)))

(defn save-to-topic-file
  "Expects msgs to be a seqable of maps of kafka message structures (key/value/headers/timestamp)"
  [msgs filename]
  (with-open [fout (io/writer filename)]
    (doseq [msg (attach-trivial-hdr-ts msgs)]
      (.write fout (str (if (string? msg) msg (json/generate-string msg)) "\n")))))

(defn upload-to-topic
  [msgs producer-config topic-name]
  (with-open [producer (jc/producer producer-config)]
    (doseq [msg msgs]
      (log/info "Uploading" (json/generate-string msg))
      (raw-core/send-update-to-exchange producer topic-name msg)
      ;(jc/send! producer (jd/map->ProducerRecord jackdaw-message))
      )
    ))

(defn -main
  ""
  [{:keys [root-dir]}]
  (let [drop-messages (generate-drop-messages {:root-dir root-dir})]
    (save-to-topic-file
      (map (fn [m] {:key   (:release_date m)
                    :value m})
           drop-messages)
      "testset-drops.topic")
    ; Might block if # msg is more than size of raw-core/producer-channel (1000)
    (process-local-drop-messages drop-messages)
    (let [producer-messages (chan-get-available! raw-core/producer-channel)
          topic-name "kferriter-testset"]
      (save-to-topic-file producer-messages "testset.topic")
      (log/info "Uploading " (count producer-messages) " to " topic-name)
      (upload-to-topic producer-messages
                       (raw-config/kafka-config raw-config/app-config)
                       topic-name))))