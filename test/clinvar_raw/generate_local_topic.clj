"
README: used to generate a topic of clinvar-raw messages from a folder structure
of root/release-date/type/operation/file

Usage from REPL:
(ns clinvar-raw.generate-local-topic)
(use 'clinvar-raw.generate-local-topic :reload-all)
(-main {:root-dir \"test/clinvar_raw/testset\"})
"
(ns clinvar-raw.generate-local-topic
  (:require [clinvar-raw.stream :as stream]
            [clinvar-raw.config :as config]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [jackdaw.client :as jc]
            [taoensso.timbre :as log]))


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
  (if (re-matches #"\d{8}.*" date)
    (s/join "-"
            ; group 0 is matching string
            (rest (re-find (re-matcher #"(\d{4})(\d{2})(\d{2})" date))))
    date))

;(defn str-to-date
;  [s]
;  (cond
;    (re-matches #"\d{8}" s) (let [sdf (SimpleDateFormat. "yyyyMMdd")]
;                              (.setTimeZone sdf (TimeZone/getTimeZone "UTC"))
;                              (.parse sdf s))
;    (re-matches #"\d{8}T\d{6}" s) (let [sdf (SimpleDateFormat. "yyyyMMdd'T'HHmmss")]
;                                      (.setTimeZone sdf (TimeZone/getTimeZone "UTC"))
;                                      (.parse sdf s))
;    :default (throw (ex-info "Failed to parse time string" {:cause s}))
;    ))
;
;(defn str-to-date-test
;  []
;  (println (str-to-date "20201028"))
;  (println (str-to-date "20201028T123045"))
;  )

(defn list-files-sorted
  [root-dir]
  (sort-by #(.getName %) (.listFiles (io/file root-dir))))

(defn generate-drop-messages
  [{:keys [root-dir root-matcher]
    :or {root-matcher #(re-matches #"\d{8}.*" %)            ; YYYYMMDD...
         }}]
  (for [date-dir (filter #(and (root-matcher (.getName %)) (.isDirectory %)) (list-files-sorted root-dir))]
    (do (log/info {:date-dir date-dir})
        (let [file-listing (filter #(not (nil? %))
                                   (for [file (file-seq date-dir)]
                                     (let [relative-path (trim-leading "/" (subs (.getPath file) (count root-dir)))]
                                       ;(println "Relative path: " relative-path)
                                       (if (re-matches #"\d{8}\w*/\w+/\w+/\d+" relative-path) ; YYYYMMDD.../.../.../d
                                         relative-path))))]
          {:release_date (yyyymmmdd-split (.getName date-dir))
           :bucket (ensure-leading "./" root-dir)
           :files (into [] file-listing)}))))

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
  "Upload a seq of JSON messages to a topic"
  [msgs producer-config topic-name]
  (with-open [producer (jc/producer producer-config)]
    (doseq [msg msgs]
      (log/info "Uploading" (json/generate-string msg))
      (stream/send-update-to-exchange producer topic-name msg)
      ;(jc/send! producer (jd/map->ProducerRecord jackdaw-message))
      )))

(defn -main
  "Must provide argument map with :root-dir and :topic-name.

  Example: {:root-dir \"./test/clinvar_raw/testdata_20201021\" :topic-name \"broad-dsp-clinvar-testdata-20201021\"}"
  [{:keys [root-dir]}]
  (let [dataset-name (.getName (io/file root-dir))
        dsp-topic (str "broad-dsp-clinvar-" dataset-name)
        drop-messages (generate-drop-messages {:root-dir root-dir})
        jackdaw-messages (map (fn [m] {:key (:release_date m)
                                       :value m})
                              drop-messages)]
    ;; Save drop messages and upload them to the drop message topic
    (save-to-topic-file jackdaw-messages (str dsp-topic ".topic"))

    (let [output-topic (:kafka-producer-topic config/env-config)
          kafka-producer-config (-> config/env-config config/kafka-config)
          producer (jc/producer kafka-producer-config)]
      (letfn [(process-local-drop-message
                [message]
                (log/info {:fn :process-local-drop-message :message message})
                (let [output-messages (stream/process-clinvar-drop
                                       message
                                       {:storage-protocol "file://"})]
                  ;; Realize whole lazy seq into memory
                  ;; TODO remove
                  (doall output-messages)
                  (run! (fn [m]
                          (log/info {:msg "Sending message" :message m})
                          (stream/send-update-to-exchange producer output-topic m))
                        output-messages)))]
        (run! process-local-drop-message drop-messages)))))
