(ns clinvar-combiner.snapshot
  (:require [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-combiner.config
             :refer [topic-metadata]]
            [clinvar-combiner.stream :as stream]
            [clojure.java.shell :refer [sh]]
            [me.raynes.fs :as fs]
            [taoensso.timbre :as log]
            [clojure.string :as s]
            [jackdaw.client :as jc]
            [clinvar-combiner.config :as config]
            [clinvar-streams.util :as util])
  (:import (java.nio.file Paths)
           (com.google.common.io ByteStreams)
           (com.google.cloud.storage
             Bucket BucketInfo Storage StorageOptions
             BlobId BlobInfo Blob Storage$BlobWriteOption)
           (java.io FileInputStream)
           (java.nio.channels FileChannel)
           (org.apache.kafka.common TopicPartition)
           (java.time Duration Instant)))


(defn download-file
  "Pull the specified file from cloud storage, writing it locally to target-path"
  [bucket blob-name target-path]
  (log/info {:fn :download-file
             :bucket bucket
             :blob-name blob-name
             :target-path target-path})
  (let [target-path (Paths/get target-path (make-array java.lang.String 0))
        blob (.get (.getService (StorageOptions/getDefaultInstance))
                   (BlobId/of bucket blob-name))]
    (.downloadTo blob target-path)
    target-path))

(defn BlobId->str
  "Formats a BlobId as a fully qualified URI string."
  [blob-id]
  (format "gs://%s/%s" (.getBucket blob-id) (.getName blob-id)))

(defn upload-file
  "Uploads src-file-name to target-bucket as dest-blob-name.
  Returns the BlobId uploaded to."
  [target-bucket src-file-name dest-blob-name]
  (log/info {:fn :upload-file
             :target-bucket target-bucket
             :src-file-name src-file-name
             :dest-blob-name dest-blob-name})
  (let [gc-storage (.getService (StorageOptions/getDefaultInstance))
        blob-id (BlobId/of target-bucket dest-blob-name)
        blob-info (-> blob-id BlobInfo/newBuilder (.setContentType "application/gzip") .build)
        from (.getChannel (FileInputStream. src-file-name))]
    (with-open [to (.writer gc-storage blob-info (make-array Storage$BlobWriteOption 0))]
      (ByteStreams/copy from to))
    blob-id))

(defn extract-file
  "Performs tar extract and gzip decompress on the path, writing the output to dest directory."
  [path dest]
  (log/info {:fn :extract-file :path path :dest dest})
  (let [ret (sh "tar" "-xzf" path "-C" dest)]
    (log/info {:fn :extract-file :msg ret})
    (if (not= 0 (:exit ret))
      (throw (ex-info "Failed to extract file" ret))
      dest)))

(defn archive-file
  "Performs tar and gzip on the path. Path can be a single file or a directory.
  Archive will contain the path, so if it is a directory, it will contain the top level
  directory itself, plus the contents, recursively."
  [path dest]
  (log/info {:fn :archive-file :path path :dest dest})
  (let [ret (sh "tar" "-czf" dest path)]
    (log/info {:fn :archive-file :msg ret})
    (if (not= 0 (:exit ret))
      (throw (ex-info "Failed to archive file" ret))
      dest)))

(defn trim-suffix
  "Return s, with trailing suffix removed if s ends with suffix."
  [s suffix]
  (if (.endsWith s suffix) (subs s 0 (- (count s) (count suffix))) s))

(defn- topic-partitions
  "Returns a seq of TopicPartitions that the consumer is subscribed to for topic-name."
  [consumer topic-name]
  (let [partition-infos (.partitionsFor consumer topic-name)]
    (map #(TopicPartition. (.topic %) (.partition %)) partition-infos)))

(defn- read-end-offsets [consumer topic-partitions]
  (let [kafka-end-offsets (.endOffsets consumer topic-partitions)
        end-offset-map (reduce (fn [acc [k v]]
                                 (assoc acc [(.topic k) (.partition k)] v))
                               {} kafka-end-offsets)]
    end-offset-map))

(defn partitions-finished-reading?
  "partitions should be a sequence of TopicPartitions, offsets should be a map
  of {[topic-name part-idx] offset ...} as from read-end-offsets.
  Returns false if the stored offset in the local db does not match
  the end offset for each partition."
  [partitions end-offsets]
  (every? identity
          (for [topic-partition partitions]
            (let [topic-name (.topic topic-partition)
                  part-idx (.partition topic-partition)
                  local-offset (db-client/get-offset topic-name part-idx)
                  remote-offset (get end-offsets [topic-name part-idx])]
              (log/debug {:fn :partitions-finished-reading?
                          :topic-name topic-name
                          :part-idx part-idx
                          :local-offset local-offset
                          :remote-offset remote-offset})
              (= local-offset remote-offset)))))

(defn make-consumer
  []
  (jc/consumer (clinvar-combiner.config/kafka-config (clinvar-combiner.config/app-config))))

(defn build-database
  [consumer partitions]
  (log/info {:fn :build-database :partitions partitions})
  (let [consume! (stream/make-consume-fn consumer)
        produce! (fn [_])
        consumer-thread (Thread. (partial stream/run-streaming-mode
                                          consume!
                                          produce!))
        end-offsets (read-end-offsets consumer partitions)
        partitions-finished-reading? (fn []
                                       (every? identity
                                               (for [topic-partition partitions]
                                                 (let [topic-name (.topic topic-partition)
                                                       part-idx (.partition topic-partition)
                                                       local-offset (db-client/get-offset topic-name part-idx)
                                                       remote-offset (get end-offsets [topic-name part-idx])]
                                                   (log/debug {:fn :partitions-finished-reading?
                                                               :topic-name topic-name
                                                               :part-idx part-idx
                                                               :local-offset local-offset
                                                               :remote-offset remote-offset})
                                                   (= local-offset remote-offset)))))]
    (.start consumer-thread)
    (while (not (partitions-finished-reading?))
      (log/debug "Waiting for topics to be finished reading...")
      (Thread/sleep (.toMillis (Duration/ofSeconds 5))))
    (log/info "Finished building database")
    ; Turn off main poll loop, which ends consumer-thread
    (reset! stream/run-streaming-mode-continue false)
    ))

;(defn update-consumer-offsets
;  "partition-offsets should be a map of {[topic-name part-idx] offset, ...}
;
;  Update the offsets in the consumer to these so that the next poll will start at offset+1
;  on each partition."
;  [consumer partition-offsets]
;  ; TODO
;  )

(defn generate-versioned-archive-name [basename version]
  (format (str basename ".%s.tar.gz")
          version))
;(defn parse-versioned-archive-name [archive-name]
;  ; TODO
;  ())



(defn download-version
  "Sets the local database state db-path to be the version specified"
  [version db-path]
  (log/info {:fn :download-version :version version :db-path db-path})
  (fs/delete db-path)
  (let [versioned-archive (generate-versioned-archive-name "clinvar.sqlite3" version)]
    (download-file config/snapshot-bucket versioned-archive versioned-archive)
    (extract-file versioned-archive ".")))

(defn ^String make-timestamp
  "Returns a string timestamp for the current UTC time.
  Formatted in ISO-8601 with punctuation (DateTimeFormatter/ISO_INSTANT)"
  []
  (str (Instant/now)))

(defn set-consumer-to-db-offset
  "Takes a consumer and a seq of TopicPartition objects."
  [consumer partitions]
  (doseq [partition partitions]
    (let [topic-name (.topic partition)
          partition-idx (.partition partition)
          local-offset (or (db-client/get-offset topic-name partition-idx) 0)]
     (log/info (format "Seeking to offset %s for partition (%s, %s)"
                       local-offset topic-name partition-idx))
     (jc/seek consumer partition local-offset))))

(defn -main [& args]
  (let [version-to-resume-from (System/getenv "DX_CV_COMBINER_SNAPSHOT_VERSION")
        consumer (make-consumer)
        topic-name (-> topic-metadata :input :topic-name)
        partitions (topic-partitions consumer topic-name)]
    (apply (partial jc/assign consumer) partitions)

    ; Use version to set database state (if needed) and set the consumer offsets
    (cond
      ; Resume from start of topic
      (empty? version-to-resume-from)
      (do (log/info "No snapshot resume version specified, starting from scratch")
          (log/info "Deleting local db")
          (fs/delete config/sqlite-db)
          (db-client/init!)
          (log/info "Seeking to beginning of topic")
          (jc/seek-to-beginning-eager consumer)),

      ; Resume from whatever the local state is
      (= "LOCAL" version-to-resume-from)
      (do (log/info "Resuming from latest local snapshot")
          (db-client/configure!)
          (set-consumer-to-db-offset consumer partitions)),

      ; Wipe local state and resume from a remote snapshot
      :else
      (do (log/info "Attempting to resume from remote snapshot version:" version-to-resume-from)
          (log/info "Deleting local db")
          (fs/delete config/sqlite-db)
          (db-client/init!)
          (download-version version-to-resume-from config/sqlite-db)
          (set-consumer-to-db-offset consumer partitions)))

    (build-database consumer partitions))

  (let [db-path clinvar-combiner.config/sqlite-db
        archive (archive-file db-path (str db-path ".tar.gz"))
        bucket config/snapshot-bucket
        versioned-name (generate-versioned-archive-name (fs/base-name db-path) (db-client/latest-release-date))
        uploaded-blob-id (upload-file bucket archive versioned-name)]
    (log/info "Uploaded:" (BlobId->str uploaded-blob-id)))
  )
