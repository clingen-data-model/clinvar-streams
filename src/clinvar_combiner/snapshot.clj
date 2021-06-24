(ns clinvar-combiner.snapshot
  (:require [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-combiner.config]
            [clojure.java.shell :refer [sh]]
            [me.raynes.fs :as fs]
            [taoensso.timbre :as log]
            [clojure.string :as s]
            [jackdaw.client :as jc])
  (:import (java.nio.file Paths)
           (com.google.common.io ByteStreams)
           (com.google.cloud.storage
             Bucket BucketInfo Storage StorageOptions
             BlobId BlobInfo Blob Storage$BlobWriteOption)
           (java.io FileInputStream)
           (java.nio.channels FileChannel)
           (org.apache.kafka.common TopicPartition)
           (java.time Duration)))


(defn download-file
  "Pull the specified file from cloud storage, writing it locally to target-path"
  [bucket blob-name target-path]
  (log/info {:fn :retrieve-snapshot
             :bucket bucket
             :blob-name blob-name
             :target-path target-path})
  (let [target-path (Paths/get target-path (make-array java.lang.String 0))
        blob (.get (.getService (StorageOptions/getDefaultInstance))
                   (BlobId/of bucket blob-name))]
    (.downloadTo blob target-path)
    target-path))

(defn BlobId->str [blob-id]
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

(defn trim-suffix [s suffix]
  (if (.endsWith s suffix) (subs s 0 (- (count s) (count suffix))) s))

(defn- topic-partitions [consumer topic-name]
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
  [consumer]
  (let [db-path clinvar-combiner.config/sqlite-db
        ; TODO maybe wipe database first? Might want both options, from scratch
        ; or from base existing data
        consumer-thread '(Thread. (partial clinvar-combiner.core/-main-streaming))
        topic-name (get-in clinvar-combiner.config/topic-metadata
                           [:input :topic-name])
        partitions (topic-partitions consumer topic-name)
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
      (log/debug "Waiting for topics to be finished reading")
      (Thread/sleep (.toMillis (Duration/ofSeconds 10))))
    ))

(defn update-consumer-offsets
  "partition-offsets should be a map of {[topic-name part-idx] offset, ...}

  Update the offsets in the consumer to these so that the next poll will start at offset+1
  on each partition."
  [partition-offsets]
  )

(defn -main [& args]
  (let [version-to-resume-from (System/getenv "DX_CV_COMBINER_SNAPSHOT_VERSION")
        consumer (make-consumer)]
    ; Update consumer offset based on version-to-resume-from
    (build-database consumer))

  (let [db-path clinvar-combiner.config/sqlite-db
        archive (archive-file db-path (str db-path ".tar.gz"))
        versioned-name (str archive ".0")
        uploaded-blob-id (upload-file "clinvar-streams-dev" archive versioned-name)])
  )

(defn -test-main [& args]
  (let [db-path clinvar-combiner.config/sqlite-db
        archive (archive-file db-path (str db-path ".tar.gz"))
        versioned-name (str archive ".0")
        uploaded-blob-id (upload-file "clinvar-streams-dev" archive versioned-name)
        _ (log/info (BlobId->str uploaded-blob-id))
        downloaded-file (download-file "clinvar-streams-dev" versioned-name archive)

        ;_ (extract-file archive ".")
        ;extracted (trim-suffix archive ".tar.gz")
        ]
    ))
