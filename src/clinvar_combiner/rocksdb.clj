(ns clinvar-combiner.rocksdb
  (:require [taoensso.timbre :as log])
  (:import (org.rocksdb RocksDB RocksDBException Options)
           (java.lang String)))

(defn init!
  "Construct and initialize RocksDB client with given path. Will create
  database if doesn't exist already."
  [^String db-path]
  (log/info "init!" db-path)
  (let [opts (-> (Options.) (.setCreateIfMissing true))]
    (RocksDB/open opts db-path)))

(defn put-record
  "Set key to val"
  [^RocksDB rocksdb ^String key ^String val]
  (log/debug "put-record" key)
  (.put rocksdb (.getBytes key) (.getBytes val)))

(defn delete-record
  "Delete record with key from database"
  [^RocksDB rocksdb ^String key]
  (log/debug "delete-record" key)
  (.delete rocksdb (.getBytes key)))

(defn get-key
  "Return val that has exact match for key"
  [^RocksDB rocksdb ^String key]
  (log/debug "get-key" key)
  (String. (.get rocksdb (.getBytes key))))

(defn get-prefix-with-matcher
  [^RocksDB rocksdb ^String key-prefix matcher-fn]
  ; RocksIterator.seek goes to first key that matches (or after) key.
  ; Since RocksDB is key-sorted, can iterate until first key doesn't start with the prefix.
  (log/debug "get-prefix-with-matcher" key-prefix matcher-fn)
  (let [iterator (.newIterator rocksdb)
        ret (transient [])]
    (.seek iterator (.getBytes key-prefix))
    (while (and (.isValid iterator) (matcher-fn (String. (.key iterator))))
      (conj! ret [(String. (.key iterator)) (String. (.value iterator))])
      (.next iterator))
    (let [ret-persist (persistent! ret)]
      (log/debug ret-persist)
      ret-persist)
  ))

(defn get-prefix
  "Return vector of all (key,val) for which key has given prefix"
  [^RocksDB rocksdb ^String key-prefix]
  ; RocksIterator.seek goes to first key that matches (or after) key.
  ; Since RocksDB is key-sorted, can iterate until first key doesn't start with the prefix.
  (log/debug "get-prefix" key-prefix)

  (letfn [(matcher-fn
           [key] (.startsWith key key-prefix))]
    (get-prefix-with-matcher rocksdb key-prefix matcher-fn))

;  (let [iterator (.newIterator rocksdb)
;        ret (transient [])]
;    (.seek iterator (.getBytes key-prefix))
;    (while (and (.isValid iterator) (.startsWith (String. (.key iterator)) key-prefix))
;      (conj! ret [(String. (.key iterator)) (String. (.value iterator))])
;      (.next iterator))
;    (let [ret-persist (persistent! ret)]
;      (log/debug ret-persist)
;      ret-persist)
;    )
  )

(defn prefix-subsequences-matcher-fn
  "Returns true if key starts with prefix, followed by each subsequence in order, with
  any number of chars between subsequences"
  [prefix subsequences key]
  (and (.startsWith key prefix)
       (loop [subseqs subsequences
              k (subs key (count prefix))]
         (log/debugf "subseqs: %s, k: %s" subseqs k)
         (if (empty? subseqs)
           true
           (if (empty? k)
             false
             (if (.startsWith k (first subseqs))
               ; Got a subseq, pop first subseq len chars of k and first subseq
               (recur (rest subseqs) (subs k (count (first subseqs))))
               ; No match for first subseq, pop first char of k
               (recur subseqs (subs k 1))
               )
             )
           )
         )
       )
  )

(defn get-prefix-with-subsequence
  "Get [k,v] instances from db if the key contains both the prefix and all of the subsequences, in order
  Example: ABCDEFGHIJKL matches {:prefix A :subsequences [A CDE H KL]}"
  [rocksdb prefix subsequences]
  (get-prefix-with-matcher rocksdb prefix (partial prefix-subsequences-matcher-fn prefix subsequences)))
