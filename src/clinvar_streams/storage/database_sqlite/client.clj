(ns clinvar-streams.storage.database-sqlite.client
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.shell :refer :all]
            [taoensso.timbre :as log])
  (:import [java.io File]
           (java.sql PreparedStatement)
           (java.util Iterator)))

(def db (atom {}))

(def sql-root "src/clinvar_streams/storage/database_sqlite/sql")

(defn sql-path
  "Returns a cwd-rooted path for a sql filename under the project's sql directory.
  Throws exception if not found."
  [filename]
  (.getPath (io/file sql-root filename)))

(defn configure!
  "Configures the client to use the database at the provided db-path.
  Opens and closes a Connection, to verify that a connection could be established"
  [db-path]
  (reset! db {:classname    "org.sqlite.JDBC"
              :subprotocol  "sqlite"
              :subname      db-path
              :foreign_keys "on"})                          ; foreign_keys=on sets PRAGMA foreign_keys=on
  (let [conn (jdbc/get-connection @db)]
    (.close conn)))

(defn init!
  "Initializes the database with given file path, relative to cwd.
  Will remove all prior contents, according to the contents of initialize.sql"
  [db-path]
  (configure! db-path)
  (let [sh-ret (sh "sqlite3" db-path (str ".read " (sql-path "initialize.sql")))]
    (if (not= 0 (:exit sh-ret))
      (do (log/error (ex-info "Failed to run initialize.sql" sh-ret))
          (throw (ex-info "Failed to run initialize.sql" sh-ret))))))

;(defn query [[sql & args]]
;  (jdbc/query @db (cons sql args)))
;
;(defn execute! [[sql & args]]
;  (jdbc/execute! @db (cons sql args)))

(defn select
  [^PreparedStatement prepared-statement]
  (let [rs (.executeQuery prepared-statement)
        rs-meta (.getMetaData rs)
        col-names (loop [count (.getColumnCount rs-meta)
                         names []]
                    ;(.getColumntype rs-meta)
                    (if (= 0 count)
                      names
                      (recur (dec count)
                             (conj names (.getColumnName rs-meta (dec count))))))]
    (let [it (reify Iterator
                    (next [this] (do (.next rs)
                                     (into {} (map #(vector % (.getObject rs %)) col-names))))
                    (hasNext [this] (not (or (.isLast rs) (.isAfterLast rs)))))
          it-seq (iterator-seq it)]
      it-seq
      )
    ;(for [:when (not (.isAfterLast rs))]
    ;  (into {} (map #(vector % (.getObject rs %)) col-names)))
    ;(let [it (reify Iter)])
    ))
