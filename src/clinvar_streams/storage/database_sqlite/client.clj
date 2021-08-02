(ns clinvar-streams.storage.database-sqlite.client
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.shell :refer :all]
            [taoensso.timbre :as log]
            [clinvar-streams.util :as util])
  (:import [java.io File]
           (java.sql PreparedStatement)
           (java.util Iterator)))

(def db (atom {}))

(def sql-resource-root "clinvar_streams/storage/database_sqlite/sql")

(defn sql-resource-path
  "Returns a cwd-rooted path for a sql filename under the project's sql directory.
  Throws exception if not found."
  [filename]
  (str sql-resource-root "/" filename))

(defn run-sql-resource [db-path filename]
  (let [resource-path (sql-resource-path "initialize.sql")]
    (let [sh-ret (sh "sqlite3" db-path :in (slurp (io/resource resource-path)))]
      (if (not= 0 (:exit sh-ret))
        (do                                                 ;(log/error (ex-info (str "Failed to run ") sh-ret))
          (throw (ex-info (str "Failed to run " resource-path) sh-ret)))))))

(defn configure!
  "Configures the client to use the database at the provided db-path.
  Opens and closes a Connection, to verify that a connection could be established.
  If the database is empty, also initializes it."
  ([]
   (configure! (util/get-env-required "SQLITE_DB")))
  ([db-path]
   ; non-core keys in db are passed as Properties to DriverManager/getConnection
   ; https://github.com/clojure/java.jdbc/blob/acffd9f5f216f8b8c1fc960c1d47b0b5feb56730/src/main/clojure/clojure/java/jdbc.clj#L271
   (reset! db {:classname "org.sqlite.JDBC"
               :subprotocol "sqlite"
               :subname db-path
               ; foreign_keys=on sets PRAGMA foreign_keys=on
               :foreign_keys "on"
               :synchronous "off"})
   (let [conn (jdbc/get-connection @db)]
     (.close conn))))

(defn initialized?
  "Returns true if the db atom has been configured and the database tables have been initialized.
  Does not check to see if the schema matches that of initialize.sql."
  ([]
   (initialized? (util/get-env-required "SQLITE_DB")))
  ([db-path]
   (and (not (empty? @db))
        (= db-path (:subname @db))
        ; http://clojure-doc.org/articles/ecosystem/java_jdbc/using_ddl.html
        (jdbc/with-db-metadata
          [md @db]
          (let [table-metas (jdbc/metadata-result
                              (.getTables md nil nil "release_sentinels" (into-array ["TABLE" "VIEW"])))]
            (not (empty? table-metas)))))))

(defn init!
  "Initializes the database with given file path, relative to cwd.
  Will remove all prior contents, according to the contents of initialize.sql"
  ([]
   (init! (util/get-env-required "SQLITE_DB")))
  ([db-path]
   (configure! db-path)
   (run-sql-resource db-path "initialize.sql")))

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
      it-seq)))

(defn update-offset [topic-name partition-idx offset]
  (log/debug {:fn :update-offset :offset offset :topic-name topic-name :partition-idx partition-idx})
  (let [ret (jdbc/execute! @db ["insert into topic_offsets(topic_name, partition, offset) values(?, ?, ?)"
                                topic-name partition-idx offset])]))

(defn get-offset [topic-name partition-idx]
  (log/debug {:fn :get-offset :topic-name topic-name :partition-idx partition-idx})
  (let [ret (jdbc/query @db ["select offset from topic_offsets where topic_name = ? and partition = ?"
                             topic-name partition-idx])
        _ (log/debug {:ret ret})
        offset (:offset (first ret))]
    (log/debug {:fn :get-offset :offset offset})
    offset))

(defn latest-release-date
  "Returns the latest date on a received release sentinel message."
  []
  (let [ret (jdbc/query @db ["select max(release_date) as m from release_sentinels"])]
    (log/debug {:fn :latest-release-date :ret ret})
    (:m (first ret))))
