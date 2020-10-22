(ns clinvar-qc.database.client
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :refer :all]
            [clojure.java.shell :refer :all]
            [clinvar-raw.core :as cr-core]
            [taoensso.timbre :as log])
  (:import [java.io File]))

(def db (atom {}))

(def sql-root "src/clinvar_qc/database/sql")

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
              :foreign_keys "on"}) ; foreign_keys=on sets PRAGMA foreign_keys=on
  (let [conn (get-connection @db)]
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
