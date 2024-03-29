(ns clinvar-streams.storage.database-psql.client
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :refer :all]
            [clojure.java.shell :refer :all]
            [clinvar-qc.config :refer [app-config]]
            [taoensso.timbre :as log])
  (:import [java.io File]
           (com.mchange.v2.c3p0 ComboPooledDataSource)))

(def db (atom {}))
(def datasource (atom nil))

(def sql-root "src/clinvar_streams/storage/database_psql/sql")

(defn sql-path
  "Returns a cwd-rooted path for a sql filename under the project's sql directory.
  Throws exception if not found."
  [filename]
  (.getPath (io/file sql-root filename)))

(defn create-datasource [] (let [ds (ComboPooledDataSource.)]
                             (log/info "Configuring datasource")
                             (.setDriverClass ds "org.postgresql.Driver")
                             (.setJdbcUrl ds (format "jdbc:postgresql://%s/%s"
                                                     (get @db :host)
                                                     (get @db :dbname)))
                             (.setUser ds (get @db :user))
                             (.setPassword ds (get @db :password))
                             (.setMinPoolSize ds 10)
                             (.setMaxPoolSize ds 500)
                             ds))

(defn configure!
  "Configures the client to use the database at the provided db-path.
  Opens and closes a Connection, to verify that a connection could be established"
  [db-path]
  ;(reset! db {:classname    "org.sqlite.JDBC"
  ;            :subprotocol  "sqlite"
  ;            :subname      db-path
  ;            :foreign_keys "on"}) ; foreign_keys=on sets PRAGMA foreign_keys=on
  (reset! db {:dbtype   "postgresql"
              :dbname   "clinvar"
              :user     (:db-user app-config)
              :host     (:db-host app-config)
              :password (:db-password app-config)})
  (reset! datasource (create-datasource)))

(defn init!
  "Initializes the database with given file path, relative to cwd.
  Will remove all prior contents, according to the contents of initialize.sql"
  [db-path]
  (configure! db-path)
  (println @db)
  (let [;initialize-sql (slurp (sql-path "initialize.sql"))
        ;sh-ret (sh "sqlite3" db-path (str ".read " (sql-path "initialize.sql")))
        sh-ret (with-open [rdr (io/reader (sql-path "initialize.sql"))]
                 ; TODO /bin/sh wrapping might not be necessary now with env passthrough below
                 (sh "/bin/sh" "-c"
                     (format "psql -h %s -U %s"
                             ;(get @db :dbname)
                             (get @db :host)
                             (get @db :user))
                     :env (assoc (into {} (System/getenv)) "PGPASSWORD" (get @db :password))
                     :in rdr))]
    (log/debug sh-ret)
    (if (not= 0 (:exit sh-ret))
      (do (log/error (ex-info "Failed to run initialize.sql" sh-ret))
          (throw (ex-info "Failed to run initialize.sql" sh-ret))))
    ; Validating that connection can be established
    (let [conn (get-connection @db)]
      (.close conn)
      (log/info "Successfully connected to db" @db))))
