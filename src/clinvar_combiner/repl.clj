(ns clinvar-combiner.repl
  (:require [clinvar-combiner.core :as core]
            [clinvar-streams.storage.database-sqlite.sink :as sink]
            [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-streams.stream-utils :as stream-utils]
            [clojure.pprint :refer [pprint]]
            [clojure.java.jdbc :as jdbc])
  (:gen-class))


(defn get-scv
  "Gets an SCV record as of release-date.

  NOTE: not sql injection safe"
  [release-date scv-id]

  (let [sql (format (str "select * from clinical_assertion ca "
                         "where ca.id = '%s' "
                         "and release_date = ( "
                         "  select max(release_date) "
                         "  from clinical_assertion "
                         "  where id = ca.id "
                         "  and release_date <= '%s' "
                         ")")
                    scv-id release-date)
        rs (jdbc/query @db-client/db [sql])]
    (cond
      (= 1 (count rs))
      (first rs)
      (< 1 (count rs))
      (throw (ex-info "More than 1 record returned for SCV"
                      {:release-date release-date :scv-id scv-id :result-set rs}))
      :default [])))

