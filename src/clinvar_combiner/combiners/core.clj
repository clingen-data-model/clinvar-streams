; This module is intended for message building not extracted into other builders modules
(ns clinvar-combiner.combiners.core
  (:require [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-combiner.combiners.variation
             :refer [variation-list-to-compound]]
            [clinvar-streams.util :refer [obj-max assoc-if set-union-all]]
            [cheshire.core :as json]
            [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as log]
            [clojure.string :as s])
  (:import (java.sql PreparedStatement ResultSet)
           (java.util Iterator)
           (java.time Instant Duration)))

(defn query [db params]
  (if-let [sql (first params)]
    (log/debug {:fn ::query :sql sql :other-params params})
    (let [start (Instant/now)
          result (jdbc/query db params)
          end (Instant/now)
          elapsed (Duration/between start end)]
      (log/debug {:fn ::query :elapsed-millis (.toMillis elapsed)})
      result)))

(defn resultset-iterator
  ([^ResultSet rs] (resultset-iterator rs {}))
  ([^ResultSet rs {:keys [close-fn]
                   :or {close-fn (fn [])}}]
   (let [meta (.getMetaData rs)
         indices (mapv inc (range (.getColumnCount meta)))
         col-names (mapv #(.getColumnName meta %) indices)]
     (letfn [(rs-datify [^ResultSet rs]
               (let [col-values (map #(.getObject rs %) indices)]
                 (let [col-name-kw (map keyword col-names)]
                   (into {} (map vector col-name-kw col-values)))))]
       (reify Iterator
         (next [this]
           (if (.isBeforeFirst rs)
             (.next rs))
           (if (not (.isAfterLast rs))
             (do (let [record (rs-datify rs)]
                   (if (not (.next rs))
                     (close-fn))
                   record))
             (throw (ex-info "After last row"
                             {:rs rs}
                             (IllegalStateException. "After last row"))))

           )
         (hasNext [this] (not (.isAfterLast rs))))))))

(defn execute-to-rs-iterator! [sql params]
  (let [conn (jdbc/get-connection @db-client/db)
        ps (.prepareStatement conn sql)]
    (doseq [[i param] (map-indexed vector params)]
      (.setObject ps (inc i) param))
    (let [rs (.executeQuery ps)]
      (doseq [[i param] (map-indexed vector params)]
        (.setObject ps (inc i) param))
      (resultset-iterator rs {:close-fn #(do (.close rs)
                                             (.close ps)
                                             (.close conn))}))))

(defn get-simple
  ([table-name] (get-simple table-name []))
  ([table-name where-conditions]
   (let [sql (format (str "select * from %s "
                          " where dirty = 1 "
                          (if (< 0 (count where-conditions))
                            (s/join " and " (map #(str "(" % ")") where-conditions)))
                          " order by id asc, release_date asc ")
                     table-name)
         conn (jdbc/get-connection @db-client/db)
         ps (.prepareStatement conn sql)
         rs (.executeQuery ps)]
     (iterator-seq
       (resultset-iterator rs {:close-fn #(do (.close rs)
                                              (.close ps)
                                              (.close conn))})))))

(defn get-dirty-variations []
  (letfn [(get-dirty-root-variations []
            ;; Return an iterator of all variations that are dirty via themself or descendants or gene association
            ;; only the most recent can ever be dirty so don't bother checking for latest release_date
            (let [s (str
                      "select v.* from variation_latest v
                      where (
                          -- either the variation or a descendant is dirty
                          v.dirty = 1
                          or
                          exists (select * from variation_latest vd
                                  where v.descendant_ids like ('%\"' || vd.id || '\"%')))
                        -- exclude those which are descendants of other variations
                        and not exists
                          (select descendant_ids
                           from variation_latest
                           where descendant_ids like ('%\"' || v.id || '\"%'))
                      order by v.id asc")
                  rs-iterator (execute-to-rs-iterator! s [])]
              (iterator-seq rs-iterator)))
          (add-variation-gene-association [variation]
            (let [gene-association-sql (str
                                         "select * from gene_association_latest ga "
                                         "where ga.variation_id = ? "
                                         "and ga.event_type <> 'delete'")
                  gene-associations (query @db-client/db
                                           [gene-association-sql
                                            (:id variation)])]
              (assoc-if variation :gene_associations gene-associations)))
          (get-variation-descendants
            [parent-variation]
            ;; Return a seq of the most recent descendants of parent-variation
            (let [descendant-ids (json/parse-string (:descendant_ids parent-variation))
                  in-clause (str "v.id in (" (s/join ", " descendant-ids) ") ")
                  sql (str
                        "select v.* from variation_latest v "
                        "where event_type <> 'delete' "
                        "and (" in-clause ")")
                  rs-iterator (execute-to-rs-iterator! sql [])]
              (iterator-seq rs-iterator)))
          (get-variation []
            (->> (get-dirty-root-variations)
                 (map add-variation-gene-association)
                 (map #(cons % (get-variation-descendants %)))
                 (map (fn [%] (log/info %) %))
                 (map variation-list-to-compound)
                 (map (fn [%] (if (< 1 (count %))
                                (throw (ex-info "Multiple records returned from variation-list-to-compound"
                                                {:value %}))
                                (first %))))
                 (map (fn [%] (log/info (into [] %)) %))
                 ))]
    (get-variation)))

(defn get-dirty
  "Returns lazy seq of all dirty records in this release.
  Avoid negating the laziness of the returned seq."
  [release-sentinel]
  (log/info "Getting all dirty non-scv records from release: " (str release-sentinel))
  (let [release-date (:release_date release-sentinel)]
    (map
      ; If record was event_type=delete set deleted=true. Remove event_type.
      (fn [rec] (dissoc (if (= "delete" (:event_type rec))
                          (assoc rec :deleted true)
                          rec)
                        :event_type))
      (lazy-cat (map #(assoc % :entity_type "submitter") (get-simple "submitter"))
                (map #(assoc % :entity_type "submission") (get-simple "submission"))
                (map #(assoc % :entity_type "trait") (get-simple "trait"))
                (map #(assoc % :entity_type "trait_set") (get-simple "trait_set"))
                (map #(assoc % :entity_type "gene") (get-simple "gene"))
                (map #(assoc % :entity_type "variation") (get-dirty-variations)) ; Special case for gene_association
                (map #(assoc % :entity_type "variation_archive") (get-simple "variation_archive"))
                (map #(assoc % :entity_type "rcv_accession") (get-simple "rcv_accession"))))))


