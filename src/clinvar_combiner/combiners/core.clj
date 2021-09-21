; This module is intended for message building not extracted into other builders modules
(ns clinvar-combiner.combiners.core
  (:require [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-combiner.combiners.variation
             :refer [variation-list-to-compound]]
            [clinvar-streams.util :refer [obj-max assoc-if set-union-all]]
            [cheshire.core :as json]
            [clojure.java.jdbc :as jdbc :refer :all]
            [taoensso.timbre :as log]
            [clojure.string :as s])
  (:import (java.sql PreparedStatement ResultSet)
           (java.util Iterator)))

;(defn get-simple [table-name]
;  (let [ct (:ct (first (jdbc/query @db-client/db
;                                   [(format "select count(*) as ct from %s where dirty = 1" table-name)])))
;        s (str (format "select * from %s where dirty = 1 " table-name)
;               " order by id, release_date asc limit %s offset %s ")
;        batch-size 100
;        batch-count (Math/ceil (/ ct batch-size))]
;    (let [last-id-pk (atom nil)
;          last-release-date-pk (atom nil)]
;      (for [batch-num (range batch-count)]
;        (let [offset (* batch-size batch-num)
;              limit batch-size
;              s-batch (format s limit offset)]
;          (jdbc/query @db-client/db [s-batch]))))))


;(defn get-simple
;  ;"For tables indexed by id and release_date"
;  ([table-name] (get-simple table-name {}))
;  ([table-name last-pks]
;   (let [s (str (format "select * from %s where dirty = 1 " table-name))
;         pk-where-templ " and (id, release_date) > (?, ?) "
;         order-clause " order by id asc, release_date asc "
;         batch-size 1000
;         limit-clause (format " limit %s " batch-size)
;         ]
;     (let [last-id-pk (:id last-pks)
;           last-release-date-pk (:release_date last-pks)]
;       (let [s-batch (str (format s table-name)
;                          (if (not-empty last-pks)
;                            (str pk-where-templ))
;                          order-clause
;                          limit-clause)
;             params (if (not-empty last-pks)
;                      [s-batch last-id-pk last-release-date-pk]
;                      [s-batch])]
;         (log/info "Running query: " s-batch)
;         (log/info "params: " (rest params))
;         (let [rs (jdbc/query @db-client/db params)]
;           (if (not= 0 (count rs))
;             (lazy-cat
;               rs
;               (get-simple table-name (select-keys (last rs)
;                                                   [:id :release_date])))
;             )
;           ))))))

(defn resultset-iterator
  ([^ResultSet rs] (resultset-iterator rs {}))
  ([^ResultSet rs {:keys [close-fn]
                   :or {close-fn (fn [])}}]
   (let [meta (.getMetaData rs)
         indices (mapv inc (range (.getColumnCount meta)))
         col-names (mapv #(.getColumnName meta %) indices)]
     (letfn [(rs-datify [^ResultSet rs]
               (let [;meta (.getMetaData rs)
                     ;col-names (map #(.getColumnName meta %) (map inc (range (.getColumnCount meta))))
                     col-values (map #(.getObject rs %) indices)]
                 (let [col-name-kw (map keyword col-names)]
                   ;(log/info {:col-names col-name-kw})
                   ;(log/info {:col-values col-values})
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

(def root-variations-sql
  "select v.id, v.descendant_ids
  from variation v
  left join variation vd
  on v.descendant_ids like ('%\"' || vd.id || '\"%')
  where (v.dirty = 1 or vd.dirty = 1)
  and
    not exists
    -- and most-recent descendant_ids which matches the current v.id
    (select descendant_ids from
       -- descendant ids for most recent of all variations
       (select descendant_ids from variation vmax
        where release_date = (select release_date from variation where id = vmax.id
                              order by release_date limit 1))
     where descendant_ids like ('%\"' || v.id || '\"%')
    )
  and v.release_date = (select max(release_date)
                        from variation where id = v.id)
  ")

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
                                         "select * from gene_association ga "
                                         "where ga.variation_id = ? "
                                         "and ga.release_date = "
                                         " (select max(release_date) "
                                         "  from gene_association "
                                         "  where variation_id = ga.variation_id "
                                         "  and gene_id = ga.gene_id) "
                                         "and event_type <> 'delete'")
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
                        "select v.* from variation v "
                        "where v.release_date = "
                        "  (select max(release_date) "
                        "   from variation where id = v.id) "
                        "and event_type <> 'delete' "
                        "and (" in-clause ")")
                  rs-iterator (execute-to-rs-iterator! sql [])]
              (iterator-seq rs-iterator)))
          (get-variation []
            (let [;dirty-variations (get-dirty-root-variations)
                  ;dirty-variations-with-gene-associations (map add-variation-gene-association dirty-variations)
                  ])

            (->> (get-dirty-root-variations)
                 (map add-variation-gene-association)
                 (map #(cons % (get-variation-descendants %)))
                 (map (fn[%] (log/info %) %))
                 (map variation-list-to-compound)
                 (map (fn [%] (if (< 1 (count %))
                                (throw (ex-info "Multiple records returned from variation-list-to-compound"
                                                {:value %}))
                                (first %))))
                 (map (fn[%] (log/info (into [] %)) %))
                 ))]
    (get-variation)))

(defn get-dirty
  "Returns lazy seq of all dirty records in this release.
  Avoid negating the laziness of the returned seq."
  [release-sentinel]
  (log/info "Getting all dirty non-scv records from release: " (str release-sentinel))
  (let [release-date (:release_date release-sentinel)]
    (letfn [
            ;(->> dirty-variations
            ;     ; Set top level release date to max and remove from gene associations
            ;     (map (fn [variation]
            ;            (let [max-release-date (apply obj-max
            ;                                          (concat [(:release_date variation)]
            ;                                                  (map :release_date (:gene_associations variation))))]
            ;              (-> variation
            ;                  (assoc :release_date max-release-date)
            ;                  ; If a gene association was deleted and the variation is not deleted
            ;                  ; mark the variation as "update".
            ;                  ; If the variation was deleted, still pass it on with the new state
            ;                  ; of the gene associations, but keeping the variation marked deleted.
            ;                  ((fn [variation]
            ;                     (let [was-gene-association-deleted
            ;                           (some #(= "delete" %) (map :event_type (:gene_associations variation)))]
            ;                       (if (and was-gene-association-deleted
            ;                                (not= "delete" (:event_type variation)))
            ;                         (assoc variation :event_type "update")
            ;                         variation))))
            ;                  (assoc :gene_associations (map #(dissoc % :release_date :event_type :dirty)
            ;                                                 (:gene_associations variation)))))))
            ;     (variation-list-to-compound)
            ;     )
            ]
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
                  (map #(assoc % :entity_type "rcv_accession") (get-simple "rcv_accession")))))))


