; This module is intended for message building not extracted into other builders modules
(ns clinvar-combiner.combiners.core
  (:require [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-combiner.combiners.variation
             :refer [variation-list-to-compound]]
            [clinvar-streams.util :refer [obj-max assoc-if set-union-all]]
            [cheshire.core :as json]
            [clojure.java.jdbc :refer :all]
            [taoensso.timbre :as log]))

(defn get-dirty
  "Returns lazy seq of all dirty records in this release.
  Avoid negating the laziness of the returned seq."
  [release-sentinel]
  (log/info "Getting all dirty non-scv records from release: " (str release-sentinel))
  (let [release-date (:release_date release-sentinel)
        get-simple (fn [table-name]
                     (let [s (format "select * from %s where dirty = 1 and release_date = ?" table-name)
                           ;u (format "update %s set dirty = 0 where release_date = ?" table-name)
                           rs (query @db-client/db [s release-date])]
                       ;(execute! @db-client/db [u release-date])
                       rs))
        ; TODO variation-list-to-compound must load whole list of variations, which might be a problem in initial release
        ; Can change sql to order by clinical_assertion_id, wrap with partition-by #(:clinical_assertion_id)
        ; and call variation-list-to-compound on each partition, since there can be no cross-assertion variations
        get-variation (fn []
                        (let [s (str "select v.* from variation v
                                where release_date = ? and
                                (dirty = 1 or
                                 exists (
                                   select * from gene_association ga
                                   where ga.dirty = 1 and ga.variation_id = v.id))")
                              dirty-variations (query @db-client/db [s release-date])
                              _ (log/infof "Got %d dirty variations" (count dirty-variations))]
                          (->> dirty-variations
                               ; Add the latest gene_association entries for this variant.
                               ; If a gene association's most recent event is a deletion, don't include it.
                               (map (fn [variation]
                                      (let [gene-association-sql (str "select * from gene_association ga "
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
                                        (assoc variation :gene_associations
                                                         (if (not (nil? gene-associations))
                                                           gene-associations [])))))
                               ; Set top level release date to max and remove from gene associations
                               (map (fn [variation]
                                      (let [max-release-date (apply obj-max
                                                                    (concat [(:release_date variation)]
                                                                            (map :release_date (:gene_associations variation))))]
                                        (-> variation
                                            (assoc :release_date max-release-date)
                                            ; If a gene association was deleted and the variation is not deleted
                                            ; mark the variation as "update".
                                            ; If the variation was deleted, still pass it on with the new state
                                            ; of the gene associations, but keeping the variation marked deleted.
                                            ((fn [variation]
                                               (let [was-gene-association-deleted
                                                     (some #(= "delete" %) (map :event_type (:gene_associations variation)))]
                                                 (if (and was-gene-association-deleted
                                                          (not= "delete" (:event_type variation)))
                                                   (assoc variation :event_type "update")
                                                   variation))))
                                            (assoc :gene_associations (map #(dissoc % :release_date :event_type :dirty)
                                                                           (:gene_associations variation)))))))
                               (variation-list-to-compound)
                               )

                          ))]
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
                (map #(assoc % :entity_type "variation") (get-variation)) ; Special case for gene_association
                (map #(assoc % :entity_type "variation_archive") (get-simple "variation_archive"))
                (map #(assoc % :entity_type "rcv_accession") (get-simple "rcv_accession"))))))


