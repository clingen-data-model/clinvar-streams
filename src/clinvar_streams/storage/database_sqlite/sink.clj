(ns clinvar-streams.storage.database-sqlite.sink
  (:require [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-streams.util :refer [in? obj-max assoc-if]]
            [cheshire.core :as json]
            [clojure.java.jdbc :refer :all]
            [clojure.string :as s]
            [clojure.set :refer [rename-keys]]
            [clojure.pprint :refer [pprint]]
            [taoensso.timbre :as log]
            [clojure.set :as set])
  (:import (java.sql PreparedStatement Connection SQLException)
           (java.util Map)))

(defn set-string
  [pstmt idx val]
  (log/trace idx val)
  (.setObject pstmt idx val))

(defn set-int
  [pstmt idx val]
  (log/trace idx val)
  ; Using generic setObject for convenience, handling of nulls
  (.setObject pstmt
              idx
              (cond (int? val) val
                    (nil? val) val
                    :else (Integer/parseInt val))))

(defn json-string-if-not-string
  [obj]
  (if (not (string? obj))
    (json/generate-string obj)
    obj))

(defn parameterize-statement
  "Type map should be in the form
  {:field1 <:string|:int>
   :field2 <:string|:int>}

   Or a collection of 2-element collections
   [[:field1 <:string|:int>]
    [:field2 <:string|:int>]]"
  [^Connection conn sql type-map value-map]
  (let [; [ [idx [field type]] ... ]
        param-map (map-indexed vector type-map)
        params (map (fn [[i [k v]]] {:idx (+ 1 i) :field k :type v}) param-map)
        pstmt (-> (.prepareStatement conn sql)
                  ((fn [%] (loop [st %
                                  ps params]
                             ;(log/debug "Looping through params" (into [] ps))
                             (if (empty? ps)
                               st
                               (let [param (first ps)]
                                 (log/tracef "[%s]: %s" (:field param) (get value-map (:field param)))
                                 (case (:type param)
                                   :string (set-string st (:idx param) (get value-map (:field param)))
                                   :int (set-int st (:idx param) (get value-map (:field param)))
                                   :object (.setObject st (:idx param) (get value-map (:field param)))
                                   (ex-info "Unknown type for arg" {:cause param}))
                                 (recur st (rest ps)))))
                     )))]
    pstmt))

(defn record-exists?
  "Returns true if in the table table-name there exists a table row for which the value of each column named in
  where-fields matches the corresponding mapped value in record"
  [{:keys [record table-name where-fields]}]
  (assert (< 0 (count where-fields)))
  (let [where-fields (into [] where-fields)
        sql (format "select case when exists(select 1 from %s where %s) 1 else 0 end as e"
                    table-name
                    (s/join " and " (map (fn [field] (str field " = ? ")) where-fields)))
        types (into {} (map (fn [%] [% :object]) (keys record)))]
    (with-open [conn (get-connection @db-client/db)]
      (let [pstmt (parameterize-statement conn sql types record)
            rs (.executeQuery pstmt)
            ret (.getInt rs "e")]
        (assert (in? [0 1] ret))                            ; Assert ret in [0 1]
        (= 1 ret)))))

; Check for primary key violation exception, log warning, run again with 'insert or replace'
(defn -assert-insert
  [{:keys [table-name type-map value-map]}]
  (let [sql (format "insert into %s(%s) values(%s)"
                    table-name
                    (s/join "," (into [] (map #(name %) (keys type-map))))
                    (s/join "," (into [] (map (fn [%] "?") (keys type-map)))))]
    (with-open [conn (get-connection @db-client/db)]
      (let [pstmt (parameterize-statement conn sql type-map value-map)]
        (try (let [updated-count (.executeUpdate pstmt)]
               (if (not= 1 updated-count)
                 (throw (ex-info (str "Failed to insert " table-name)
                                 {:cause {:sql sql :types type-map :values value-map}}))))
             (catch Exception e
               (log/error (ex-info "Exception on insert"
                                   {:cause {:sql sql :types type-map :values value-map}
                                    :sql-state (.getSQLState e)}))
               (throw e)))))))

(defn assert-insert
  [{:keys [table-name type-map value-map]}]
  (try (-assert-insert {:table-name table-name :type-map type-map :value-map value-map})
       (catch Exception e
         (log/error (json/generate-string e))
         (throw e))))

(defn assert-update
  [{:keys [table-name type-map value-map where-fields]}]
  (assert (< 0 (count where-fields)))
  (let [; ensure constant order for fields by placing into vectors
        where-fields (into [] where-fields)
        non-where-fields (into [] (filter #(not (contains? where-fields %)) (keys type-map)))
        set-clause (str " set " (s/join "," (map (fn [field] (str (name field) " = ?"))
                                                 non-where-fields)))
        where-clause (str " where " (s/join " and " (map (fn [pk-field] (str (name pk-field) " = ?"))
                                                         where-fields)))
        sql (format "update %s %s %s"
                    table-name
                    ;(s/join "," (map (fn [_] "?") (keys set-clause)))
                    set-clause
                    where-clause)
        ; to ensure ordering, convert maps to vectors
        ; parametrize statement just expects [k v] iterables
        type-vec (map (fn [field] [field (get type-map field)])
                      (concat non-where-fields where-fields))
        value-vec (map (fn [field] [field (get value-map field)])
                       (concat non-where-fields where-fields))
        _ (pprint sql)
        _ (pprint type-vec)
        _ (pprint value-vec)
        ]
    (with-open [conn (get-connection @db-client/db)]
      (let [pstmt (parameterize-statement conn sql type-vec value-vec)]
        (try (let [updated-count (.executeUpdate pstmt)]
               (if (< 1 updated-count)
                 (throw (ex-info (str "Failed to update " table-name)
                                 {:cause {:sql sql :types type-map :values value-map}}))))
             (catch Exception e
               (log/error (ex-info "Exception on update" {:cause {:sql sql :types type-map :values value-map}}))
               (throw e))))
      )))

(defn assert-delete
  [{:keys [table-name type-map value-map where-fields]}]
  (let [where-clause (str " where "
                          (s/join " and " (map #(str (name %) " = ? ") where-fields)))

        sql (format "delete from % %"
                    table-name
                    where-clause)
        type-vec (map (fn [field] [field (get type-map field)])
                      where-fields)
        value-vec (map (fn [field] [field (get value-map field)])
                       where-fields)
        ]
    (with-open [conn (get-connection @db-client/db)]
      (let [pstmt (parameterize-statement conn sql type-vec value-vec)]
        (try (let [updated-count (.executeUpdate pstmt)]
               (log/infof "Deleted %d records from %s" updated-count table-name))
             (catch Exception e
               (log/error (ex-info "Exception on delete" {:cause {:sql sql :types type-map :values value-map}}))
               (throw e)))))))

(defn store-submitter
  [submitter]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :int
               :org_category :string
               :current_name :string
               :current_abbrev :string
               :all_names :string
               :all_abbrevs :string}
        values (merge (select-keys submitter (keys types))
                      {:dirty 1
                       :all_names (json/generate-string (:all_names submitter))
                       :all_abbrevs (json/generate-string (:all_abbrevs submitter))})]
    (assert-insert {:table-name "submitter"
                    :type-map types
                    :value-map values})
    ;(doseq [name (:all_names submitter)]
    ;  (assert-insert {:table-name "submitter_names"
    ;                  :type-map   names-types
    ;                  :value-map  {:submitter_id (:id submitter)
    ;                               :name         name}}))
    ;(doseq [abbrev (:all_abbrevs submitter)]
    ;  (assert-insert {:table-name "submitter_abbrevs"
    ;                  :type-map   abbrevs-types
    ;                  :value-map  {:submitter_id (:id submitter)
    ;                               :abbrev       abbrev}}))
    ;:update (do
    ;          (assert-delete {:table-name   "submitter"
    ;                          :type-map     submitter-types
    ;                          :value-map    submitter
    ;                          :where-fields [:id]})
    ;          (store-submitter :create submitter))
    ))

(defn store-submission
  [submission]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :submission_date :string
               :submitter_id :int}
        values (merge (select-keys submission (keys types))
                      {:dirty 1
                       :additional_submitter_ids (json/generate-string (:all_names submission))})]
    (assert-insert {:table-name "submission"
                    :type-map types
                    :value-map values})))

(defn store-trait
  [trait]
  (let [trait-types {:release_date :string
                     :dirty :int
                     :event_type :string

                     :id :string
                     :medgen_id :string
                     :type :string
                     :name :string
                     :content :string
                     :alternate_names :string
                     :alternate_symbols :string
                     :keywords :string
                     :attribute_content :string
                     :xrefs :string}
        values (merge (select-keys trait (keys trait-types))
                      {:dirty 1
                       :alternate_names (json/generate-string (:alternate_names trait))
                       :alternate_symbols (json/generate-string (:alternate_symbols trait))
                       :keywords (json/generate-string (:keywords trait))
                       :attribute_content (json-string-if-not-string (:attribute_content trait))
                       :xrefs (json-string-if-not-string (:xrefs trait))})]
    (assert-insert {:table-name "trait"
                    :type-map trait-types
                    :value-map values})))

(defn store-trait-set
  [trait-set]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :int
               :type :string
               :content :string
               :trait_ids :string}
        values (merge (select-keys trait-set (keys types))
                      {:dirty 1
                       :trait_ids (json/generate-string (:trait_ids trait-set))})]
    (assert-insert {:table-name "trait_set"
                    :type-map types
                    :value-map values})))

(defn store-clinical-assertion-trait-set
  [clinical-assertion-trait-set]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :type :string
               :content :string
               :clinical_assertion_trait_ids :string}
        values (merge (select-keys clinical-assertion-trait-set (keys types))
                      {:dirty 1
                       :clinical_assertion_trait_ids
                       (json/generate-string (:clinical_assertion_trait_ids clinical-assertion-trait-set))})

        trait-ids-types {:release_date :string
                         :clinical_assertion_trait_set_id :string
                         :clinical_assertion_trait_id :string}
        trait-id-values-seq (map (fn [%] {:release_date (:release_date clinical-assertion-trait-set)
                                          :clinical_assertion_trait_set_id (:id clinical-assertion-trait-set)
                                          :clinical_assertion_trait_id %})
                                 (:clinical_assertion_trait_ids clinical-assertion-trait-set))
        ]
    (assert-insert {:table-name "clinical_assertion_trait_set"
                    :type-map types
                    :value-map values})
    (doseq [v trait-id-values-seq]
      (assert-insert {:table-name "clinical_assertion_trait_set_clinical_assertion_trait_ids"
                      :type-map trait-ids-types
                      :value-map v}))))

(defn store-clinical-assertion-trait
  [clinical-assertion-trait]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :type :string
               :name :string
               :medgen_id :string
               :trait_id :int
               :content :string
               :xrefs :string
               :alternate_names :string}
        values (merge (select-keys clinical-assertion-trait (keys types))
                      {:dirty 1
                       :xrefs (json/generate-string (:xrefs clinical-assertion-trait))
                       :alternate_names (json/generate-string (:alternate_names clinical-assertion-trait))})]
    (assert-insert {:table-name "clinical_assertion_trait"
                    :type-map types
                    :value-map values})))

(defn store-gene
  [gene]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :int
               :hgnc_id :string
               :symbol :string
               :full_name :string}
        values (merge gene {:dirty 1})]
    (assert-insert {:table-name "gene"
                    :type-map types
                    :value-map values})))

(defn store-variation
  [variation]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :name :string
               :variation_type :string
               :subclass_type :string
               :allele_id :string
               :number_of_copies :int
               :content :string

               :protein_changes :string
               :child_ids :string
               :descendant_ids :string}
        values (merge (select-keys variation (keys types))
                      {:dirty 1
                       :protein_changes (json/generate-string (:protein_changes variation))
                       :child_ids (json/generate-string (:child_ids variation))
                       :descendant_ids (json/generate-string (:descendant_ids variation))})]
    (assert-insert {:table-name "variation"
                    :type-map types
                    :value-map values})))

(defn store-gene-association
  [gene-association]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :relationship_type :string
               :source :string
               :content :string
               :variation_id :int
               :gene_id :int}
        values (merge gene-association {:dirty 1})]
    (assert-insert {:table-name "gene_association"
                    :type-map types
                    :value-map values})))

(defn store-variation-archive
  [variation-archive]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :version :int
               :variation_id :int
               :date_created :string
               :date_last_updated :string
               :num_submissions :int
               :num_submitters :int
               :record_status :string
               :review_status :string
               :species :string
               :interp_date_last_evaluated :string
               :interp_type :string
               :interp_description :string
               :interp_explanation :string
               :interp_content :string
               :content :string}
        values (merge variation-archive {:dirty 1})]
    (assert-insert {:table-name "variation_archive"
                    :type-map types
                    :value-map values})))

(defn store-rcv-accession
  [rcv-accession]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :version :int
               :title :text
               :date_last_evaluated :string
               :review_status :string
               :interpretation :string
               :submission_count :int
               :variation_archive_id :string
               :variation_id :int
               :trait_set_id :int}
        values (merge rcv-accession {:dirty 1})]
    (assert-insert {:table-name "rcv_accession"
                    :type-map types
                    :value-map values})))

(defn store-clinical-assertion
  [clinical-assertion]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :version :int
               :internal_id :int
               :title :string
               :local_key :string
               :assertion_type :string
               :date_created :string
               :date_last_updated :string
               :submitted_assembly :string
               :review_status :string
               :interpretation_description :string
               :interpretation_date_last_evaluated :string
               :variation_archive_id :string
               :variation_id :int
               :submitter_id :int
               :submission_id :string
               :rcv_accession_id :string
               :trait_set_id :int
               :clinical_assertion_trait_set_id :string
               :content :string

               :interpretation_comments :string
               :submission_names :string
               :clinical_assertion_observation_ids :string}
        values (merge (select-keys clinical-assertion (keys types))
                      {:dirty 1
                       :interpretation_comments (json/generate-string (:interpretation_comments clinical-assertion))
                       :submission_names (json/generate-string (:submission_names clinical-assertion))
                       :clinical_assertion_observation_ids (json/generate-string
                                                             (:clinical_assertion_observation_ids clinical-assertion))})

        obs-types {:release_date :string
                   :clinical_assertion_id :string
                   :observation_id :string}
        obs-values-seq (map (fn [%] {:release_date (:release_date clinical-assertion)
                                     :clinical_assertion_id (:id clinical-assertion)
                                     :observation_id %})
                            (:clinical_assertion_observation_ids clinical-assertion))]
    ;(println (json/generate-string values))
    (assert-insert {:table-name "clinical_assertion"
                    :type-map types
                    :value-map values})
    (doseq [v obs-values-seq]
      (assert-insert {:table-name "clinical_assertion_observation_ids"
                      :type-map obs-types
                      :value-map v}))))

(defn store-clinical-assertion-observation
  [observation]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :clinical_assertion_trait_set_id :string
               :content :string}
        values (merge observation {:dirty 1})]
    (assert-insert {:table-name "clinical_assertion_observation"
                    :type-map types
                    :value-map values})))

(defn store-trait-mapping
  [trait-mapping]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :clinical_assertion_id :string
               :trait_type :string
               :mapping_type :string
               :mapping_value :string
               :mapping_ref :string
               :medgen_id :string
               :medgen_name :string}
        values (merge trait-mapping {:dirty 1})]
    (assert-insert {:table-name "trait_mapping"
                    :type-map types
                    :value-map values})))

(defn store-clinical-assertion-variation
  [variation]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :variation_type :string
               :subclass_type :string
               :clinical_assertion_id :string
               :content :string

               :child_ids :string
               :descendant_ids :string}
        values (merge variation
                      {:dirty 1
                       :child_ids (json/generate-string (:child_ids variation))
                       :descendant_ids (json/generate-string (:descendant_ids variation))})
        desc-types {:release_date :string
                    :clinical_assertion_variation_id :string
                    :clinical_assertion_variation_descendant_id :string}
        desc-values-seq (map
                          (fn [%] {:release_date (:release_date variation)
                                   :clinical_assertion_variation_id (:id variation)
                                   :clinical_assertion_variation_descendant_id %})
                          (:descendant_ids variation))]
    (assert-insert {:table-name "clinical_assertion_variation"
                    :type-map types
                    :value-map values})
    (doseq [v desc-values-seq]
      (assert-insert {:table-name "clinical_assertion_variation_descendant_ids"
                      :type-map desc-types
                      :value-map v}))
    ))


(defn store-release-sentinel
  [sentinel]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :sentinel_type :string
               :source :string
               :reason :string
               :notes :string

               :rules :string}
        values (merge sentinel
                      {:dirty 1
                       :rules (json/generate-string (:rules sentinel))})]
    (assert-insert {:table-name "release_sentinels"
                    :type-map types
                    :value-map values})))

(defn flatten-kafka-message
  [msg]
  (if (contains? msg :content)
    (merge (dissoc msg :content) (:content msg))))

(defn store-message
  "Receive incoming kafka message"
  [^Map msg]

  (let [; rename type -> event_type
        ; must be first due to use of :type in content fields
        entity (rename-keys msg {:type :event_type})
        ; pull content fields up to top level
        entity (flatten-kafka-message entity)
        ; if no clingen_version, set to 0
        ;entity (if (nil? (:clingen_version entity))
        ;         (assoc entity :clingen_version "0")
        ;         entity)
        ;msg-type (keyword (:type msg))
        entity-type (:entity_type entity)]
    ;(log/infof "store-message: %s %s" entity-type (:id entity))
    ;(log/infof "store-message: %s" entity)
    ;(if (= "release_sentinel" msg-type)
    ;  (do (log/info "Got release sentinel")
    ;      (store-release-sentinel msg)))
    (case entity-type
      "clinical_assertion" (store-clinical-assertion entity)
      "clinical_assertion_observation" (store-clinical-assertion-observation entity)
      "clinical_assertion_trait" (store-clinical-assertion-trait entity)
      "clinical_assertion_trait_set" (store-clinical-assertion-trait-set entity)
      "clinical_assertion_variation" (store-clinical-assertion-variation entity)
      "gene" (store-gene entity)
      "gene_association" (store-gene-association entity)
      "rcv_accession" (store-rcv-accession entity)
      "submission" (store-submission entity)
      "submitter" (store-submitter entity)
      "trait" (store-trait entity)
      "trait_mapping" (store-trait-mapping entity)
      "trait_set" (store-trait-set entity)
      "variation" (store-variation entity)
      "variation_archive" (store-variation-archive entity)
      ; Internal type
      "release_sentinel" (store-release-sentinel entity)
      (log/error "Unknown message entity_type " entity-type ", " (str msg))))
  :ok)


(defn set-union-all
  "Return the set union of all of the provided cols."
  [& cols]
  (loop [todo cols
         output #{}]
    (if (empty? todo)
      output
      (recur (rest todo)
             (set/union output (into #{} (first todo)))))))

(defn- mark-dirtiness
  "where-ands should be a seq of triples [['id' '=' 'scv200'] ['id' '=' 'scv1']]

  Returns the number of records updated
  "
  [table-name where-ands dirtiness]
  (assert (< 0 (count where-ands)))
  (let [dirtiness (Integer/parseInt dirtiness)]
    (assert (in? dirtiness [0 1]))
    (let [update-sql (format "update %s set dirty = %s where %s "
                             table-name
                             dirtiness
                             (s/join " and " (map #(str "(" (nth % 0) (nth % 1) (nth % 2) ")") where-ands)))
          _ (log/debug update-sql)
          updated-counts (execute! @db-client/db [update-sql])]
      (reduce + updated-counts))))

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
        get-variation (fn []
                        (let [s (str "select v.* from variation v
                                where release_date = ? and
                                (dirty = 1 or
                                 exists (
                                   select * from gene_association ga
                                   where ga.dirty = 1 and ga.variation_id = v.id))")
                              dirty-variations (query @db-client/db [s release-date])
                              _ (log/infof "Got %d dirty variations" (count dirty-variations))
                              ; Add the latest gene_association entries for this variant.
                              ; If a gene association's most recent event is a deletion, don't include it.
                              dirty-variations
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
                                                          gene-associations
                                                          []))))
                                   dirty-variations)]
                          ; Set top level release date to max and remove from gene associations
                          (map (fn [variation]
                                 (let [max-release-date (apply obj-max
                                                               (concat [(:release_date variation)]
                                                                       (map #(:release_date %) (:gene_associations variation))))]
                                   (-> variation
                                       (assoc :release_date max-release-date)
                                       ; If a gene association was deleted and the variation is not deleted
                                       ; mark the variation as "update".
                                       ; If the variation was deleted, still pass it on with the new state
                                       ; of the gene associations, but keeping the variation marked deleted.
                                       ((fn [variation]
                                          (let [was-gene-association-deleted
                                                (some #(= "delete" %) (map #(:event_type %)
                                                                           (:gene_associations variation)))]
                                            (if (and was-gene-association-deleted
                                                     (not= "delete" (:event_type variation)))
                                              (assoc variation :event_type "update")
                                              variation))))
                                       (assoc :gene_associations (map #(dissoc % :release_date :event_type :dirty)
                                                                      (:gene_associations variation))))))
                               dirty-variations)
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


(defn dirty-bubble-scv
  "Propagates dirtiness of record A to record B which when aggregated contains A, up
  to clinical_assertion. Only includes records truly 'owned' by the assertion.

  Returns seq of the dirty SCVs regardless of why it was determined to be dirty.

  NOTE: this function does not modify any dirty bits. Calling again will return the same results."
  [release-sentinel]
  ; clinical_assertion_trait -> clinical_assertion_trait_set -> clinical_assertion_observation
  (log/info "bubbling dirty sub-records for release-sentinel: " release-sentinel)
  (let [release-date (get release-sentinel :release_date)
        _ (log/info "release_date: " (str release-date))
        traits-fn (fn [release-date]
                    (let [query-str (str "select * from clinical_assertion_trait t "
                                         " where t.release_date = ? "
                                         " and t.dirty = 1")
                          _ (log/debug query-str)
                          traits (query @db-client/db [query-str release-date] {:keywordize? true})]
                      (log/infof "Got %d dirty traits" (count traits))
                      traits))
        trait-sets-fn (fn [release-date traits]
                        "traits is a seq of maps containing at least :id of traits"
                        (loop [trait-batches (partition-all 1000 traits)
                               trait-sets []]
                          (if (empty? trait-batches)
                            trait-sets
                            (let [trait-set-sql
                                  (str "select * from clinical_assertion_trait_set ts "
                                       ; In current release and is dirty
                                       "where (ts.release_date = ? and ts.dirty = 1) "
                                       "or ( "
                                       ; Has a trait updated in this release
                                       "  exists "
                                       "  (select * from clinical_assertion_trait_set_clinical_assertion_trait_ids ti "
                                       ; Release date is either current or most recent and trait is dirty
                                       "   where (ti.release_date = ts.release_date or "
                                       "          ti.release_date = (select max(release_date) "
                                       "            from clinical_assertion_trait_set_clinical_assertion_trait_ids ti2"
                                       "            where ti2.clinical_assertion_trait_set_id = ts.id)) "
                                       "     and ti.clinical_assertion_trait_set_id = ts.id "
                                       "     and ti.clinical_assertion_trait_id in ("
                                       (s/join "," (map #(str "'" (:id %) "'") (first trait-batches))) ; quoted trait ids
                                       "     )"             ; end in
                                       "  ) "               ; end exists
                                       ")")                 ; end or
                                  _ (log/debug trait-set-sql)
                                  new-trait-sets (query @db-client/db [trait-set-sql release-date])]
                              (log/infof "Got %d dirty trait sets" (count new-trait-sets))
                              (recur
                                (rest trait-batches)
                                (concat trait-sets new-trait-sets))))))
        observations-fn (fn [release-date trait-sets]
                          (loop [trait-set-batches (partition-all 1000 trait-sets)
                                 observations []]
                            (if (empty? trait-set-batches)
                              observations
                              (let [observation-sql
                                    (str "select * from clinical_assertion_observation o "
                                         "where (o.release_date = ? and o.dirty = 1) "
                                         "or ( "
                                         "  exists ( "
                                         "    select * from clinical_assertion_trait_set ts "
                                         "    where (ts.release_date = o.release_date or "
                                         "           ts.release_date = (select max(release_date) "
                                         "                              from clinical_assertion_trait_set ts2 "
                                         "                              where ts2.id = ts.id)) "
                                         "    and ts.id = o.clinical_assertion_trait_set_id "
                                         "    and ts.id in ( "
                                         (s/join "," (map #(str "'" (:id %) "'") (first trait-set-batches)))
                                         "    ) "
                                         "  ) "
                                         ")")
                                    _ (log/debug observation-sql)
                                    new-observations (query @db-client/db [observation-sql release-date])]
                                (log/infof "Got %d dirty observations" (count new-observations))
                                (recur
                                  (rest trait-set-batches)
                                  (concat observations new-observations))))))
        trait-mappings-fn (fn [release-date]
                            (let [trait-mapping-sql
                                  (str "select * from trait_mapping "
                                       "where release_date = ? and dirty = 1")
                                  _ (log/debug trait-mapping-sql)
                                  trait-mappings (query @db-client/db [trait-mapping-sql release-date])]
                              (log/infof "Got %d dirty trait mappings" (count trait-mappings))
                              trait-mappings))
        ca-variation-fn (fn [release-date]
                          (let [variation-sql
                                (str "select * from clinical_assertion_variation v1 "
                                     "where (v1.release_date = ? and v1.dirty = 1) "
                                     ; Has a descendant that is dirty in any release
                                     "or exists ( "
                                     "  select * from clinical_assertion_variation_descendant_ids des "
                                     "  left join clinical_assertion_variation v2 "
                                     "  on v2.id = des.clinical_assertion_variation_descendant_id "
                                     ;"  and v2.release_date = des.release_date "
                                     "  where des.clinical_assertion_variation_id = v2.id "
                                     "  and des.release_date = v1.release_date "
                                     "  and v2.dirty = 1"
                                     ") ")
                                _ (log/debug variation-sql)
                                variations (query @db-client/db [variation-sql release-date])]
                            (log/infof "Got %d dirty variations" (count variations))
                            variations
                            ))
        ca-fn (fn [{:keys [release-date
                           observations
                           ca-trait-sets
                           ca-trait-mappings
                           ca-variations]}]
                ; For each dirty subrecord, get the latest corresponding clinical assertion
                ; and combine to a set of distinct clinical assertions
                (let [dirty (query @db-client/db
                                   [(str "select * from clinical_assertion "
                                         "where release_date = ? and dirty = 1")
                                    release-date])
                      _ (log/debugf "clinical_assertions dirty based on them selves: %d" (count dirty))
                      dirty-obs (loop [batches (partition-all 1000 observations)
                                       output []]
                                  (if (empty? batches)
                                    output
                                    (let [ins (s/join "," (map #(str "'" (:id %) "'") (first batches)))
                                          sql (str "select * from clinical_assertion ca "
                                                   "where exists ( "
                                                   "  select * from clinical_assertion_observation_ids oi "
                                                   "  left join clinical_assertion_observation o "
                                                   "  on o.id = oi.observation_id "
                                                   "  and o.release_date = oi.release_date "
                                                   "  where oi.clinical_assertion_id = ca.id "
                                                   "  and oi.observation_id in (" ins ")) "
                                                   "and ca.release_date = (select max(release_date) "
                                                   "                       from clinical_assertion "
                                                   "                       where id = ca.id)")]
                                      (log/debug sql)
                                      (recur (rest batches)
                                             (concat output (query @db-client/db [sql]))))))
                      _ (log/debugf "clinical_assertions dirty based on observations: %d" (count dirty-obs))
                      dirty-trait-sets (loop [batches (partition-all 1000 ca-trait-sets)
                                              output []]
                                         (if (empty? batches)
                                           output
                                           (let [ins (s/join "," (map #(str "'" (:id %) "'") (first batches)))
                                                 sql (str "select ca.* from clinical_assertion ca "
                                                          "left join clinical_assertion_trait_set ts "
                                                          "on ts.id = ca.clinical_assertion_trait_set_id "
                                                          "and ts.release_date = ca.release_date "
                                                          "where ts.id in (" ins ") "
                                                          "and ca.release_date = (select max(release_date) "
                                                          "                       from clinical_assertion "
                                                          "                       where id = ca.id)")]
                                             (log/debug sql)
                                             (recur (rest batches)
                                                    (concat output (query @db-client/db [sql]))))))
                      _ (log/debugf "clinical_assertions dirty based on trait sets (and traits): %d" (count dirty-trait-sets))
                      dirty-trait-mapping (loop [batches (partition-all 1000 ca-trait-mappings)
                                                 output []]
                                            (if (empty? batches)
                                              ; Multiple trait mappings per SCV, filter to unique SCVs
                                              (distinct output)
                                              (let [ins (s/join "," (map #(str "'" (:clinical_assertion_id %) "'")
                                                                         (first batches)))
                                                    sql (str "select ca.* from clinical_assertion ca "
                                                             "left join trait_mapping tm "
                                                             "on tm.clinical_assertion_id = ca.id "
                                                             "where tm.clinical_assertion_id in (" ins ") "
                                                             "and ca.release_date = (select max(release_date) "
                                                             "                       from clinical_assertion "
                                                             "                       where id = ca.id)")]
                                                (log/debug sql)
                                                (recur (rest batches)
                                                       (concat output (query @db-client/db [sql]))))))
                      _ (log/debugf "clinical_assertions dirty based on trait mappings: %d" (count dirty-trait-mapping))
                      dirty-variation (loop [batches (partition-all 1000 ca-variations)
                                             output []]
                                        (if (empty? batches)
                                          output
                                          (let [ins (s/join "," (map #(str "'" (:id %) "'") (first batches)))
                                                sql (str "select ca.* from clinical_assertion ca "
                                                         "where exists ( "
                                                         "  select * from clinical_assertion_variation v"
                                                         "  where v.id in (" ins ")"
                                                         "  and v.clinical_assertion_id = ca.id"
                                                         ") "
                                                         "and ca.release_date = (select max(release_date) "
                                                         "                       from clinical_assertion "
                                                         "                       where id = ca.id)")]
                                            (log/debug sql)
                                            (recur (rest batches)
                                                   (concat output (query @db-client/db [sql]))))))
                      _ (log/debugf "clinical_assertions dirty based on variations: %d" (count dirty-variation))
                      ca-union (set-union-all dirty
                                              dirty-obs
                                              dirty-trait-sets
                                              dirty-trait-mapping
                                              dirty-variation)
                      _ (log/debugf "clinical_assertions dirty in total (%s): %d" release-date (count ca-union))
                      ]
                  ca-union))

        traits (traits-fn release-date)
        trait-sets (trait-sets-fn release-date traits)
        observations (observations-fn release-date trait-sets)
        trait-mappings (trait-mappings-fn release-date)
        variations (ca-variation-fn release-date)
        ]
    (log/infof "Got %d total dirty traits" (count traits))
    (log/infof "Got %d total dirty trait sets" (count trait-sets))
    (log/infof "Got %d total dirty observations" (count observations))
    (log/infof "Got %d total dirty trait mappings" (count trait-mappings))
    (log/infof "Got %d total dirty variations" (count variations))

    (let [dirty-clinical-assertions (ca-fn {:release-date release-date
                                            :observations observations
                                            :ca-trait-sets trait-sets
                                            :ca-trait-mappings trait-mappings
                                            :ca-variations variations})]

      dirty-clinical-assertions)))


(defn simplify-dollar-map [m]
  "Return (get m :$) if m is a map and :$ is the only key. Otherwise return m."
  (if (and (map? m)
           (= '(:$) (keys m)))
    (:$ m)
    m))

(defn as-vec-if-not [val]
  "If val is not a seq, return it in a vector."
  (if (and (not (string? val))
           ; seq? should only return true for things that are themselves seqs, not all seqable
           (seq? val))
    val [val]))

(defn validate-variation-tree
  [variation]
  ; TODO check subclasses
  ;(let [others (filterv #(not (in? (:subclass_type %) ["Genotype" "Haplotype" "SimpleAllele"]))
  ;                      variations)])
  ;(if (< 0 (count others))
  ;      (throw (ex-info "Found variations for assertion of unknown subclass type"
  ;                      {:variations variations :unknown others}))
  ; TODO check no variation appears twice
  ; TODO check genotype->haplotype->simpleallele expected topological order
  )

(defn variation-list-to-compound
  "Takes a collection of clinical assertion variations and nests the child variations (^String :child_ids)
  under compound variations as a vector on the key :child-variations of the parent."
  [variations]
  (let [variations (filter #(not (nil? %)) variations)]
    (if (= 0 (count variations))
     nil
     (let [variations (mapv (fn [v] (let [child-ids (into [] (json/parse-string (:child_ids v)))]
                                      (if (< 0 (count child-ids))
                                        (assoc v :child_ids child-ids)
                                        (dissoc v :child_ids))))
                            (mapv #(assoc % :parent_ids []) variations))
           id-to-variation (atom (into {} (map #(vector (:id %) %) variations)))]

       ; Add reverse relations from children to parents
       (doseq [[id v] (into {} @id-to-variation)]           ; Copy value of id-to-variation, not sure if necessary
         (when-let [child-ids (:child_ids v)]
           (log/debug "Updating child variations of " (:id v) (into [] child-ids))
           (doseq [child-id child-ids]
             ; Update child variation to have current id in its parent_ids vector
             (do (log/debugf "Adding variation id %s as parent of %s" id child-id)
                 (swap! id-to-variation (fn [old]
                                          (let [child-variation (get old child-id)]
                                            (assoc old child-id (assoc child-variation :parent_ids
                                                                                       (conj (:parent_ids child-variation)
                                                                                             id))))))))))
       ; Remove empty parent_ids vectors
       (reset! id-to-variation (into {} (map (fn [[k v]]
                                               [k (if (= 0 (count (:parent_ids v)))
                                                    (dissoc v :parent_ids) v)])
                                             @id-to-variation)))

       (log/info "id-to-variation: " @id-to-variation)

       ; Root variation is the one with no parent_ids field (was removed above)
       (let [root (into {} (filter #(nil? (:parent_ids (second %))) @id-to-variation))]
         (if (not= 1 (count root))
           (throw (ex-info "Could not determine root variation in variation list"
                           {:id-to-variation @id-to-variation}))
           (let [[root-id root-variation] (first root)]
             (letfn [(add-children-fn [variation]
                       (log/infof "variation: %s child_ids: %s" (:id variation) (:child_ids variation))
                       (let [child-ids (:child_ids variation)]
                         (if child-ids
                           (let [child-variations (mapv #(get @id-to-variation %) child-ids)
                                 child-variations (mapv #(dissoc % :parent_ids) child-variations)]
                             (log/info "child-variations: " child-variations)
                             (if (some #(= nil %) child-variations)
                               (throw (ex-info (format "Variation referred to child id not found")
                                               {:child-ids child-ids :ids (keys @id-to-variation)}))
                               (let [recursed-child-variations (mapv #(if (:child_ids %) (add-children-fn %) %)
                                                                     child-variations)]
                                 (assoc variation :child_variations recursed-child-variations))))
                           variation)))]
               (log/info "Adding children to root variation recursively")
               (add-children-fn root-variation)
               ))))
       )))
  )

(defn post-process-built-clinical-assertion
  "Perform clean up operations, field value parsing, and version reconciliation on
  the output of build-clinical-assertion records. This should almost always be called."
  [assertion]
  (log/debug "Post processing scv: " (json/generate-string assertion))
  (log/debug "Adding collection methods and allele origins")
  (let [observations (:clinical_assertion_observations assertion)
        observations (map (fn [observation]
                            (assoc observation :parsed_content (json/parse-string (:content observation) true)))
                          observations)
        log-fn (fn [v] (log/info (into [] v)) v)
        method-types (->> observations
                          (map #(get-in % [:parsed_content :Method]))
                          (map #(as-vec-if-not %))
                          (flatten)
                          (map #(:MethodType %))
                          (filter #(not (nil? %)))
                          (map #(simplify-dollar-map %))
                          (distinct))
        allele-origins (->> observations
                            (map #(get-in % [:parsed_content :Sample]))
                            (map #(as-vec-if-not %))
                            (flatten)
                            (map #(:Origin %))
                            (filter #(not (nil? %)))
                            (map #(simplify-dollar-map %))
                            (distinct))]
    (assoc assertion :collection_methods method-types
                     :allele_origins allele-origins))

  (log/debug "Bubbling up max release date to top level")
  (let [release-dates (filterv #(not (nil? %))
                               (concat [(:release_date assertion)]
                                       (map #(:release_date %) (:clinical_assertion_observations assertion))
                                       (map #(:release_date %) (:clinical_assertion_variations assertion))
                                       ;:clinical_assertion_trait_set (in observation and top level)
                                       (map #(:release_date %)
                                            (flatten
                                              (map #(:clinical_assertion_trait_set %) ; vec
                                                   (conj (:clinical_assertion_observations assertion)
                                                         assertion))))
                                       ;:clinical_assertion_traits (in observation and top level)
                                       (map (fn [t] (:release_date t))
                                            (flatten
                                              (map (fn [ts] (:clinical_assertion_traits ts))
                                                   (flatten
                                                     (map (fn [o] (:clinical_assertion_trait_set o)) ; vec
                                                          (conj (:clinical_assertion_observations assertion)
                                                                assertion))))))
                                       )
                               )
        max-release-date (apply obj-max release-dates)]
    (log/debugf "Assertion record release dates: %s , max is %s" (json/generate-string release-dates) max-release-date)
    (letfn [(clean-trait-set [trait-set]
              (dissoc
                (assoc trait-set :clinical_assertion_traits
                                 (map (fn [t] (dissoc t :release_date :dirty :event_type))
                                      (:clinical_assertion_traits trait-set)))
                :release_date :dirty :event_type))]
      (-> assertion
          (assoc :release_date max-release-date)
          ; Remove :release_date, :dirty, :event_type from sub-records
          (assoc :clinical_assertion_trait_set (clean-trait-set (:clinical_assertion_trait_set assertion)))
          (assoc :clinical_assertion_observations (map #(-> %
                                                            (dissoc :release_date :dirty :event_type)
                                                            (assoc :clinical_assertion_trait_set
                                                                   (clean-trait-set (:clinical_assertion_trait_set %))))
                                                       (:clinical_assertion_observations assertion)))
          (assoc :clinical_assertion_variations (map #(dissoc % :release_date :dirty :event_type)
                                                     (:clinical_assertion_variations assertion)))
          ; Set entity_type used by downstream processors
          (assoc :entity_type "clinical_assertion")
          ; If assertion event is delete, set deleted
          ((fn [a] (if (= "delete" (:event_type a)) (assoc a :deleted true) a)))
          ; Remove internal fields from assertion
          (dissoc :event_type :dirty)))))

(defn build-clinical-assertion
  "Takes a clinical assertion datified record as returned by sink/dirty-bubble-scv, and
  attaches all sub-records regardless of dirty status."
  [clinical-assertion]
  ; For the clinical assertion record val, combine all linked entities
  (log/debug "building clinical assertion" (:id clinical-assertion))
  (let [scv-id (:id clinical-assertion)
        release-date (:release_date clinical-assertion)]
    (let [obs-traitset-fn (fn [observation]
                            (let [sql (str "select ts.* from clinical_assertion_trait_set ts "
                                           "where ts.id = ? "
                                           "and ts.release_date = (select max(release_date) "
                                           "                       from clinical_assertion_trait_set "
                                           "                       where id = ts.id) "
                                           "and ts.event_type <> 'delete'")]
                              (log/debug sql)
                              (let [rs (query @db-client/db [sql (:clinical_assertion_trait_set_id observation)])]
                                (if (< 1 (count rs))
                                  (throw (ex-info "Trait set query for observation returned more than one result"
                                                  {:observation observation :sql sql :trait_sets rs})))
                                (assoc observation :clinical_assertion_trait_set (first rs)))))
          traitset-trait-fn (fn [trait-set]
                              (log/debug "looking for traits for trait set" (json/generate-string trait-set))
                              (let [sql (str "select t.* from clinical_assertion_trait_set_clinical_assertion_trait_ids tsti "
                                             "left join "
                                             "  (select * from clinical_assertion_trait t "
                                             "   where t.release_date = (select max(release_date) "
                                             "                           from clinical_assertion_trait where id = t.id)"
                                             "   ) t "
                                             "on t.id = tsti.clinical_assertion_trait_id "
                                             "where tsti.clinical_assertion_trait_set_id = ? "
                                             "and tsti.release_date = (select max(release_date) "
                                             "                         from clinical_assertion_trait_set_clinical_assertion_trait_ids "
                                             "                         where clinical_assertion_trait_set_id = tsti.clinical_assertion_trait_set_id "
                                             "                         and clinical_assertion_trait_id = tsti.clinical_assertion_trait_id) "
                                             "and t.event_type <> 'delete'")]
                                (log/debug sql)
                                (let [updated-trait-set (assoc trait-set :clinical_assertion_traits
                                                                         (query @db-client/db [sql (:id trait-set)]))]
                                  (log/debug "updated trait set" (json/generate-string updated-trait-set))
                                  updated-trait-set)))

          clinical-assertion
          (assoc clinical-assertion
            :clinical_assertion_observations
            (let [sql (str "select o.* from clinical_assertion_observation_ids oi "
                           "left join "
                           "  (select * from clinical_assertion_observation o "
                           "   where o.release_date = (select max(release_date) "
                           "                           from clinical_assertion_observation where id = o.id)"
                           "  ) o "
                           "on o.id = oi.observation_id "
                           "where oi.clinical_assertion_id = ? "
                           "and oi.release_date = (select max(release_date) "
                           "                       from clinical_assertion_observation_ids "
                           "                       where clinical_assertion_id = oi.clinical_assertion_id "
                           "                       and observation_id = oi.observation_id) "
                           "and o.event_type <> 'delete'")]
              (log/debug sql)
              ; Update observations with trait sets
              ; Update trait sets with traits
              (let [observations (query @db-client/db [sql scv-id])
                    ; add :clinical_assertion_trait_set to each
                    observations-with-ts (map #(obs-traitset-fn %) observations)]
                ; update each :clinical_assertion_trait_set obj to also have :clinical_assertion_traits
                (map (fn [observation]
                       (let [trait-set-with-traits (traitset-trait-fn (:clinical_assertion_trait_set observation))]
                         (log/debug "updated observation trait sets" (json/generate-string trait-set-with-traits))
                         (assoc observation :clinical_assertion_trait_set trait-set-with-traits)))
                     observations-with-ts))))

          clinical-assertion
          (assoc clinical-assertion :clinical_assertion_trait_set
                                    ; If no trait set id, just set it null
                                    (if (:clinical_assertion_trait_set_id clinical-assertion)
                                      (let [sql (str "select ts.* from clinical_assertion_trait_set ts "
                                                     "where ts.id = ? "
                                                     "and ts.release_date = (select max(release_date) "
                                                     "                       from clinical_assertion_trait_set "
                                                     "                       where id = ts.id) "
                                                     "and ts.event_type <> 'delete'")]
                                        (log/debug sql)
                                        (let [ts (query @db-client/db [sql (:clinical_assertion_trait_set_id clinical-assertion)])]
                                          (if (< 1 (count ts))
                                            (throw (ex-info "clinical_assertion->clinical_assertion_trait_set returned multiple records"
                                                            {:trait_sets ts})))
                                          (let [ts-with-traits (traitset-trait-fn (first ts))]
                                            (log/debug "top level updated trait set with traits"
                                                       (json/generate-string ts-with-traits))
                                            ts-with-traits)
                                          ))))

          clinical-assertion
          (assoc clinical-assertion :clinical_assertion_variations
                                    (let [sql (str "select v.* from clinical_assertion_variation v "
                                                   "where v.clinical_assertion_id = ? "
                                                   "and v.release_date = (select max(release_date) "
                                                   "                      from clinical_assertion_variation "
                                                   "                      where id = v.id) "
                                                   "and v.event_type <> 'delete'")]
                                      (log/debug sql)
                                      (query @db-client/db [sql scv-id])))

          ; Process some internal fields
          ;clinical-assertion (assoc clinical-assertion :entity_type "clinical_assertion")

          clinical-assertion
          (assoc clinical-assertion :submission_names
                                    (json/parse-string (:submission_names clinical-assertion)))
          clinical-assertion
          (assoc clinical-assertion :interpretation_comments
                                    (json/parse-string (:interpretation_comments clinical-assertion)))
          clinical-assertion
          (assoc clinical-assertion :clinical_assertion_observation_ids
                                    (json/parse-string (:clinical_assertion_observation_ids clinical-assertion)))
          ]
      clinical-assertion)
    )
  )
