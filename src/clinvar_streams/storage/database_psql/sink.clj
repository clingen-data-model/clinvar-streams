(ns clinvar-streams.storage.database-psql.sink
  (:require [clinvar-streams.storage.database-psql.client :as db-client]
            [clinvar-streams.util :refer :all]
            [cheshire.core :as json]
            [clojure.java.jdbc :refer :all :exclude [get-connection]]
            [clojure.core.async :as async]
            [clojure.string :as s]
            [clojure.pprint :refer [pprint]]
            [taoensso.timbre :as log])
  (:import (java.sql PreparedStatement Connection SQLException)))

(defn get-connection
  "Overrides clojure.java.jdbc/get-connection"
  [db-spec]
  (.getConnection @db-client/datasource)
  )

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

; TODO check against :create :delete :update fields from kafka message
;(defn entity-exists
;  [entity]
;  (let [entity-type (:entity_type entity)]
;    (case entity-type
;      "clinical_assertion" ()))
;  )


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
(defn assert-insert
  [{:keys [table-name type-map value-map]}]
  (let [sql (format "insert into %s(%s) values(%s)"
                    table-name
                    (s/join "," (into [] (map #(name %) (keys type-map))))
                    (s/join "," (into [] (map (fn [%] "?") (keys type-map)))))]
    ;(println @db-client/db)
    (with-open [conn (get-connection @db-client/db)]
      (let [pstmt (parameterize-statement conn sql type-map value-map)]
        (try (let [updated-count (.executeUpdate pstmt)]
               (if (not= 1 updated-count)
                 (throw (ex-info (str "Failed to insert " table-name)
                                 {:cause {:sql sql :types type-map :values value-map}}))))
             (catch Exception e
               (log/error (ex-info "Exception on insert" {:cause {:sql sql :types type-map :values value-map}}))
               (throw e)))))))

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

;(defmulti assert-crud (fn [crud-op mapargs] crud-op))
;(defmethod assert-crud :create [crud-op mapargs] (assert-insert mapargs))
;(defmethod assert-crud :update [crud-op mapargs] (assert-update mapargs))

(defn store-clinical-assertion
  [crud-op clinical-assertion]
  (let [type-map {:id                                 :string
                  :version                            :int
                  :internal_id                        :int
                  :title                              :string
                  :local_key                          :string
                  :assertion_type                     :string
                  :date_created                       :string
                  :date_last_updated                  :string
                  :submitted_assembly                 :string
                  :review_status                      :string
                  :interpretation_description         :string
                  :interpretation_date_last_evaluated :string
                  :variation_archive_id               :string
                  :variation_id                       :int
                  :submitter_id                       :int
                  :submission_id                      :string
                  :rcv_accession_id                   :string
                  :trait_set_id                       :int
                  :clinical_assertion_trait_set_id    :string
                  :content                            :string}]
    (case crud-op
      :create (assert-insert {:table-name "clinical_assertion"
                              :type-map   type-map
                              :value-map  clinical-assertion})
      :update (assert-update {:table-name   "clinical_assertion"
                              :type-map     type-map
                              :value-map    clinical-assertion
                              :where-fields [:id]}))

    ))

(defn store-submitter
  [crud-op submitter]
  (let [submitter-types {:id             :int
                         :org_category   :string
                         :current_name   :string
                         :current_abbrev :string}
        names-types {:submitter_id :int
                     :name         :string}
        abbrevs-types {:submitter_id :int
                       :abbrev       :string}]
    (case crud-op
      :create (do
                (assert-insert {:table-name "submitter"
                                :type-map   submitter-types
                                :value-map  submitter})
                (doseq [name (:all_names submitter)]
                  (assert-insert {:table-name "submitter_names"
                                  :type-map   names-types
                                  :value-map  {:submitter_id (:id submitter)
                                               :name         name}}))
                (doseq [abbrev (:all_abbrevs submitter)]
                  (assert-insert {:table-name "submitter_abbrevs"
                                  :type-map   abbrevs-types
                                  :value-map  {:submitter_id (:id submitter)
                                               :abbrev       abbrev}})))
      :update (do
                (assert-delete {:table-name   "submitter"
                                :type-map     submitter-types
                                :value-map    submitter
                                :where-fields [:id]})
                (store-submitter :create submitter))
      )))

(defn store-clinical-assertion-observation
  [crud-op observation]
  (let [types {:id                              :string
               :clinical_assertion_trait_set_id :string
               :content                         :string}]
    (case crud-op
      :create (assert-insert {:table-name "clinical_assertion_observation"
                              :type-map   types
                              :value-map  observation})
      :update (assert-update {:table-name   "clinical_assertion_observation"
                              :type-map     types
                              :value-map    observation
                              :where-fields [:id]}))
    ))

(defn store-clinical-assertion-trait
  [crud-op clinical-assertion-trait]
  (let [types {:id        :string
               :type      :string
               :name      :string
               :medgen_id :string
               :trait_id  :int
               :content   :string}
        xrefs-types {:clinical_assertion_trait_id :string
                     :xref                        :string}
        alternate-names-types {:clinical_assertion_trait_id :string
                               :alternate_name              :string}]
    (case crud-op
      :create (do
                (assert-insert {:table-name "clinical_assertion_trait"
                                :type-map   types
                                :value-map  clinical-assertion-trait})
                (doseq [xref (:xrefs clinical-assertion-trait)]
                  (assert-insert {:table-name "clinical_assertion_trait_xrefs"
                                  :type-map   xrefs-types
                                  :value-map  {:clinical_assertion_trait_id (:id clinical-assertion-trait)
                                               :xref                        xref}}))
                (doseq [alternate-name (:alternate_names clinical-assertion-trait)]
                  (assert-insert {:table-name "clinical_assertion_trait_alternate_names"
                                  :type-map   alternate-names-types
                                  :value-map  {:clinical_assertion_trait_id (:id clinical-assertion-trait)
                                               :name                        alternate-name}})))
      :update (do
                (assert-delete {:table-name   "clinical_assertion_trait"
                                :type-map     types
                                :value-map    clinical-assertion-trait
                                :where-fields [:id]})
                (store-clinical-assertion-trait :create clinical-assertion-trait))
      )))

(defn store-clinical-assertion-trait-set
  [crud-op clinical-assertion-trait-set]
  (let [types {:id      :string
               :type    :string
               :content :string}
        mapping-types {:clinical_assertion_trait_set_id :string
                       :clinical_assertion_trait_id     :string}]
    (case crud-op
      :create (do
                (assert-insert {:table-name "clinical_assertion_trait_set"
                                :type-map   types
                                :value-map  clinical-assertion-trait-set})
                (doseq [trait-id (:clinical_assertion_trait_ids clinical-assertion-trait-set)]
                  (assert-insert {:table-name "clinical_assertion_trait_set_clinical_assertion_traits"
                                  :type-map   mapping-types
                                  :value-map  {:clinical_assertion_trait_set_id (:id clinical-assertion-trait-set)
                                               :clinical_assertion_trait_id     trait-id}})))
      :update (do
                (assert-delete {:table-name   "clinical_assertion_trait_set"
                                :type-map     types
                                :value-map    clinical-assertion-trait-set
                                :where-fields [:id]})
                (store-clinical-assertion-trait-set :create clinical-assertion-trait-set))
      )
    ))

(defn store-clinical-assertion-variation
  [crud-op variation]
  (let [variation-types {:id                    :string
                         :variation_type        :string
                         :subclass_type         :string
                         :clinical_assertion_id :string
                         :content               :string}
        child-types {:clinical_assertion_variation_id       :string
                     :clinical_assertion_variation_child_id :string}
        descendant-types {:clinical_assertion_variation_id            :string
                          :clinical_assertion_variation_descendant_id :string}]
    (case crud-op
      :create (do
                (assert-insert {:table-name "clinical_assertion_variation"
                                :type-map   variation-types
                                :value-map  variation})
                (doseq [child-id (:child_ids variation)]
                  (assert-insert {:table-name "clinical_assertion_variation_child_ids"
                                  :type-map   child-types
                                  :value-map  {:clinical_assertion_variation_id       (:id variation)
                                               :clinical_assertion_variation_child_id child-id}}))
                (doseq [descendant-id (:descendant_ids variation)]
                  (assert-insert {:table-name "clinical_assertion_variation_descendant_ids"
                                  :type-map   descendant-types
                                  :value-map  {:clinical_assertion_variation_id            (:id variation)
                                               :clinical_assertion_variation_descendant_id descendant-id}})))
      :update (do
                (assert-delete {:table-name   "clinical_assertion_variation"
                                :type-map     variation-types
                                :value-map    variation
                                :where-fields [:id]})
                (store-clinical-assertion-variation :create variation))
      )))

(defn store-variation
  [crud-op variation]
  (let [variation-types {:id               :int
                         :variation_type   :string
                         :subclass_type    :string
                         :allele_id        :int
                         :number_of_copies :int
                         :content          :string
                         }
        protein-change-types {:variation_id   :int
                              :protein_change :string}
        child-types {:variation_id       :int
                     :variation_child_id :int}
        descendant-types {:variation_id            :int
                          :variation_descendant_id :int}
        protein-changes (map (fn [pc] {:variation_id   (:id variation)
                                       :protein_change pc})
                             (:protein_change variation))
        ;_ (pprint protein-change-types)
        ;_ (pprint protein-changes)
        ]
    (case crud-op
      :create (do
                (assert-insert {:table-name "variation"
                                :type-map   variation-types
                                :value-map  variation})
                (doseq [protein-change (:protein_change variation)]
                  (assert-insert {:table-name "variation_protein_changes"
                                  :type-map   protein-change-types
                                  :value-map  {:variation_id   (:id variation)
                                               :protein_change protein-change}}))
                (doseq [child-id (:child_ids variation)]
                  (assert-insert {:table-name "variation_child_ids"
                                  :type-map   child-types
                                  :value-map  {:variation_id       (:id variation)
                                               :variation_child_id child-id}}))
                (doseq [descendant-id (:descendant_ids variation)]
                  (assert-insert {:table-name "variation_descendant_ids"
                                  :type-map   descendant-types
                                  :value-map  {:variation_id            (:id variation)
                                               :variation_descendant_id descendant-id}})))
      :update (do
                (assert-delete {:table-name   "variation"
                                :type-map     variation-types
                                :value-map    variation
                                :where-fields [:id]})
                (store-variation :create variation))
      )))

(defn store-gene
  [crud-op gene]
  (let [gene-types {:id        :int
                    :hgnc_id   :string
                    :symbol    :string
                    :full_name :string}]
    (case crud-op
      :create (assert-insert {:table-name "gene"
                              :type-map   gene-types
                              :value-map  gene})
      :update (assert-update {:table-name   "gene"
                              :type-map     gene-types
                              :value-map    gene
                              :where-fields [:id]})
      )))

(defn store-gene-association
  [crud-op gene-association]
  (let [types {:relationship_type :string
               :source            :string
               :content           :string
               :variation_id      :int
               :gene_id           :int}]
    (case crud-op
      :create (assert-insert {:table-name "gene_association"
                              :type-map   types
                              :value-map  gene-association})
      :update (assert-update {:table-name   "gene_association"
                              :type-map     types
                              :value-map    gene-association
                              :where-fields [:variation_id :gene_id]})
      )))

(defn store-rcv-accession
  [crud-op rcv-accession]
  (let [types {:id                   :string
               :version              :int
               :title                :text
               :date_last_evaluated  :string
               :review_status        :string
               :interpretation       :string
               :submission_count     :int
               :variation_archive_id :string
               :variation_id         :int
               :trait_set_id         :int}]
    (case crud-op
      :create (assert-insert {:table-name "rcv_accession"
                              :type-map   types
                              :value-map  rcv-accession})
      :update (assert-update {:table-name   "rcv_accession"
                              :type-map     types
                              :value-map    rcv-accession
                              :where-fields [:id]})
      )))

(defn store-submission
  [crud-op submission]
  (let [types {:id              :string
               :submission_date :string
               :submitter_id    :int}
        additional-submitter-types {:submission_id :int
                                    :submitter_id  :int}]
    (case crud-op
      :create (do
                (assert-insert {:table-name "submission"
                                :type-map   types
                                :value-map  submission})
                (doseq [submitter-id (:additional_submitter_ids submission)]
                  (assert-insert {:table-name "submission_additional_submitters"
                                  :type-map   additional-submitter-types
                                  :value-map  {:submission_id (:id submission)
                                               :submitter_id  submitter-id}})))
      :update (do
                (assert-delete {:table-name   "submission"
                                :type-map     types
                                :value-map    submission
                                :where-fields [:id]})
                (store-submission :create submission))
      )))

(defn store-trait
  [crud-op trait]
  (let [trait-types {:id        :string
                     :medgen_id :string
                     :type      :string
                     :name      :string
                     :content   :string}
        alternate-name-types {:trait_id       :string
                              :alternate_name :string}
        alternate-symbol-types {:trait_id         :string
                                :alternate_symbol :string}
        keyword-types {:trait_id :string
                       :keyword  :string}
        attribute-content-types {:trait_id          :string
                                 :attribute_content :string}
        xref-types {:trait_id :string
                    :xref     :string}]
    (case crud-op
      :create (do
                (assert-insert {:table-name "trait"
                                :type-map   trait-types
                                :value-map  trait})
                (doseq [alternate-name (:alternate_names trait)]
                  (assert-insert {:table-name "trait_alternate_names"
                                  :type-map   alternate-name-types
                                  :value-map  {:trait_id       (:id trait)
                                               :alternate_name alternate-name}}))
                (doseq [alternate-symbol (:alternate_symbols trait)]
                  (assert-insert {:table-name "trait_alternate_symbol"
                                  :type-map   alternate-symbol-types
                                  :value-map  {:trait_id         (:id trait)
                                               :alternate_symbol alternate-symbol}}))
                (doseq [keyword (:keywords trait)]
                  (assert-insert {:table-name "trait_keywords"
                                  :type-map   keyword-types
                                  :value-map  {:trait_id (:id trait)
                                               :keyword  keyword}}))
                (doseq [attribute-content (:attribute_content trait)]
                  (assert-insert {:table-name "trait_attribute_content"
                                  :type-map   attribute-content-types
                                  :value-map  {:trait_id          (:id trait)
                                               :attribute_content attribute-content}}))
                (doseq [xref (:xrefs trait)]
                  (assert-insert {:table-name "trait_xrefs"
                                  :type-map   xref-types
                                  :value-map  {:trait_id (:id trait)
                                               :xref     xref}})))
      :update (do
                (assert-insert {:table-name "trait"
                                :type-map   trait-types
                                :value-map  trait})
                (store-trait :create trait))
      )))


(defn store-trait-mapping
  [crud-op trait-mapping]
  (let [types {:clinical_assertion_id :string
               :trait_type            :string
               :mapping_type          :string
               :mapping_value         :string
               :mapping_ref           :string
               :medgen_id             :string
               :medgen_name           :string}]
    (case crud-op
      :create (assert-insert {:table-name "trait_mapping"
                              :type-map   types
                              :value-map  trait-mapping})
      :update (assert-update {:table-name   "trait_mapping"
                              :type-map     types
                              :value-map    trait-mapping
                              ; TODO non-keyed table, use selected fields
                              :where-fields [:clinical_assertion_id
                                             :mapping_type
                                             :mapping_value
                                             :mapping_ref]})
      )))

(defn store-trait-set
  [crud-op trait-set]
  (let [trait-set-types {:id      :int
                         :type    :string
                         :content :string}
        trait-id-types {:trait_set_id :int
                        :trait_id     :string}]
    (case crud-op
      :create (do
                (assert-insert {:table-name "trait_set"
                                :type-map   trait-set-types
                                :value-map  trait-set})
                (doseq [trait-id (:trait_ids trait-set)]
                  (assert-insert {:table-name "trait_set_trait_ids"
                                  :type-map   trait-id-types
                                  :value-map  {:trait_set_id (:id trait-set)
                                               :trait_id     trait-id}})))
      :update (do
                (assert-delete {:table-name   "trait_set"
                                :type-map     trait-set-types
                                :value-map    trait-set
                                :where-fields [:id]})
                (store-trait-set :create trait-set)
                )
      )
    ))

;(defmulti store-variation-archive (fn [crud-op entity] crud-op))

(defn store-variation-archive
  [crud-op variation-archive]
  (let [types {:id                         :string
               :version                    :int
               :variation_id               :int
               :date_created               :string
               :date_last_updated          :string
               :num_submissions            :int
               :num_submitters             :int
               :record_status              :string
               :review_status              :string
               :species                    :string
               :interp_date_last_evaluated :string
               :interp_type                :string
               :interp_description         :string
               :interp_explanation         :string
               :interp_content             :string
               :content                    :string}]
    (case crud-op
      :create (assert-insert {:table-name "variation_archive"
                              :type-map   types
                              :value-map  variation-archive})
      :update (assert-update {:table-name   "variation_archive"
                              :type-map     types
                              :value-map    variation-archive
                              :where-fields [:id]})
      )
    ))

(defn store-message
  "Receive incoming kafka message"
  [msg-str]

  (let [msg (json/parse-string msg-str true)
        entity (:content msg)
        crud-op (keyword (:type msg))
        entity-type (:entity_type entity)]
    ;(log/info "store-message: " (str entity))
    (case entity-type
      "clinical_assertion" (store-clinical-assertion crud-op entity)
      "clinical_assertion_observation" (store-clinical-assertion-observation crud-op entity)
      "clinical_assertion_trait" (store-clinical-assertion-trait crud-op entity)
      "clinical_assertion_trait_set" (store-clinical-assertion-trait-set crud-op entity)
      "clinical_assertion_variation" (store-clinical-assertion-variation crud-op entity)
      "gene" (store-gene crud-op entity)
      "gene_association" (store-gene-association crud-op entity)
      "rcv_accession" (store-rcv-accession crud-op entity)
      "submission" (store-submission crud-op entity)
      "submitter" (store-submitter crud-op entity)
      "trait" (store-trait crud-op entity)
      "trait_mapping" (store-trait-mapping crud-op entity)
      "trait_set" (store-trait-set crud-op entity)
      "variation" (store-variation crud-op entity)
      "variation_archive" (store-variation-archive crud-op entity)
      (log/error "Unknown message entity_type " entity-type ", " msg-str))
    ))

(defonce message-queue (async/chan 1000))

(defn store-message-async
  "Receive incoming kafka message"
  [msg-str]
  ;(log/info "store-message: " (str entity))
  (async/>!! message-queue msg-str))
