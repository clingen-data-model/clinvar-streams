(ns clinvar-streams.storage.database-sqlite.sink2
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
                                 (recur st (rest ps))))))))]
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

(defn build-insert-op
  "Given table-name, a map of column types and a map of column values,
  return a map of parameterized sql string, and seq of parameter values as
  {:sql \"...\" :parameter-values [...]}"
  [{:keys [table-name type-map value-map]}]
  (let [col-name-keys (keys type-map)
        sql (format "insert into %s(%s) values(%s); "
                    table-name
                    (s/join "," (mapv #(name %) col-name-keys))
                    (s/join "," (mapv (fn [%] "?") col-name-keys)))]
    {:sql sql
     :parameter-types (mapv #(get type-map %) col-name-keys)
     :parameter-values (mapv #(get value-map %) col-name-keys)}))

(defn merge-ops
  "Given seq of sql ops in form:
  {:sql \"...\" :parameter-values [...]}
  Return a seq of n/batch-size merged ops containing all of the sql statements and parameter values"
  [ops]
  (let []
    (for [ops-batch (partition-all 500 ops)]
      {:sql (s/join "; " (mapv #(:sql %) ops-batch))
       :parameter-types (flatten (mapv #(:parameter-types %) ops-batch))
       :parameter-values (flatten (mapv #(:parameter-values %) ops-batch))})))

(defn parameterize-op-statement
  [conn sql values]
  (let [params (map (fn [[i value]] {:idx (+ 1 i) :value value})
                    (map-indexed vector values))
        pstmt (.prepareStatement conn sql)]
    (doseq [param params]
      ;(log/debug "Looping through params" (into [] ps))
      (log/trace param)
      (.setObject pstmt (:idx param) (:value param)))
    pstmt))

(defn sql-op->PreparedStatement
  [op]
  (with-open [conn (get-connection @db-client/db)]
    (let [sql (:sql op)
          values (:parameter-values op)
          pstmt (parameterize-op-statement conn (:sql op) (:parameter-values op))]
      (try (let [updated-count (.executeUpdate pstmt)]
             (if (not= 1 updated-count)
               (throw (ex-info (str "Failed to insert")
                               {:cause {:sql sql :values values}}))))
           (catch Exception e
             (log/error (ex-info "Exception on insert"
                                 {:cause {:sql sql :values values}
                                  :sql-state (.getSQLState e)}))
             (throw e))))))

; Check for primary key violation exception, log warning, run again with 'insert or replace'
;(defn -assert-insert
;  [{:keys [table-name type-map value-map]}]
;  (let [sql (format "insert into %s(%s) values(%s)"
;                    table-name
;                    (s/join "," (into [] (map #(name %) (keys type-map))))
;                    (s/join "," (into [] (map (fn [%] "?") (keys type-map)))))]
;    (with-open [conn (get-connection @db-client/db)]
;      (let [pstmt (parameterize-statement conn sql type-map value-map)]
;        (try (let [updated-count (.executeUpdate pstmt)]
;               (if (not= 1 updated-count)
;                 (throw (ex-info (str "Failed to insert " table-name)
;                                 {:cause {:sql sql :types type-map :values value-map}}))))
;             (catch Exception e
;               (log/error (ex-info "Exception on insert"
;                                   {:cause {:sql sql :types type-map :values value-map}
;                                    :sql-state (.getSQLState e)}))
;               (throw e)))))))

;(defn assert-insert
;  [{:keys [table-name type-map value-map]}]
;  (try (-assert-insert {:table-name table-name :type-map type-map :value-map value-map})
;       (catch Exception e
;         (log/error (json/generate-string e))
;         (throw e))))

;(defn assert-update
;  [{:keys [table-name type-map value-map where-fields]}]
;  (assert (< 0 (count where-fields)))
;  (let [; ensure constant order for fields by placing into vectors
;        where-fields (into [] where-fields)
;        non-where-fields (into [] (filter #(not (contains? where-fields %)) (keys type-map)))
;        set-clause (str " set " (s/join "," (map (fn [field] (str (name field) " = ?"))
;                                                 non-where-fields)))
;        where-clause (str " where " (s/join " and " (map (fn [pk-field] (str (name pk-field) " = ?"))
;                                                         where-fields)))
;        sql (format "update %s %s %s"
;                    table-name
;                    ;(s/join "," (map (fn [_] "?") (keys set-clause)))
;                    set-clause
;                    where-clause)
;        ; to ensure ordering, convert maps to vectors
;        ; parametrize statement just expects [k v] iterables
;        type-vec (map (fn [field] [field (get type-map field)])
;                      (concat non-where-fields where-fields))
;        value-vec (map (fn [field] [field (get value-map field)])
;                       (concat non-where-fields where-fields))
;        _ (pprint sql)
;        _ (pprint type-vec)
;        _ (pprint value-vec)
;        ]
;    (with-open [conn (get-connection @db-client/db)]
;      (let [pstmt (parameterize-statement conn sql type-vec value-vec)]
;        (try (let [updated-count (.executeUpdate pstmt)]
;               (if (< 1 updated-count)
;                 (throw (ex-info (str "Failed to update " table-name)
;                                 {:cause {:sql sql :types type-map :values value-map}}))))
;             (catch Exception e
;               (log/error (ex-info "Exception on update" {:cause {:sql sql :types type-map :values value-map}}))
;               (throw e))))
;      )))

;(defn assert-delete
;  [{:keys [table-name type-map value-map where-fields]}]
;  (let [where-clause (str " where "
;                          (s/join " and " (map #(str (name %) " = ? ") where-fields)))
;
;        sql (format "delete from % %"
;                    table-name
;                    where-clause)
;        type-vec (map (fn [field] [field (get type-map field)])
;                      where-fields)
;        value-vec (map (fn [field] [field (get value-map field)])
;                       where-fields)
;        ]
;    (with-open [conn (get-connection @db-client/db)]
;      (let [pstmt (parameterize-statement conn sql type-vec value-vec)]
;        (try (let [updated-count (.executeUpdate pstmt)]
;               (log/infof "Deleted %d records from %s" updated-count table-name))
;             (catch Exception e
;               (log/error (ex-info "Exception on delete" {:cause {:sql sql :types type-map :values value-map}}))
;               (throw e)))))))

(defn- try-parse-int
  [^String val]
  (cond
    (nil? val) val
    (int? val) val
    (string? val) (try (Integer/parseInt val)
                       (catch NumberFormatException e val))
    :default val))

(defn parse-ints [type-map value-map]
  (let [int-cols (filter #(= :int (get type-map %)) (keys type-map))
        int-vals (into {} (map (fn [k] [k (try-parse-int (get value-map k))]) int-cols))]
    (merge value-map int-vals)))

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
    (build-insert-op {:table-name "submitter"
                      :type-map types
                      :value-map values})))

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
    (build-insert-op {:table-name "submission"
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
    (build-insert-op {:table-name "trait"
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
    (build-insert-op {:table-name "trait_set"
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
                                 (:clinical_assertion_trait_ids clinical-assertion-trait-set))]
    (concat [(build-insert-op {:table-name "clinical_assertion_trait_set"
                               :type-map types
                               :value-map values})]
            (for [v trait-id-values-seq]
              (build-insert-op {:table-name "clinical_assertion_trait_set_clinical_assertion_trait_ids"
                                :type-map trait-ids-types
                                :value-map v})))))

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
    (build-insert-op {:table-name "clinical_assertion_trait"
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
    (build-insert-op {:table-name "gene"
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
        descendant-types {:release_date :string
                          :variation_id :string
                          :descendant_id :string}
        values (merge (select-keys variation (keys types))
                      {:dirty 1
                       :protein_changes (json/generate-string (:protein_changes variation))
                       :child_ids (json/generate-string (:child_ids variation))
                       :descendant_ids (json/generate-string (:descendant_ids variation))})
        descendant-values (map #(identity {:release_date (:release_date variation)
                                           :variation_id (:id variation)
                                           :descendant_id %})
                               (:descendant_ids variation))]
    (concat [(build-insert-op {:table-name "variation"
                               :type-map types
                               :value-map values})]
            (for [v descendant-values]
              (build-insert-op {:table-name "variation_descendant_ids"
                                :type-map descendant-types
                                :value-map v})))))

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
    (build-insert-op {:table-name "gene_association"
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
    (build-insert-op {:table-name "variation_archive"
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
    (build-insert-op {:table-name "rcv_accession"
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
    (concat [(build-insert-op {:table-name "clinical_assertion"
                               :type-map types
                               :value-map values})]
            (for [v obs-values-seq]
              (build-insert-op {:table-name "clinical_assertion_observation_ids"
                                :type-map obs-types
                                :value-map v})))))

(defn store-clinical-assertion-observation
  [observation]
  (let [types {:release_date :string
               :dirty :int
               :event_type :string

               :id :string
               :clinical_assertion_trait_set_id :string
               :content :string}
        values (merge observation {:dirty 1})]
    (build-insert-op {:table-name "clinical_assertion_observation"
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
    (build-insert-op {:table-name "trait_mapping"
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
    (concat [(build-insert-op {:table-name "clinical_assertion_variation"
                               :type-map types
                               :value-map values})]
            (for [v desc-values-seq]
              (build-insert-op {:table-name "clinical_assertion_variation_descendant_ids"
                                :type-map desc-types
                                :value-map v})))))

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
    (build-insert-op {:table-name "release_sentinels"
                      :type-map types
                      :value-map values})))

(defn flatten-kafka-message
  "Moves everything under :content up to top level of map"
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

(defn make-message-insert-op
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
      (log/error "Unknown message entity_type " entity-type ", " (str msg)))))
