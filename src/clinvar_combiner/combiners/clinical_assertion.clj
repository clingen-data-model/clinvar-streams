(ns clinvar-combiner.combiners.clinical-assertion
  (:require [clinvar-combiner.combiners.variation
             :refer [variation-list-to-compound
                     validate-variation-tree]]
            [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-streams.util
             :refer [obj-max assoc-if as-vec-if-not simplify-dollar-map set-union-all]]
            [cheshire.core :as json]
            [clojure.java.jdbc :refer :all]
            [taoensso.timbre :as log]
            [clojure.string :as s]))

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
          (assoc :clinical_assertion_variation
                 ; expect one only, and validate
                 (let [compound (variation-list-to-compound
                                  (map #(dissoc % :release_date :dirty :event_type)
                                       (:clinical_assertion_variations assertion)))]
                   (cond (< 1 (count compound))
                         (throw (ex-info "Assertion had more than 1 clinical assertion variation object"
                                         {:assertion assertion :variations compound}))
                         (= 1 (count compound))
                         (if (validate-variation-tree (first compound))
                           (first compound)))))
          (dissoc :clinical_assertion_variations)

          ; Set entity_type used by downstream processors
          (assoc :entity_type "clinical_assertion")
          ; If assertion event is delete, set deleted
          ((fn [a] (if (= "delete" (:event_type a)) (assoc a :deleted true) a)))
          ; Remove internal fields from assertion
          (dissoc :event_type :dirty)))))
