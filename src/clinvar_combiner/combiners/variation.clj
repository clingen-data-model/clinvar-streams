(ns clinvar-combiner.combiners.variation
  (:require [clinvar-streams.storage.database-sqlite.client :as db-client]
            [clinvar-streams.util :refer [in? obj-max assoc-if]]
            [cheshire.core :as json]
            [clojure.java.jdbc :refer :all]
            [taoensso.timbre :as log]))

(defn variation-list-to-compound
  "Takes a collection of clinical assertion variations and nests the child variations (^String :child_ids)
  under compound variations as a vector on the key :child_variations of the parents(s).

  Returns a vector of root variations (those which are not children of any others)."
  [variations]
  (let [variations (filter #(not (nil? %)) variations)]
    (if (= 0 (count variations))
      []
      (let [variations (map (fn [v] (let [child-ids (into [] (json/parse-string (:child_ids v)))]
                                      (if (< 0 (count child-ids))
                                        (assoc v :child_ids child-ids)
                                        (dissoc v :child_ids))))
                            (map #(assoc % :parent_ids []) variations))
            id-to-variation (atom (into {} (map #(vector (:id %) %) variations)))]

        ; Add reverse relations from children to parents
        (doseq [[id v] (into {} @id-to-variation)]          ; Copy value of id-to-variation, not sure if necessary
          (when-let [child-ids (:child_ids v)]
            (log/debug "Updating child variations of " (:id v) (into [] child-ids))
            (doseq [child-id child-ids]
              ; Update child variation to have current id in its parent_ids vector
              (do (log/debugf "Adding variation id %s as parent of %s" id child-id)
                  (swap! id-to-variation
                         (fn [old]
                           (assoc old child-id (update (get old child-id)
                                                       :parent_ids
                                                       (fn [p] (conj p id))))))))))
        ; Remove empty parent_ids vectors
        (reset! id-to-variation (into {} (map (fn [[k v]]
                                                [k (if (= 0 (count (:parent_ids v)))
                                                     (dissoc v :parent_ids) v)])
                                              @id-to-variation)))

        (log/debug "id-to-variation: " @id-to-variation)

        ; Root variation is the one with no parent_ids field (was removed above)
        (let [roots (into {} (filter #(nil? (:parent_ids (second %))) @id-to-variation))]
          (if (= 0 (count roots))
            (throw (ex-info "Could not find any root variations in variation list"
                            {:id-to-variation @id-to-variation}))
            (let [structured
                  (for [[root-id root-variation] roots]
                    (letfn [(remove-unnecessary-keys [variation]
                              (dissoc variation :child_ids :clinical_assertion_id :descendant_ids))
                            (add-children-fn [variation]
                              (log/debugf "variation: %s child_ids: %s" (:id variation) (:child_ids variation))
                              (let [child-ids (:child_ids variation)]
                                (let [variation (remove-unnecessary-keys variation)]
                                  (if child-ids
                                    (let [child-variations (map #(get @id-to-variation %) child-ids)
                                          child-variations (map #(dissoc % :parent_ids) child-variations)]
                                      (log/trace "child-variations:" child-variations)
                                      (if (some #(= nil %) child-variations)
                                        (throw (ex-info (format "Variation referred to child id not found")
                                                        {:child-ids child-ids :ids (keys @id-to-variation)}))
                                        (let [recursed-child-variations (map #(if (:child_ids %) (add-children-fn %) %)
                                                                             child-variations)]
                                          (assoc variation :child_variations (map remove-unnecessary-keys
                                                                                  recursed-child-variations)))))
                                    variation)
                                  )))]
                      ; Add children to root variation recursively
                      (add-children-fn root-variation)))]
              ; remove all of these from the output maps
              structured
              ;(map #(dissoc % :clinical_assertion_id :child_ids :descendant_ids) structured)
              )))))))

(defn validate-variation-tree
  "Returns true if validations succeed, throw exception otherwise.
  Checks structural expectations for a variation or compound variation. Does not look at content
  of variation aside from the type of variation, which is used to validate structural relationships."
  [variation]
  ; TODO check no variation appears twice (?)

  ; Check genotype->haplotype->simpleallele expected topological order
  (let [descendant-subclass-map {"Genotype" ["Haplotype" "SimpleAllele"]
                                 "Haplotype" ["SimpleAllele"]
                                 "SimpleAllele" nil}
        child-variations (:child_variations variation)
        subclass (:subclass_type variation)]
    (when (not (contains? descendant-subclass-map subclass))
      (throw (ex-info "Variation subclass not recognized" {:variation variation})))

    (if (not (= 0 (count child-variations)))
      (if (nil? (get descendant-subclass-map subclass))
        (throw (ex-info "Variation has children but no mapped child subclass" {:variation variation}))
        (if (not (every? (fn [child]
                           (if (not (in? (:subclass_type child) (get descendant-subclass-map subclass)))
                             (throw (ex-info "Variation child did not match expected type"
                                             {:variation variation
                                              :subclass_type subclass
                                              :child_subclass_type (:subclass_type child)
                                              :expected_child_subclass_type (get descendant-subclass-map subclass)})))
                           (validate-variation-tree child))
                         child-variations))
          (throw (ex-info "Variation has invalid child variation(s)" {:variation variation})))))
    true))
