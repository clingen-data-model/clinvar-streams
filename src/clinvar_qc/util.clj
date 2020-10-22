(ns clinvar-qc.util
  (:gen-class))

(defn in?
  "Returns true if e in coll, false otherwise"
  [coll e]
  (not= nil (some #(= e %) coll)))