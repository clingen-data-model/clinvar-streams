(ns clinvar-streams.util
  (:require [clojure.string :as s]
            [clojure.core.async :as async])
  (:gen-class))

(defn get-env-required
  "Performs System/getenv variable-name, but throws an exception if value is empty"
  [variable-name]
  (let [a (System/getenv variable-name)]
    (if (not (empty? a)) a
                         (ex-info (format "Environment variable %s must be provided" variable-name) {}))))

(defn not-empty?
  [coll]
  (not (empty? coll)))

(defn in?
  "Returns true if e in col"
  [e col]
  (some #(= e %) col))

(defn not-in? [e col]
  (not (in? e col)))

(defn match-in?
  "Returns true if any e in col contains pattern"
  [pattern col]
  (some? (some #(re-find (re-pattern pattern) %) col)))

(defn match-not-in?
  "Returns true if no e in col contain the pattern"
  [pattern col]
  (not (match-in? pattern col)))

(defn match-every?
  "Returns true if all e in col contains pattern"
  [pattern col]
  (every? #(re-find (re-pattern pattern) %) col))

(defn unordered-eq?
  "Returns true if the two collections are equal, regardless of order"
  [col1 col2]
  (= (sort col1) (sort col2)))

(defn chan-get-available!
  "Returns the values currently available in the chan. Does not block."
  [from-channel]
  (loop [ch from-channel
         out []]
    (let [m (async/poll! from-channel)]
      (if (not (nil? m))
        (recur ch (conj out m))
        out))))

(defn obj-max
  "max function but using Object.compareTo. vals must be homogeneous types."
  [& vals]
  (if (or (nil? vals) (= 0 (count vals))) nil)
  (loop [max-val (first vals)
         to-check (rest vals)]
    (if (empty? to-check)
      max-val
      (recur
        (if (< 0 (.compareTo (first to-check) max-val))
          (first to-check)
          max-val)
        (rest to-check)))))
