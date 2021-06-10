(ns clinvar-streams.util
  (:require [clojure.string :as s]
            [clojure.core.async :as async]
            [clojure.set :as set])
  (:gen-class))

(defn get-env-required
  "Performs System/getenv variable-name, but throws an exception if value is empty"
  [variable-name]
  (let [a (System/getenv variable-name)]
    (if (not (empty? a))
      a
      (throw (ex-info (format "Environment variable %s must be provided" variable-name) {})))))

(defn assoc-if
  "Assoc k v pair to m if v is not nil"
  [m k v] (if (not (nil? v)) (assoc m k v) m))

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
  "max function but using Object.compareTo. vals must be homogeneous types.
  nils are ignored."
  [& vals]
  (if (or (nil? vals) (= 0 (count vals))) nil)
  (let [vals (filter #(not= nil %) vals)]
    (loop [max-val (first vals)
           to-check (rest vals)]
      (if (empty? to-check)
        max-val
        (recur
          (if (< 0 (.compareTo (first to-check) max-val))
            (first to-check)
            max-val)
          (rest to-check))))))

(defn simplify-dollar-map [m]
  "Return (get m :$) if m is a map and :$ is the only key. Otherwise return m.
  Useful for BigQuery JSON serialization where single values may be turned into $ maps"
  (if (and (map? m)
           (= '(:$) (keys m)))
    (:$ m)
    m))

(defn as-vec-if-not [val]
  "If val is not a sequential collection (maps, sets, strings are not), return it in a vector."
  ; strings are already not sequential, but it feels safer to be explicit
  (if (and (not (string? val))
           (sequential? val))
    val [val]))

(defn set-union-all
  "Return the set union of all of the provided cols."
  [& cols]
  (loop [todo cols
         output #{}]
    (if (empty? todo)
      output
      (recur (rest todo)
             (set/union output (into #{} (first todo)))))))
