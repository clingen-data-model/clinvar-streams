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
  (letfn [(first-greater? [a b] (< 0 (.compareTo a b)))
          (obj-max-2 [a b] (if (first-greater? a b) a b))]
    (reduce obj-max-2 (filter (comp not nil?) vals))))

(defn apply-to-if
  "Returns TO with F applied using ARGS appended with V, if V is not nil
   Examples:
   This will do (assoc {:a 1} :b v) but only if v is not nil,
   otherwise it will return the original map {:a 1}
   (apply-to-if {:a 1} assoc [:b] v)

   This can also be used for assoc-in
   (apply-to-if m assoc-in [[:c :d]] v)
   "
  [to f args v]
  (if ((comp not nil?) v)
    (apply f to (concat args [v]))
    to))

(defn select-keys-nested
  "Same as select-keys, but elements of keyvals can be a seq passable to get-in.
   Keeps the same nesting structure specified in the keyval.
   Example:
   (select-keys-nested {:a {:b {:c 3 :d 4}} :e 5} [[:a :b :c]])
   -> {:a {:b {:c 3}}}
   "
  [m keyvals]
  (reduce (fn [agg k]
            (cond (sequential? k) (if (not (nil? (get-in m k)))
                                    (assoc-in agg k (get-in m k)) agg)
                  :else (if (not (nil? (get m k)))
                          (assoc agg k (get m k)) agg)))
          {} keyvals))

(defn simplify-dollar-map
  "Return (get m :$) if m is a map and :$ is the only key. Otherwise return m.
  Useful for BigQuery JSON serialization where single values may be turned into $ maps"
  [m]
  (if (and (map? m)
           (= '(:$) (keys m)))
    (:$ m)
    m))

(defn as-vec-if-not
  "If val is not a sequential collection (maps, sets, strings are not), return it in a vector."
  [val]
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
