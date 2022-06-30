(ns clinvar-raw.ingest
  "Compare weird JSON-encoded XML messages."
  (:require [clinvar-raw.debug]
            [clojure.data.json  :as json]
            [clojure.spec.alpha :as s]
            [clojure.zip        :as zip]))

;; Messages are encoded as JSON which lacks sets.  Data that are
;; semantically unordered are encoded as JSON arrays which are
;; necessarily ordered.  Now that mangled data is confined to the
;; `content` field in the message which is further encoded as a string
;; which must be parsed into JSON before being decoded into EDN.

(defn decode
  "Decode JSON string with stringified `content` field into EDN."
  [json]
  (-> json json/read-str (update "content" json/read-str)))

(defn ^:private disorder
  "Return EDN with any vector fields converted to sets."
  [edn]
  (letfn [(branch? [node] (or   (map? node) (vector? node)))
          (entry?  [node] (isa? (type node) clojure.lang.MapEntry))
          (make    [node children]
            (into (if (entry? node) [] (empty node)) children))]
    (loop [loc (zip/zipper branch? seq make edn)]
      (if (zip/end? loc) (zip/root loc)
          (let [node (zip/node loc)]
            (recur (zip/next
                    (if (entry? node)
                      (let [[k v] node]
                        (if (vector? v) (zip/replace loc [k (set v)]) loc))
                      loc))))))))

(s/fdef differ?
  :args (s/cat :now   (constantly true)
               :was   (constantly true))
  :ret  (s/or  :false nil?
               :true  (complement nil?))
  :fn   #(clinvar-raw.debug/trace %)
  #_#(or (nil? (:ret %))
         (==   (:ret %) (-> % :args :now hash))))

(defn differ?
  "Nil when NOW equals WAS after DISORDERing their vectors into sets.
  Otherwise a hash of the DISORDERed NOW."
  [now was]
  (let [now-edn (disorder now)
        was-edn (disorder was)]
    (when-not (= now-edn was-edn)
      (hash now-edn))))
