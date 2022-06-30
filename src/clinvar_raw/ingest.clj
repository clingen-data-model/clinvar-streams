(ns clinvar-raw.ingest
  "Compare weird JSON-encoded XML messages."
  (:require [clojure.data.json  :as json]
            [clojure.zip        :as zip]))

;; Messages are encoded as JSON which lacks sets.  Data that are
;; semantically unordered are encoded as JSON arrays, which forces
;; an ordering that creates spurious differences between messages.
;;
;; Now that mangled data is confined to the `content` field in the
;; message, which is further encoded as a string which must be parsed
;; into JSON before being decoded into EDN.

;; Consequently, pass JSON message file contents through `decode` to
;; lift them into EDN and handle the stringified `content` field, then
;; pass the resulting decoded JSON as EDN to `differ?` to detect
;; differences between messages.

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

(defn differ?
  "Nil when NOW equals WAS after DISORDERing their vectors into sets.
  Otherwise a hash of the DISORDERed NOW."
  [now was]
  (let [now-edn (disorder now)
        was-edn (disorder was)]
    (when-not (= now-edn was-edn)
      (hash now-edn))))
