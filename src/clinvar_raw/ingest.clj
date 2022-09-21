(ns clinvar-raw.ingest
  "Compare weird JSON-encoded XML messages."
  (:require [clinvar-streams.storage.rocksdb :as rocksdb]
            [clinvar-streams.util :refer [select-keys-nested]]
            [clojure.data.json :as json]
            [clojure.zip       :as zip]
            [digest]
            [taoensso.timbre :as log]))

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

(defn ^:private parse-json
  "Try to parse `object` as JSON or give up and return `object`."
  [object]
  (try (json/read-str object)
       (catch Throwable _ object)))

(defn decode
  "Try to decode OBJECT as a JSON object with a further stringified
  `content` field into EDN or give up and just return OBJECT."
  [object]
  (try (-> object json/read-str (update "content" parse-json))
       (catch Throwable _ object)))

(defn disorder
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

;; (defstate dedup-db
;;   :start (rocksdb/open "clinvar-raw-dedup.db")
;;   :stop (rocksdb/close dedup-db))

(defn clinvar-concept-identity [message]
  (let [entity-type (-> message :content :entity_type)]
    (case entity-type
      :trait_mapping (select-keys-nested message [[:content :entity_type]
                                                  [:content :clinical_assertion_id]
                                                  [:content :medgen_id]])
      :gene_association (select-keys-nested message [[:content :entity_type]
                                                     [:content :gene_id]
                                                     [:content :variation_id]])
      (select-keys-nested message [[:content :entity_type]
                                   [:content :id]]))))

;; (defn clinvar-message-without-meta
;;   "Drops the fields in the wrapper around :content.
;;    Ex: :release_date, :event_type "
;;   [message]
;;   {:content (:content message)})

(defn store-new!
  "Takes edn M, stores its hash in the dedup database.
   Stored as ^Integer (hash m) -> ^String (-> m digest/md5)
   rocks-put calls nippy/freeze on the value. Could add a put-raw-value."
  [db m]
  (let [k (clinvar-concept-identity m)
        v (-> m (dissoc :release_date) disorder)]
    (log/debug :fn :store-new! :k k)
    (rocksdb/rocks-put! db k v)))

(defn duplicate?
  "Takes a map M, returns truthy (not nil/false) if has been seen before.
   Does a full comparison of m, except for :release_date.

   If the content of m has been seen before but the only difference is that the
   previous was a create and the current is an update, returns :create-to-update.
   Caller may want to use this to persist the update.
   TODO change keys to sorted vecs and hash"
  [db current-m #_{:keys [persist-create-update-dup?]
                   :or {persist-create-update-dup? true}}]
  (let [k (clinvar-concept-identity current-m)
        current-v (-> current-m (dissoc :release_date) disorder)
        previous-v (rocksdb/rocks-get db k)]
    (log/debug :fn :duplicate? :k k :current current-v :previous previous-v)
    ;; Compare the messages (without :release_date)
    ;; If they are different but the only difference is that
    ;; the previous was a :create and the current is an :update,
    ;; treat this as a duplicate.
    (or (= previous-v current-v)
        (if (and (= :create (:event_type previous-v))
                 (= :update (:event_type current-v))
                 (= (:content previous-v) (:content current-v)))
          :create-to-update ;; truthy
          false))))
