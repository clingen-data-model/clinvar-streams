(ns clinvar-raw.gcpbucket
  "GCP bucket operations"
  (:require [cheshire.core      :as json]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [org.httpkit.client :as http])
  (:import [com.google.auth.oauth2 GoogleCredentials]))

(def ^:private staging-bucket
  "Name of the bucket where Monster ingest stages the ClinVar files."
  "broad-dsp-monster-clingen-prod-staging-storage")

(def ^:private api-url
  "The Google Cloud API URL."
  "https://www.googleapis.com/")

(def ^:private storage-url
  "The Google Cloud URL for storage operations."
  (str api-url "storage/v1/"))

(def ^:private bucket-url
  "The Google Cloud Storage URL for bucket operations."
  (str storage-url "b/"))

(def ^:private google-adc-token
  "Nil or a token for the Application Default Credentials."
  (delay
    (some-> (GoogleCredentials/getApplicationDefault)
            (.createScoped
             ["https://www.googleapis.com/auth/devstorage.read_only"])
            .refreshAccessToken .getTokenValue)))

(defn auth-header
  "Return an Authorization header map with bearer TOKEN."
  [token]
  {"Authorization" (str/join \space ["Bearer" token])})

(defn ^:private list-prefixes
  "Return all matching prefixes in bucket as a lazy sequence.
  Prefixes are not items in cloud storage. Prefixes are the path
  part of the object. The item contains a name which is the file name."
  ([bucket prefix]
   (let [params  {:delimiter "/" :maxResults 999 :prefix prefix}
         request {:as           :stream
                  :content-type :application/json
                  :headers      (auth-header @google-adc-token)
                  :method       :get
                  :query-params params
                  :url          (str bucket-url bucket "/o")}]
     (letfn [(get-cloud-storage-prefixes [pageToken]
               (let [{:keys [nextPageToken prefixes error]}
                     (-> request
                         (assoc-in [:query-params :pageToken] pageToken)
                         http/request deref :body io/reader (json/parse-stream true))]
                 (if (some? error)
                   (throw (ex-info (str "Exception expanding prefixes in storage bucket '" bucket
                                        "' from '" prefix "'.") {:error error}))
                   (lazy-cat prefixes
                             (when nextPageToken
                               (get-cloud-storage-prefixes nextPageToken))))))]
       (get-cloud-storage-prefixes ""))))
  ([bucket]
   (list-prefixes bucket "")))

(defn ^:private list-items
  "Return all item names in bucket with prefix as a lazy sequence.
  Prefixes are the path part of the object. The item contains a name which
  is the file name."
  [bucket prefix]
  (let [params  {:delimiter "/" :maxResults 999 :prefix prefix}
        request {:as           :stream
                 :content-type :application/json
                 :headers      (auth-header @google-adc-token)
                 :method       :get
                 :query-params params
                 :url          (str bucket-url bucket "/o")}]
    (letfn [(get-cloud-storage-items [pageToken]
              (let [{:keys [nextPageToken items error]}
                    (-> request
                        (assoc-in [:query-params :pageToken] pageToken)
                        http/request deref :body io/reader (json/parse-stream true))]
                (if (some? error)
                  (throw (ex-info (str "Exception expanding items in storage bucket '" bucket
                                       "' from '" prefix "'.") {:error error}))
                  (lazy-cat (map :name items)
                            (when nextPageToken
                              (get-cloud-storage-items nextPageToken))))))]
      (get-cloud-storage-items ""))))

(defn ^:private expand-all-prefixes
  "Recursively expand all prefixes from a starting prefix in a bucket until
  you get to the most expanded prefix and no more expansions are found."
  [bucket prefix]
  (loop [prefixes (list-prefixes bucket prefix)]
    (let [new-prefixes (flatten (map #(list-prefixes bucket %) prefixes))]
      (if (not (empty? new-prefixes))
        (recur new-prefixes)
        prefixes))))

(defn list-all-files
  "List all of the files found under the list of prefixes under prefix in bucket."
  [bucket prefix]
  (->> (expand-all-prefixes bucket prefix)
       (map #(list-items bucket %))
       flatten))
