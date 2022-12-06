(ns injest
  "Mistakenly ingest stuff."
  (:require [clojure.data.json  :as json]
            [clojure.java.io    :as io]
            [clojure.pprint     :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.string     :as str]
            [hickory.core       :as html]
            [hickory.select     :as css]
            [org.httpkit.client :as http]))

(defmacro dump
  "Dump [EXPRESSION VALUE] where VALUE is EXPRESSION's value."
  [expression]
  `(let [x# ~expression]
     (do
       (pprint ['~expression x#])
       x#)))

(s/def ::table
  (s/and sequential?
         (partial every? sequential?)
         #(== 1 (count (set (map count %))))
         #(== (count (set (first %))) (count (first %)))))

(def ftp-site
  "FTP site of the National Library of Medicine."
  "https://ftp.ncbi.nlm.nih.gov")

(def staging
  "The bucket where Monster ingest stages the ClinVar files."
  "gs://broad-dsp-monster-clingen-prod")

(defn tabulate
  "Return a vector of vectors from FILE as a Tab-Separated-Values table."
  [file]
  (letfn [(split [line] (str/split line #"\t"))]
    (-> file io/reader line-seq
        (->> (map split)))))

(defn mapulate
  "Return a sequence of labeled maps from the TSV TABLE."
  [table]
  {:pre [(s/valid? ::table table)]}
  (let [[header & rows] table]
    (map (partial zipmap header) rows)))

(defn clinvar_releases_pre_20221027
  "Return the TSV file as a sequence of maps."
  []
  (-> "./clinvar_releases_pre_20221027.tsv"
      tabulate mapulate))

(defn parse-json
  "Parse STREAM as JSON or print it."
  [stream]
  (try (json/read stream :key-fn keyword)
       (catch Throwable x
         (pprint {:exception x :stream (slurp stream)})
         stream)))

(defn fetch
  "Fetch STUFF from FTP-SITE."
  [& stuff]
  (-> {:as      :text
       :headers {"Accept:"      "application/json"
                 "Content-type" "application/json"}
       :method  :get
       :url     (str/join "/" (into [ftp-site] stuff))}
      http/request deref :body html/parse html/as-hickory))

(comment
  (fetch "pub" "clinvar" "xml" "clinvar_variation" "weekly_release")
  "tbl")
