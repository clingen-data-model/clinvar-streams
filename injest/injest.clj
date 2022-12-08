(ns injest
  "Mistakenly ingest stuff."
  (:require [clojure.java.io    :as io]
            [clojure.pprint     :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.string     :as str]
            [hickory.core       :as html]
            [hickory.select     :as css]
            [org.httpkit.client :as http])
  (:import [java.text SimpleDateFormat]))

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

(def ^:private ftp-site
  "FTP site of the National Library of Medicine."
  "https://ftp.ncbi.nlm.nih.gov")

(def ^:private staging
  "The bucket where Monster ingest stages the ClinVar files."
  "gs://broad-dsp-monster-clingen-prod")

(defn ^:private tabulate
  "Return a vector of vectors from FILE as a Tab-Separated-Values table."
  [file]
  (letfn [(split [line] (str/split line #"\t"))]
    (-> file io/reader line-seq
        (->> (map split)))))

(defn ^:private mapulate
  "Return a sequence of labeled maps from the TSV TABLE."
  [table]
  {:pre [(s/valid? ::table table)]}
  (let [[header & rows] table]
    (map (partial zipmap header) rows)))

(defn ^:private clinvar_releases_pre_20221027
  "Return the TSV file as a sequence of maps."
  []
  (-> "./clinvar_releases_pre_20221027.tsv"
      tabulate mapulate))

(defn ^:private fetch
  "Return STUFF from FTP-SITE as a hicory tree."
  [& stuff]
  (-> {:as     :text
       :method :get
       :url    (str/join "/" (into [ftp-site] stuff))}
      http/request deref :body html/parse html/as-hickory))

(def ^:private ftp-time
  "This is how the FTP site timestamps."
  (SimpleDateFormat. "yyyy-MM-dd kk:mm:ss"))

(defn ^:private instify
  "Parse string S as a date and return its Instant or NIL."
  [s]
  (try (.parse ftp-time s) (catch Throwable _)))

(defn ^:private longify
  "Return S or S parsed into a Long after stripping commas."
  [s]
  (try (-> s (str/replace "," "") parse-long)
       (catch Throwable _ s)))

(defn ^:private fix-ftp-map
  "Fix the FTP map entry M by parsing its string values."
  [m]
  (-> m
      (update "Size"          longify)
      (update "Released"      instify)
      (update "Last Modified" instify)
      (->> (remove (comp nil? second))
           (into {}))))

(defn parse
  "Parse this FTP site's hickory CONTENT and MAPULATE it."
  [content]
  (letfn [(span?   [elem] (-> elem :attrs :colspan))
          (unelem  [elem] (if (map? elem) (-> elem :content first) elem))]
    (let [selected (css/select
                    (css/or
                     (css/child (css/tag :thead) (css/tag :tr) (css/tag :th))
                     (css/child (css/tag :tr) (css/tag :td)))
                    content)
          span (->> selected (keep span?) first parse-long)]
      (->> selected
           (remove span?)
           (map (comp unelem first :content))
           (partition-all span)
           mapulate
           (map fix-ftp-map)))))

(comment

  (->> ["pub" "clinvar" "xml" "clinvar_variation" "weekly_release"]
       (apply fetch)
       parse)

  (->> ["pub" "clinvar" "xml" "clinvar_variation"]
       (apply fetch)
       parse)

  (->> ["pub" "clinvar" "xml"]
       (apply fetch)
       parse)

  (->> ["pub" "clinvar"]
       (apply fetch)
       )

  (->> ["pub" "clinvar" "xml" "weekly_release"]
       (apply fetch)
       parse)
  )
