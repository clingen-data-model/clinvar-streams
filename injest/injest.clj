(ns injest
  "Mistakenly ingest stuff."
  (:require [clojure.data.json  :as json]
            [clojure.instant    :as instant]
            [clojure.java.io    :as io]
            [clojure.java.shell :as shell]
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
  "gs://broad-dsp-monster-clingen-prod-staging-storage")

"gs://broad-dsp-monster-clingen-prod-staging-storage/20221213T010000/"

(defn parse-json
  "Parse STREAM as JSON or print it."
  [stream]
  (try (json/read stream :key-fn keyword)
       (catch Throwable x
         (pprint {:exception x :stream (slurp stream)})
         stream)))

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

(def ^:private ftp-time-ymdhm
  "And sometimes THIS is how the FTP site timestamps."
  (SimpleDateFormat. "yyyy-MM-dd kk:mm"))

(defn ^:private instify-ymdhm
  "Parse string S as a date and return its Instant or NIL."
  [s]
  (try (.parse ftp-time-ymdhm s) (catch Throwable _)))

(defn ^:private longify
  "Return S or S parsed into a Long after stripping commas."
  [s]
  (or (try (-> s (str/replace "," "") parse-long)
           (catch Throwable _))
      s))

(defn ^:private fix-ftp-map
  "Fix the FTP map entry M by parsing its string values."
  [m]
  (-> m
      (update "Size"          longify)
      (update "Released"      instify)
      (update "Last Modified" instify)
      (update "Last modified" instify-ymdhm) ; Programmers suck.
      (->> (remove (comp nil? second))
           (into {}))))

;; This dispatch function is an HACK.
;;
(defmulti parse
  "Parse this FTP site's hickory CONTENT and MAPULATE it."
  (comp :type first :content))

;; Handle 4-column FTP fetches with directories and files.
;;
(defmethod parse :element parse-4
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

;; Handle 3-column FTP fetches with only directories.
;; The middle group is the 'Last modified' FTP-TIME-YMDHM timestamp.
;;
(defmethod parse :document-type parse-3
  [content]
  (let [regex #"^\s*(.*)\s*\t\s*(\d\d\d\d\-\d\d\-\d\d \d\d:\d\d)\s+(\S+)\s*$"
        [top & rows] (->> content
                          (css/select (css/child (css/tag :pre)))
                          first :content)
        header (map str/trim (str/split top #"     *"))]
    (letfn [(unelem [elem] (if (map? elem) (-> elem :content first) elem))
            (break  [line] (->> line (re-matches regex) rest))]
      (->> rows
           (keep unelem)
           (partition-all 2)
           (map (partial str/join \tab))
           rest
           (map break)
           (cons header)
           mapulate
           (map fix-ftp-map)))))

(defn ^:private shell
  "Run ARGS in a shell and return stdout or throw."
  [& args]
  (let [{:keys [exit err out]} (apply shell/sh args)]
    (when-not (zero? exit)
      (throw (ex-info (format "injest: %s exit status from: %s : %s"
                              exit args err))))
    (str/trim out)))

;; Wrap an authorization header around Bearer TOKEN.
;;
(defn ^:private auth-header
  []
  {"Authorization"
   (str/join \space ["Bearer" (shell "gcloud" "auth" "print-access-token")])})

(def ^:private api-url
  "The Google Cloud API URL."
  "https://www.googleapis.com/")

(def ^:private storage-url
  "The Google Cloud URL for storage operations."
  (str api-url "storage/v1/"))

(def ^:private bucket-url
  "The Google Cloud Storage URL for bucket operations."
  (str storage-url "b/"))

(def ^:private _-? (set "_-"))
(def ^:private digit? (set "0123456789"))
(def ^:private lowercase? (set "abcdefghijklmnopqrstuvwxyz"))
(def ^:private bucket-allowed? (into _-? (concat digit? lowercase?)))

(s/def ::bucket-name
  (s/and string?
         (partial every? bucket-allowed?)
         (complement (comp _-? first))
         (complement (comp _-? last))
         (comp (partial > 64) count)
         (comp (partial <  2) count)))

(defn ^:private parse-gs-url
  "Return BUCKET and OBJECT from a gs://bucket/object URL."
  [url]
  (let [[gs-colon nada bucket object] (str/split url #"/" 4)]
    (when-not
        (and (every? seq [gs-colon bucket])
             (= "gs:" gs-colon)
             (= "" nada))
      (throw (ex-info "Bad GCS URL" {:url url})))
    [bucket (or object "")]))

(defn ^:private list-objects
  "The objects in BUCKET with PREFIX in a lazy sequence."
  ([bucket prefix]
   (letfn [(each [pageToken]
             (let [{:keys [items nextPageToken]}
                   (-> {:as           :stream
                        :content-type :application/json
                        :headers      (auth-header)
                        :method       :get
                        :url          (str bucket-url bucket "/o")
                        :query-params {:prefix prefix
                                       :maxResults 999
                                       :pageToken pageToken}}
                       http/request deref :body io/reader parse-json)]
               (lazy-cat items (when nextPageToken (each nextPageToken)))))]
     (each "")))
  ([url]
   (apply list-objects (parse-gs-url url))))

(defn staged
  "Return the staged ClinVarRelease.xml.gz objects from GCS."
  []
  (let [suffix "/xml/ClinVarRelease.xml.gz"]
    (letfn [(release?  [o] (str/ends-with? (:name o) suffix))
            (summarize [o] (select-keys o [:name :updated]))
            (instify   [o] (update o :updated instant/read-instant-timestamp))]
      (->> staging list-objects
           (filter release?)
           (map (comp instify summarize))))))

(comment
  (def wtf (list-objects staging "/"))
  (let [[bucket prefix] (parse-gs-url staging)]
    (list-objects bucket "/"))
  (list-objects (str staging "/"))
  (parse-gs-url "gs://bucket/")
  (def xmls (staged))
  (count xmls)
  (->> staging parse-gs-url)
  (->> ["pub" "clinvar" "xml" "clinvar_variation" "weekly_release"]
       (apply fetch)
       parse)
  (->> ["pub" "clinvar"]
       (apply fetch)
       parse)
  (def xmls
    (->> staging list-objects
         (map #(select-keys % [:name :updated]))
         (filter #(str/ends-with? (:name %) "/xml/ClinVarRelease.xml.gz"))))
  (instant/read-instant-date "2018-12-14 09:17")
  (instant/read-instant-date "2022-12-12 04:43:54")
  (count xmls)
  ;; => 426
  (instant/read-instant-date "2021-11-03T00:01:53.859Z")
  ;; => #inst "2021-11-03T00:01:53.859-00:00"
  (instant/read-instant-timestamp "2021-11-03T00:01:53.859Z")
  ;; => #inst "2021-11-03T00:01:53.859000000-00:00"
  tbl)
