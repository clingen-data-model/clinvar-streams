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

(def ^:private weekly-url
  "The weekly_release URL."
  (str/join
   "/"
   [ftp-site "pub" "clinvar" "xml" "clinvar_variation" "weekly_release" ""]))

(def ^:private staging-bucket
  "Name of the bucket where Monster ingest stages the ClinVar files."
  "broad-dsp-monster-clingen-prod-staging-storage")

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
  (-> "./injest/clinvar_releases_pre_20221027.tsv"
      tabulate mapulate))

(defn ^:private fetch-ftp
  "Return the FTP site at the HTTP URL as a hicory tree."
  [url]
  (-> {:as     :text
       :method :get
       :url    url}
      http/request deref :body html/parse html/as-hickory))

(def ^:private ftp-time
  "This is how the FTP site timestamps."
  (SimpleDateFormat. "yyyy-MM-dd kk:mm:ss"))

(def ^:private ftp-time-ymdhm
  "And sometimes THIS is how the FTP site timestamps."
  (SimpleDateFormat. "yyyy-MM-dd kk:mm"))

(defn ^:private instify
  "Parse string S as a date and return its Instant or NIL."
  [s]
  (try (.parse ftp-time s) (catch Throwable _)))

(defn ^:private instify-ymdhm
  "Parse string S as a date differently and return its Instant or NIL."
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
(defmulti parse-ftp
  "Parse this FTP site's hickory CONTENT and MAPULATE it."
  (comp :type first :content))

;; Handle 4-column FTP fetches with directories and files.
;;
(defmethod parse-ftp :element parse-4
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
(defmethod parse-ftp :document-type parse-3
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
  "Return an Authorization header map with bearer TOKEN."
  [token]
  {"Authorization" (str/join \space ["Bearer" token])})

(def ^:private api-url
  "The Google Cloud API URL."
  "https://www.googleapis.com/")

(def ^:private storage-url
  "The Google Cloud URL for storage operations."
  (str api-url "storage/v1/"))

(def ^:private bucket-url
  "The Google Cloud Storage URL for bucket operations."
  (str storage-url "b/"))

(defn ^:private list-prefixes
  "Return all names in BUCKET with PREFIX in a lazy sequence."
  ([bucket prefix]
   (let [params  {:delimiter "/" :maxResults 999 :prefix prefix}
         token   (shell "gcloud" "auth" "print-access-token")
         request {:as           :stream
                  :content-type :application/json
                  :headers      (auth-header token)
                  :method       :get
                  :query-params params
                  :url          (str bucket-url bucket "/o")}]
     (letfn [(each [pageToken]
               (let [{:keys [nextPageToken prefixes]}
                     (-> request
                         (assoc-in [:query-params :pageToken] pageToken)
                         http/request deref :body io/reader parse-json)]
                 (lazy-cat prefixes
                           (when nextPageToken (each nextPageToken)))))]
       (each ""))))
  ([bucket]
   (list-prefixes bucket "")))

(defn latest-staged
  "Return the latest timestamp from staging BUCKET."
  [bucket]
  (let [regex #"^(\d\d\d\d)(\d\d)(\d\d)T(\d\d)(\d\d)(\d\d)/$"]
    (letfn [(instify [prefix]
              (let [[ok YYYY MM DD hh mm ss] (re-matches regex prefix)]
                (when ok
                  (instant/read-instant-timestamp
                   (str YYYY \- MM \- DD \T hh \: mm \: ss)))))]
      (->> bucket list-prefixes (map instify) sort last))))

(defn ftp-since
  "Return files from WEEKLY-URL more recent than INSTANT."
  [instant]
  (letfn [(since? [file]
            (apply < (map inst-ms [instant (file "Last Modified")])))]
    (->> weekly-url fetch-ftp parse-ftp rest (filter since?))))

(def ^:private slack-manifest
  "The Slack Application manifiest for ClinVar FTP Watcher."
  (let [long (str/join \space ["Notify the genegraph-dev team"
                               "when new files show up in the"
                               weekly-url
                               "FTP site"
                               "before the Monster Ingest team notices."])]
    {:_metadata
     {:major_version 1
      :minor_version 1}
     :display_information
     {:background_color "#006db6"       ; Broad blue
      :description      "Tell us when new files show up in the FTP site."
      :long_description long
      :name             "ClinVar FTP Watcher"},
     #_#_
     :settings
     {:is_hosted              false
      :org_deploy_enabled     false
      :socket_mode_enabled    false
      :token_rotation_enabled false}}))

;; More information on the meaning of error responses:
;; https://api.slack.com/methods/chat.postMessage#errors
;;
(defn ^:private post-slack-message
  "Post MESSAGE to CHANNEL with link unfurling disabled."
  [channel message]
  (let [token   (System/getenv "INJEST_SLACK_TOKEN")
        body    (json/write-str {:channel      channel
                                 :text         message
                                 :unfurl_links false
                                 :unfurl_media false})]
    (-> {:as           :stream
         :body         body
         :content-type :application/json
         :headers      (auth-header token)
         :method       :post
         :url          "https://slack.com/api/chat.postMessage"}
        http/request deref :body io/reader parse-json)))

;; Slack API has its own way of reporting statuses:
;; https://api.slack.com/web#slack-web-api__evaluating-responses
;;
(defn ^:private post-slack-message-or-throw
  "Post `message` to `channel` and throw if response indicates a failure."
  [channel message]
  (let [response (post-slack-message channel message)]
    (when-not (:ok response)
      (throw (ex-info "Slack API chat.postMessage failed"
                      {:channel  channel
                       :message  message
                       :response response})))
    response))

(defn make-message
  "Return a Slack message to report FILES newer than TIMESTAMP."
  [timestamp files]
  {:staged timestamp
   :newer  files})

(defn -main
  [& args]
  (let [[verb & more] args]
    (case verb
      "test"  (let [staged (latest-staged staging-bucket)
                    staged #inst "2022-12-26T01:00:00.000000000-00:00"
                    result {:staged staged :newer (ftp-since staged)}]
                (pprint result))
      "slack" (let [staged (latest-staged staging-bucket)
                    staged #inst "2022-12-26T01:00:00.000000000-00:00"
                    result {:staged staged :newer (ftp-since staged)}]
                (pprint result))
      (pprint "help"))))

(comment
  "https://sparkofreason.github.io/jvm-clojure-google-cloud-function/"
  "https://github.com/google-github-actions/get-secretmanager-secrets"
  "https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#onschedule"
  "https://pathom3.wsscode.com/docs/tutorials/serverless-pathom-gcf/"
  "gs://broad-dsp-monster-clingen-prod-staging-storage/20221214T010000/"
  {"Name" "ClinVarVariationRelease_2022-1211.xml.gz",
   "Size" 2235469492,
   "Released" #inst "2022-12-12T09:43:54.000-00:00",
   "Last Modified" #inst "2022-12-12T09:43:54.000-00:00"}
  "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/weekly_release/"
  (-main "test")
  tbl)
