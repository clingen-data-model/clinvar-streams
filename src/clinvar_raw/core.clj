(ns clinvar-raw.core
  (:require [jackdaw.client.log :as jl]
            [clojure.pprint :as pprint]
            [cheshire.core :as json]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as log]
            [clinvar-raw.config :as cfg]
            [clinvar-raw.stream :as stream]
            [clinvar-raw.service]
            [clinvar-raw.ingest]))

(defn start-states! []
  (mount.core/start #'clinvar-raw.stream/dedup-db
                    #'clinvar-raw.service/service))

(defn -main [& args]
  (start-states!)
  (stream/start-with-env))
