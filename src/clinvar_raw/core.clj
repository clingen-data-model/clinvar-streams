(ns clinvar-raw.core
  (:require [jackdaw.client.log :as jl]
            [clojure.pprint :as pprint]
            [cheshire.core :as json]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as log]
            [clinvar-raw.config :as cfg]
            [clinvar-raw.stream :as stream]
            [clinvar-raw.service])
  (:import (java.lang Thread)
           (java.time Duration))
  (:gen-class))


(defn -main [& args]
  (mount.core/start #'clinvar-raw.service/service)
  (stream/start))
