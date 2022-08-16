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


#_(defn -main-unsafe
    [& args]
    (.start (Thread. (partial stream/process-drop-messages (cfg/app-config))))
    (.start (Thread. (partial stream/send-producer-messages
                              (cfg/app-config)
                              (cfg/kafka-config (cfg/app-config)))))

    (stream/listen-for-clinvar-drop
     (cfg/app-config)
     (cfg/kafka-config (cfg/app-config))))

(defn -main [& args]
  (mount.core/start #'clinvar-raw.service/service)
  (stream/start))
