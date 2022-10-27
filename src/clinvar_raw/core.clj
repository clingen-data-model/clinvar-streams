(ns clinvar-raw.core
  "Start processing of clinvar-raw stream"
  (:require [mount.core :as mount]
            [clinvar-raw.stream :as stream]
            [clinvar-raw.service]))

(defn start-states! []
  (mount/start #'clinvar-raw.stream/dedup-db
               #'clinvar-raw.service/service))

(defn -main [& args]
  (start-states!)
  (stream/start-with-env))
