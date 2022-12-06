(ns clinvar-raw.core
  (:require [mount.core :as mount]
            [clinvar-raw.stream :as stream]
            [clinvar-raw.service]
            [clinvar-raw.ingest]))

(defn start-states! []
  (mount/start #'clinvar-raw.stream/dedup-db
               #'clinvar-raw.service/service))

(defn -main [& args]
  (start-states!)
  (stream/start-with-env))
