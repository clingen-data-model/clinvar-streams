(ns clinvar-raw.core
  (:require [clojure.pprint :as pprint]
            [mount.core]
            [clinvar-raw.stream :as stream]
            [clinvar-raw.service]
            [clinvar-raw.ingest]))

(defn start-states! []
  (mount.core/start #'clinvar-raw.stream/dedup-db
                    #'clinvar-raw.service/service))

(defn -main [& args]
  (start-states!)
  (stream/start-with-env))
