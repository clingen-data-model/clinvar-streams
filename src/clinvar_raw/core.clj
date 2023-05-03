(ns clinvar-raw.core
  (:require [mount.core :as mount]
            [clinvar-raw.single-release :as single-release]
            [clinvar-raw.stream :as stream]
            [clinvar-raw.service :as service]))

(defn start-states! [args]
  (mount/start #'stream/dedup-db)
  (mount/start #'service/service))

(defn -main [args]
  (if (= "single-release" (first args))
    (single-release/start-with-env args)
    (do
      (start-states! args)
      (stream/start-with-env args))))
