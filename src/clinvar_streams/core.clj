(ns clinvar-streams.core
  (:require [clojure.tools.cli :as cli]
            [clinvar-raw.core :as raw-core]
            ;[clinvar-qc.core :as qc-core]
            [clinvar-combiner.core :as combiner-core])
  (:gen-class))

(defn -main [& args]
  (assert (< 0 (count args)) "Must provide mode argument")
  (let [mode (first args)]
    (cond
      (= "clinvar-raw" mode) (raw-core/-main (rest args))
      (= "clinvar-combiner" mode) (combiner-core/-main (rest args))
      :default (throw (ex-info "Unrecognized startup mode" {:cause mode})))))
