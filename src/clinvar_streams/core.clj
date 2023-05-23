(ns clinvar-streams.core
  (:require [clinvar-raw.core :as raw-core]
            [clinvar-combiner.core :as combiner-core])
  (:gen-class))

(defn -main [& args]
  (assert (< 0 (count args)) "Must provide mode argument")
  (let [mode (first args)]
    (cond
      (or (= "clinvar-raw" mode)
          (= "single-release" mode)) (raw-core/-main args)
      (= "clinvar-combiner" mode) (combiner-core/-main (rest args))
      :else (throw (ex-info "Unrecognized startup mode" {:cause mode})))))
