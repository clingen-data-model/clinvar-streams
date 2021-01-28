(ns clinvar-streams.core
  (:require [clojure.tools.cli :as cli]
            [clinvar-raw.core :as raw-core]
            [clinvar-qc.core :as qc-core])
  (:gen-class))

(def cli-matic-conf
  {:command "clinvar-streams"
   :opts    [{:as      "Startup Mode"
              :default nil
              :option  "mode"
              :type    :string}]})

;(def cli-options
;  ;; An option with a required argument
;  [[nil "--mode MODE" "Startup mode"
;    :validate [#(.contains ["clinvar-raw" "clinvar-qc"] %) "Must be clinvar-raw or clinvar-qc"]]])

(defn -main [& args]
  (assert (< 0 (count args)) "Must provide mode argument")
  (let [mode (first args)]
    (cond
      (= "clinvar-raw" mode) (raw-core/-main (rest args))
      (= "clinvar-qc" mode) (qc-core/-main (rest args)))))
