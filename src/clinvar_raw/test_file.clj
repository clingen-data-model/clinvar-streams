(ns clinvar-raw.test-file
  (:require [clojure.tools.cli :refer [parse-opts]]))

(def cli-options
  ;; An option with a required argument
  [[nil "--mode MODE" "Startup mode"
    :validate [#(.contains ["clinvar-raw" "clinvar-qc"] %) "Must be clinvar-raw or clinvar-qc"]]
   ;["-h" "--help"]
   ])

(defn -main [& args]
  (parse-opts args cli-options :in-order true))