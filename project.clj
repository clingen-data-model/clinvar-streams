(defproject clinvar-streams "0.1.0-SNAPSHOT"
  :description "ClinVar data streams for ClinGen ecosystem applications"
  :url "https://github.com/clingen-data-model/clinvar-streams"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.2.603"]
                 [org.clojure/tools.namespace "1.0.0"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.clojure/tools.cli "1.0.194"]
                 [cheshire "5.10.0"]
                 [fundingcircle/jackdaw "0.7.4"]
                 [com.google.cloud/google-cloud-storage "1.101.0"]
                 [com.taoensso/timbre "4.10.0"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [org.postgresql/postgresql "42.2.16"]
                 [org.xerial/sqlite-jdbc "3.32.3.2"]
                 ]
  :repl-options {:init-ns clinvar-combiner.core
                 :caught clojure.repl/pst}
  :main ^:skip-aot clinvar-streams.core
  :aot [clinvar-streams.core]
  :target-path "target/%s"
  :auto-clean false
  :profiles {:uberjar {:uberjar-name "clinvar-streams.jar"
                       :aot :all}
             :testdata {:main clinvar-raw.generate-local-topic
                        ;:aot [#"clinvar-raw.*"]
                        :repl-options {:init-ns clinvar-raw.generate-local-topic}}
             }
  )
