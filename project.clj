(defproject clinvar-streams "0.1.0-SNAPSHOT"
  :description "ClinVar data streams for ClinGen ecosystem applications"
  :url "https://github.com/clingen-data-model/clinvar-streams"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/core.async "1.6.673"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.clojure/tools.cli "1.0.214"]
                 [org.clojure/tools.namespace "1.3.0"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [cheshire "5.11.0"]
                 [cli-matic "0.5.4"]
                 [clj-commons/fs "1.6.310"]
                 [com.google.cloud/google-cloud-storage "2.15.1"]
                 [com.taoensso/encore "3.42.0"]
                 [com.taoensso/nippy "3.2.0"]
                 [com.taoensso/timbre "6.0.3"]
                 [digest/digest "1.4.10"]
                 [fundingcircle/jackdaw "0.9.4"]
                 [io.pedestal/pedestal.jetty "0.5.8"]
                 [io.pedestal/pedestal.route "0.5.8"]
                 [io.pedestal/pedestal.service "0.5.8"]
                 [mount "0.1.16"]
                 [org.clj-commons/hickory "0.7.3"]
                 [org.postgresql/postgresql "42.5.1"]
                 [org.slf4j/slf4j-simple "2.0.5"]
                 [org.xerial/sqlite-jdbc "3.40.0.0"]]
  :repl-options {:init-ns clinvar-raw.stream
                 :caught clojure.repl/pst
                 :jvm-opts ["-Xms256m"
                            #_"-XX:MaxRAMPercentage=25"
                            "-Xmx1024m"]}
  :jvm-opts ["-Xms256m"]
  :main clinvar-streams.core
  :aot [clinvar-streams.core]
  :resource-paths ["resources"]
  :target-path "target/%s"
  :auto-clean false
  :profiles
  {#_#_:run-with-repl {:main clinvar-streams.core-repl
                       :repl-options {:init-ns clinvar-streams.core-repl}}
   :uberjar {:uberjar-name "clinvar-streams.jar"
             :aot [clinvar-streams.core]}
   :testdata {:main clinvar-raw.generate-local-topic
                                        ;:aot [#"clinvar-raw.*"]
              :repl-options {:init-ns clinvar-raw.generate-local-topic}}})
