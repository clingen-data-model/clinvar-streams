(defproject clinvar-streams "0.1.0-SNAPSHOT"
  :description "ClinVar data streams for ClinGen ecosystem applications"
  :url "https://github.com/clingen-data-model/clinvar-streams"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.2.603"]
                 [org.clojure/tools.namespace "1.1.0"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.clojure/tools.cli "1.0.206"]
                 [io.pedestal/pedestal.service "0.5.7"]
                 [io.pedestal/pedestal.route "0.5.7"]
                 [io.pedestal/pedestal.jetty "0.5.7"]
                 [org.slf4j/slf4j-simple "1.7.28"]
                 [mount "0.1.16"]
                 [cli-matic "0.4.3"]
                 [cheshire "5.10.0"]
                 [mount/mount "0.1.16"]
                 [clj-commons/fs "1.5.2"]
                 [fundingcircle/jackdaw "0.7.4"]
                 [com.google.cloud/google-cloud-storage "1.115.0"]
                 [com.taoensso/encore "3.24.0"]
                 [com.taoensso/nippy "3.2.0"]
                 [com.taoensso/timbre "5.2.1"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [digest/digest "1.4.10"]
                 [nrepl "0.8.3"]
                 [org.postgresql/postgresql "42.2.16"]
                 [org.xerial/sqlite-jdbc "3.32.3.2"]]
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
  :profiles {;:run-with-repl {:main clinvar-streams.core-repl
             ;            :repl-options {:init-ns clinvar-streams.core-repl}}
             :uberjar {:uberjar-name "clinvar-streams.jar"
                       :aot [clinvar-streams.core]}
             :testdata {:main clinvar-raw.generate-local-topic
                        ;:aot [#"clinvar-raw.*"]
                        :repl-options {:init-ns clinvar-raw.generate-local-topic}}})
