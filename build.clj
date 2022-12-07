(ns build
  "Build this thing."
  (:require [clojure.tools.build.api :as b]))

(def defaults
  "The defaults to configure a build."
  (let [classes "target/classes"]
    {:class-dir  classes
     :exclude    [#"^META-INF/LICENSE$"]
     :java-opts  ["-Dclojure.main.report=stderr"]
     :main       'clinvar-streams.core
     :path       "target"
     :project    "deps.edn"
     :target-dir classes
     :uber-file  "target/clinvar-streams.jar"}))

(defn uber
  "Throw or make an uberjar from source."
  [_]
  (let [{:keys [paths] :as basis} (b/create-basis defaults)
        project                   (assoc defaults :basis basis)]
    (b/delete      project)
    (b/copy-dir    (assoc project :src-dirs paths))
    (b/compile-clj (-> project
                       (dissoc :src-dirs)
                       (assoc :ns-compile '[clinvar-streams.core])))
    (b/uber        project)))
