(ns clinvar-streams.core-repl
  (:require [nrepl.core :as nrepl]
            [nrepl.server]
            [mount.core :refer [defstate]]
            [clinvar-streams.core]))

(defstate
  server
  :start (nrepl.server/start-server
           :init-ns *ns*
           :bind "127.0.0.1"
           :port 60001)
  :stop (nrepl.server/stop-server server))

(defn -main [& args]
  (mount.core/start #'server)
  (apply clinvar-streams.core/-main args))
