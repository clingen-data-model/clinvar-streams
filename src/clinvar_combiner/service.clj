(ns clinvar-combiner.service
  (:require [clinvar-combiner.stream :as stream]
            [io.pedestal.http.route :as route]
            [io.pedestal.http :as http]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as log])
  (:import (java.time Duration)))

(defn pre-stop [request]
  (log/info "In prestop hook handler")
  (log/info "Shutting down streaming mode...")
  (reset! stream/run-streaming-mode-continue false)
  (while @stream/run-streaming-mode-is-running
    (log/info "Shutting down streaming mode...")
    (Thread/sleep (.toMillis (Duration/ofSeconds 1))))
  (log/info "Finished shutting down streaming mode.")
  {:status 200
   :headers {}
   :body nil})

(def routes
  (route/expand-routes
    #{["/PreStop" :get pre-stop :route-name :PreStop]}))

(defn create-server []
  (http/create-server
    {:io.pedestal.http/routes routes
     :io.pedestal.http/type :jetty
     :io.pedestal.http/join? false
     :io.pedestal.http/port 8080}))

(defn start []
  (http/start (create-server)))

(defstate service
          :start (http/start (create-server))
          :stop (http/stop service))
