(ns clinvar-raw.service
  (:require [clinvar-raw.stream :as stream]
            [io.pedestal.http.route :as route]
            [io.pedestal.http :as http]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as log]
            [clojure.core.async :as async])
  (:import (java.time Duration Instant)
           (java.util Date)))


(defn pre-stop [request]
  (log/info "In prestop hook handler")
  (let [prn-log #(log/info "Shutting down clinvar-raw streaming...")]
    (prn-log)
    (reset! stream/listening-for-drop false)
    (async/close! stream/producer-channel)
    (while @stream/is-any-thread-running?
      (log/info "PreStop shutdown hook waiting for " #'stream/is-any-thread-running?)
      (Thread/sleep (.toMillis (Duration/ofSeconds 3)))))
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
     :io.pedestal.http/host "0.0.0.0"
     :io.pedestal.http/port 8080}))

(defn start []
  (http/start (create-server)))

(defstate service
          :start (http/start (create-server))
          :stop (http/stop service))
