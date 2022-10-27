(ns clinvar-raw.service
  "Pedestal server to handle communication with Kubernetes."
  (:require [clinvar-raw.stream :as stream]
            [io.pedestal.http.route :as route]
            [io.pedestal.http :as http]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as log]))

;;; docstrings? 
(defn pre-stop [request]
  (log/info "In prestop hook handler")
  (let [prn-log #(log/info "Shutting down clinvar-raw streaming...")] ;; consider log/error, warn
    (prn-log)
    (reset! stream/listening-for-drop false)
    (Thread/sleep (* 10 1000)))
  (log/info (str "Finished shutting down streaming mode."
                 " Processing thread may continue if a release is in progress."))
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

(declare service)
(defstate service
  :start (http/start (create-server))
  :stop (http/stop service))
