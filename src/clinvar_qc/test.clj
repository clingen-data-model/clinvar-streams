;(ns clinvar-qc.test
;  (:require [clojure.java.io :as io]
;            [clojure.string :as s]
;            ;[genegraph.database.instance :refer [db]]
;            ;[genegraph.database.names :as names]
;            ;[genegraph.database.query.resource :as resource]
;            ;[genegraph.database.util :as util])
;            )
;  (:import (org.apache.jena.rdf.model Model)
;           (org.apache.jena.riot RDFDataMgr)))
;
;
;
;(defn main
;  []
;  (let [model (RDFDataMgr/loadModel "src/clinvar_repair/clinical_assertion.jsonld")]
;    (println model)
;    ))