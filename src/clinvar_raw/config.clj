(ns clinvar-raw.config
  (:require [clojure.walk :as walk]
            [taoensso.timbre :as timbre]
            [clinvar-streams.config :as streams-config]))

(timbre/set-level! :info)

(def env-config
  (merge streams-config/env-config
         (-> (System/getenv)
             (select-keys
              (map name [:DX_CV_RAW_INPUT_TOPIC
                         :DX_CV_RAW_OUTPUT_TOPIC
                         :DX_CV_RAW_MAX_INPUT_COUNT]))
             walk/keywordize-keys)))

(def appender
  "File appender for timbre."
  (timbre/spit-appender
   {:fname (str (:CLINVAR_STREAMS_DATA_DIR env-config)
                "logs/clinvar-streams.log")}))

(timbre/swap-config!
 #(update % :appenders merge {:file appender}))

(defn kafka-config
  "Uses :KAFKA_USER :KAFKA_PASSWORD, :KAFKA_HOST :KAFKA_GROUP in opts."
  [{:keys [KAFKA_GROUP KAFKA_BROKER KAFKA_PASSWORD KAFKA_USER] :as opts}]
  (streams-config/kafka-config opts))
