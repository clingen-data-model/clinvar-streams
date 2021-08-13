(ns clinvar-combiner.test
  (:require [clojure.core.async :as async])
  (:import (java.util Date)))

(defmacro time-fn
  "Evaluates expr and prints the time it took.  Returns the value of
 expr."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (prn (str "Elapsed time: " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs"))
     ret#))



(def seq-size 10000)
(def pipeline-size 1000)
(def sleep-range [5 10])

(defn f1
  [val]
  ; sleep for random ms [1,5]
  (Thread/sleep (+ (first sleep-range)
                   (rand-int (- (second sleep-range)
                                (first sleep-range)))))
  (* 2 val))

(defn -main-noasync [& args]
  (let [start-time (Date.)
        output (into [] (map f1 (range seq-size)))
        end-time (Date.)]
    (printf "seq size %d pipeline size %d duration %d\n"
            seq-size pipeline-size (- (.getTime end-time) (.getTime start-time)))
    ))

(defn -main [& args]
  (let [c-in (async/chan 1e6)
        c-out (async/chan 1e6)]
    (async/go
      (doseq [i (range seq-size)]
        (async/>! c-in i)
        )
      (async/close! c-in)
      )

    (println "Starting pipeline")
    (let [start-time (Date.)]
      (async/pipeline pipeline-size c-out (map f1) c-in true)

      (let [output (async/<!! (async/into [] c-out))
            end-time (Date.)]
        (printf "seq size %d pipeline size %d duration %d\n"
                seq-size pipeline-size (- (.getTime end-time) (.getTime start-time)))
        ;(println output)
        )
      ))
  )
