(ns clinvar-raw.spec.spec-asserts
  (:require [clojure.spec.alpha :as spec]))

(defn string-is-int?
  "Returns true if input string `s` is an arbitrarily large integer"
  [s]
  (re-matches #"\d+" s))

(defn string-is-int-width?
  "Returns true if input string `s` is an arbitrarily large integer"
  [s width]
  (not (nil? (re-matches (re-pattern (str "\\d{" width "}")) s))))

(defn string-is-yyyy-mm-dd?
  "Returns true if input string `s` is a date with format dddd-dd-dd where d is an int in range [0-9]"
  [s]
  (not (nil? (re-matches (re-pattern "\\d{4}-\\d{2}-\\d{2}") s))))

(defn string-not-empty?
  "Returns true if input string `s` is not empty"
  [s]
  (< 0 (.length s)))

(defn scv-number?
  [s]
  (not (nil? (re-matches #"SCV\d+" s))))

(defn scv-number-versioned?
  [s]
  (not (nil? (re-matches #"SCV[\d.]+" s))))
