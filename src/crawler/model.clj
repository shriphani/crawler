(ns crawler.model
  "Contains code to read and process a
   sampled model"
  (:require [clojure.java.io :as io])
  (:import [java.io PushbackReader]))

(defn read-model
  [a-model-file]
  (with-open [rdr (io/reader a-model-file)]
    (-> rdr PushbackReader. read)))

(defn identify-paging-filter-candidates
  [{model :model corpus :corpus}]
  (group-by
   first
   (map first model)))
