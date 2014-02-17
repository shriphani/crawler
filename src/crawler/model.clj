(ns crawler.model
  "Contains code to read and process a
   sampled model"
  (:require [clojure.java.io :as io]
            [crawler.dom :as dom])
  (:import [java.io PushbackReader]))

(defn read-model
  [a-model-file]
  (with-open [rdr (io/reader a-model-file)]
    (-> rdr PushbackReader. read)))

(defn plan
  "Plan of execution for the model"
  [a-model]
  (let [total (apply + (map second a-model))]
    (sort-by (juxt
              #(-> % first count)
              #(- total (second %))) a-model)))
