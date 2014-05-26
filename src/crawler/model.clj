(ns crawler.model
  "Contains code to read and process a
   sampled model"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crawler.corpus :as corpus]
            [crawler.dom :as dom]
            [structural-similarity.xpath-text :as similarity])
  (:use [clojure.pprint :only [pprint]])
  (:import [java.io PushbackReader]))

(defn read-model
  [a-model-file]
  (with-open [rdr (io/reader a-model-file)]
    (-> rdr PushbackReader. read)))

(defn associated-corpus-file
  "Finds the corpus file that produced
   provided model file"
  [a-model-file]
  (string/replace a-model-file #".model" ".corpus"))
