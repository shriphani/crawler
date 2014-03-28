(ns crawler.model
  "Contains code to read and process a
   sampled model"
  (:require [clojure.java.io :as io]
            [crawler.corpus :as corpus]
            [crawler.dom :as dom]
            [structural-similarity.xpath-text :as similarity])
  (:use [clojure.pprint :only [pprint]])
  (:import [java.io PushbackReader]))

(defn read-model
  [a-model-file]
  (with-open [rdr (io/reader a-model-file)]
    (-> rdr PushbackReader. read)))

(defn read-corpus
  [a-corpus-file]
  (with-open [rdr (io/reader a-corpus-file)]
    (-> rdr PushbackReader. read)))

(defn not-paginated
  [an-action-seq pagination]
  (not
   (some
    (fn [[src-xpath actions]]
      (some
       (fn [action]
         (let [paging-action (cons action src-xpath)]
           (some
            (fn [[x y]]
              (= x y))
            (map vector paging-action (reverse an-action-seq)))))
       actions))
    pagination)))

(defn trim-pagination
  [model pagination]
  (filter
   (fn [[action-seq _]]
     (not-paginated action-seq pagination))
   model))

(defn fix-model
  "Code that takes a sampled model and
   fixes up the crawled model"
  [model-file corpus-file]
  (let [model       (read-model model-file)
        corpus-data (corpus/read-corpus-file corpus-file)

        pagination (corpus/pagination-in-corpus corpus-data)
        pagination-trimmed (trim-pagination model pagination)]
    {:action     pagination-trimmed
     :pagination pagination}))

