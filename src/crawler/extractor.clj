;;;; Soli Deo Gloria
;;;; spalakod@cs.cmu.edu

(ns crawler.extractor
  "Code to operate on a webpage and lookup URLs"
  (:require [clojure.set :as set]
            [crawler.dom :as dom]
            [crawler.records :as records]
            [crawler.utils :as utils]
            [itsy.core :as itsy]))

(def *xpath-scores* (atom {}))
(def *xpath-records* (atom {}))

(defn add-xpath
  [url-xpaths url xpath]
  (merge-with concat url-xpaths {url [xpath]}))

(defn update-xpaths-productivities
  "Expected data structure:
a vector of xpath and the associated anchor-tags
in the records"
  [records-anchors]
  (map
   (fn [[xpath anchor]])))

(defn process-page
  "Store the page signature"
  [url body]
  (let [xpaths-and-records (records/page-xpaths-records body)
        xpaths             (map #(-> % :xpath) xpaths-and-records)
        mined-records      (map #(-> % :records) xpaths-and-records)
        map-xpaths-records (into {} (map vector xpaths mined-records))
        records-anchors    (map (fn [rs]
                                  (apply clojure.set/union
                                         (flatten
                                          (map records/record-anchors rs))))
                                mined-records)]
    (do

      ;; update global xpath scores
      (swap! *xpath-records*
             utils/atom-merge-with
             clojure.set/union
             (into
              {}
              (map vector xpaths records-anchors))))))
