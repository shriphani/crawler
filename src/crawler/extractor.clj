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

(defn record-explore-potential
  "At first, we just use # of distinct links"
  [a-record]
  (-> a-record
      (records/record-anchors)))

(defn process-page
  "Store the page signature"
  [url body]
  (let [xpaths-and-records (into [] (records/page-xpaths-records body))
        xpaths             (map first xpaths-and-records)
        records            (map second xpaths-and-records)
        records-anchors    (map (fn [a-record-set]
                                  (->> a-record-set
                                       (map
                                        #(record-explore-potential %))))
                                records)
        records-hrefs      (map
                            (fn [a-record-set]
                              (count (apply set/union a-record-set)))
                            records-anchors)]
    (map vector xpaths records-hrefs)))
