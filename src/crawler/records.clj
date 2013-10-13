;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.records
  (:require [crawler.dom :as dom]
            [crawler.utils :as utils]
            [clojure.string :as string])
  (:use     [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]]))

(defn tag-only-children
  [a-node]
  (filter
   #(and (not= "#document" (.getNodeName %))
         (not= "#text" (.getNodeName %)))
   (dom/node-children a-node)))

(defn record-signature
  "A signature for a set of records on a page.
Typically consists of an xpath to a record
and names + classes of the constituent elements"
  [a-record-set]
  (let [first-record          (first  a-record-set)
        xpath-to-first-record (first (dom/xpath-to-node first-record))]
    {:xpath    xpath-to-first-record
     :contents (distinct
                (map #(.getNodeName %)
                     (tag-only-children first-record)))}))

(defn resolve-records
  [records]
  (reduce
   (fn [acc v]
     (merge-with concat acc {(first (dom/xpath-to-node (first v))) v}))
   {}
   records))

(defn page-xpaths-records
  "Return a list of xpaths and records on a page
ranked by just the frequency of a record on the page"
  [page-src]
  (let [xpaths          (:xpaths
                         (dom/minimum-maximal-xpath-set page-src))

        records         (dom/minimum-maximal-xpath-records
                         xpaths (dom/html->xml-doc page-src))

        resolved-records (resolve-records records)]
    
    (->> resolved-records
         (sort-by #(count (second %)))
         reverse)))
