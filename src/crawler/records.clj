;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.records
  (:require [crawler.dom :as dom]
            [crawler.utils :as utils]
            [clojure.string :as string])
  (:use     [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]]
            (incanter stats)))

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

(defn positive-outlier-records
  "Outlier in terms of # of individual records"
  [record-sets]
  (let [mean-record-set-size  (/ (reduce +' 0 (map count record-sets))
                                 (count record-sets))

        differences           (map #(utils/abs (- (count %) mean-record-set-size))
                                   record-sets)
        
        stdev-record-set-size (/ (reduce +' 0 differences)
                                 (count differences))]
    
    (filter
     #(> (count %) (+ mean-record-set-size
                      (* 1.5 stdev-record-set-size)))
     record-sets)))

(defn records-diversity
  [record-set]
  (try (->> record-set
            first
            ($x:node+ ".//*[not(*)]")
            count)
       (catch Exception e 0)))

(defn resolve-records
  [records]
  (reduce
   (fn [acc v]
     (merge-with concat acc {(first (dom/xpath-to-node (first v))) v}))
   {}
   records))

(defn record-signatures
  [page-src]
  (let [xpaths                 (:xpaths
                                (dom/minimum-maximal-xpath-set page-src))

        records                (dom/minimum-maximal-xpath-records
                                xpaths (dom/html->xml-doc page-src))

        anchor-records         (filter
                                #(= "a" (.getNodeName (first %)))
                                records)

        resolved-records       (vals (resolve-records records))

        not-anchor-only        (filter
                                (fn [rs] (not= (.getNodeName (first rs))
                                              "a"))
                                resolved-records)

        positive-outlier-recs  (positive-outlier-records not-anchor-only)]
    
    (if (seq positive-outlier-recs)
      (cons {:outlier        positive-outlier-recs
             :outlier-xpaths (map #(first
                                    (dom/xpath-to-node (first %)))
                                  positive-outlier-recs)}
            (map
             #(first
               (dom/xpath-to-node
                (first %)))
             (reverse
              (sort-by
               records-diversity not-anchor-only))))
      (cons
       {:outlier nil
        :outlier-xpaths nil}
       (map
        #(first
          (dom/xpath-to-node
           (first %)))
        (reverse
         (sort-by
          records-diversity not-anchor-only)))))))

(defn record-signatures-2
  [page-src]
  (let [xpaths                 (:xpaths
                                (dom/minimum-maximal-xpath-set page-src))

        records                (dom/minimum-maximal-xpath-records
                                xpaths (dom/html->xml-doc page-src))

        anchor-records         (filter
                                #(= "a" (.getNodeName (first %)))
                                records)

        resolved-records       (vals (resolve-records records))

        not-anchor-only        (reverse
                                (sort-by
                                 records-diversity resolved-records))]
    
    
    {:outlier        (first not-anchor-only)
     :outlier-xpaths (first
                      (dom/xpath-to-node (first (first not-anchor-only))))
     :records        (first not-anchor-only)}))
