;;;; Soli Deo Gloria
;;;; spalakod@cs.cmu.edu

(ns crawler.extractor
  "Code to operate on a webpage and lookup URLs"
  (:require [clj-http.client :as client]
            [clojure.set :as set]
            [crawler.dom :as dom]
            [crawler.page :as page]
            [crawler.records :as records]
            [crawler.utils :as utils]
            [itsy.core :as itsy]
            [org.bovinegenius [exploding-fish :as uri]]))

(def *xpath-hrefs* (atom {})) ; hold the unique hrefs for each xpath
(def *xpath-df* (atom {}))    ; hold the df score for each xpath
(def *visited* (atom (set [])))

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

(defn sample
  [urls host]
  (let [candidates (filter
                    (fn [url]
                      (and (= host (uri/host url))
                           (not (some #{url} @*visited*))))
                    (into [] urls))
        
        sampled    (when (> (count candidates) 0)
                     (rand-nth candidates))]
    
    (when sampled
     (if (and (= (uri/host sampled) host)
              (not (some #{sampled} @*visited*)))
       sampled
       (recur urls host)))))

(defn update-df
  [xpaths-hrefs]
  (let [xpaths     (map first xpaths-hrefs)
        xpaths-cnt (map (fn [x] {x 1}) xpaths)]
    (doseq [xpath-cnt xpaths-cnt]
      (swap! *xpath-df* utils/atom-merge-with +' xpath-cnt))))

(defn update-hrefs
  [xpaths-hrefs]
  '*)

(defn explore-xpath-and-update
  "Sample a link from xpath-hrefs,
make an update to the global table"
  [xpath hrefs host signature]
  (let [sampled          (sample hrefs host)
        
        body             (try (-> (client/get sampled) :body)
                              (catch Exception e
                                (do (println "Failed: " xpath)
                                    (println "URL: " sampled)
                                    (println (.getMessage e)))))
        xpaths-hrefs'    (if body
                           (dom/minimum-maximal-xpath-set body sampled)
                           nil)
        in-host-xpath-hs (map
                          first
                          (filter
                           (fn [[xpath hrefs]]
                             (some (fn [a-href]
                                     (= (uri/host sampled)
                                        (uri/host a-href)))
                                   hrefs))
                           xpaths-hrefs'))

        page-sim         (page/signature-similarity
                          signature in-host-xpath-hs)]
    (if xpaths-hrefs'
     (do
       (update-df xpaths-hrefs')
       (update-hrefs xpaths-hrefs')
       (swap! *visited* conj sampled)

       ;; do something about enumeration here)
     nil)))

(defn process-new-page
  "Store the page signature"
  [url body one-hop?]
  (do
    (swap! *visited* conj url)
    (let [xpaths-hrefs        (dom/minimum-maximal-xpath-set
                              body url)
          in-host-xpath-hrefs (filter
                               (fn [[xpath hrefs]]
                                 (some (fn [a-href]
                                         (= (uri/host url)
                                            (uri/host a-href)))
                                       hrefs))
                               xpaths-hrefs)
          in-host-xpaths      (map first in-host-xpath-hrefs)]
      (doseq [[xpath hrefs] in-host-xpath-hrefs]
        (explore-xpath-and-update
         xpath hrefs (uri/host url) in-host-xpaths)))))
