;;;; Soli Deo Gloria
;;;; spalakod@cs.cmu.edu

(ns crawler.extractor
  "Code to operate on a webpage and lookup URLs"
  (:require [clj-http.client :as client]
            [clojure.set :as set]
            [crawler.dom :as dom]
            [crawler.page :as page]
            [crawler.rank :as rank]
            [crawler.records :as records]
            [crawler.utils :as utils]
            [itsy.core :as itsy]
            [org.bovinegenius [exploding-fish :as uri]])
  (:use [clojure.tools.logging :only (info error)]
        [clj-logging-config.log4j])
  (:import (org.apache.commons.lang StringEscapeUtils)))

(utils/global-logger-config)

(def *page-sim-thresh* 0.85)
(def *sample-fraction* (/ 1 4))   ; fraction of links to look at
                                  ; before sampling

(def *xpath-hrefs*   (atom {}))   ; hold the unique hrefs for each xpath
(def *xpath-df*      (atom {}))   ; hold the df score for each xpath
(def *xpath-tf*      (atom {}))   ; per-page tf scores
(def *visited*       (atom
                      (set [])))  ; set of visited documents
(def *url-documents* (atom {}))   ; url -> body map

(defn add-xpath
  [url-xpaths url xpath]
  (merge-with concat url-xpaths {url [xpath]}))

(defn visit-and-record-page
  [a-link]
  (let [body (utils/get-and-log a-link)]
    (swap! *visited* conj a-link)
    (swap!
     *url-documents* utils/atom-merge-with set/union {a-link (set [body])})
    body))

(defn update-tf
  [xpaths-tfs]
  (doseq [[xpath tf] xpaths-tfs]
    (swap! *xpath-tf* utils/atom-merge-with concat {xpath [tf]})))

(defn record-explore-potential
  "At first, we just use # of distinct links"
  [a-record]
  (-> a-record
      (records/record-anchors)))

(defn sample
  ([urls host n]
     (sample urls host n []))
  
  ([urls host n sampled-list]
     (if (zero? n)
       sampled-list
       (let [candidates (filter
                         (fn [url]
                           (and
                            (= host (uri/host url))
                            (not (some #{url} @*visited*))
                            (not (some #{url} sampled-list))))
                         (into [] urls))
             
             sampled    (when (> (count candidates) 0)
                          (rand-nth candidates))]
         
         (if (and sampled
                  (= (uri/host sampled) host)
                  (not (some #{sampled} @*visited*))
                  (not (some #{sampled} sampled-list)))
           (recur urls host (dec n) (conj sampled-list sampled))
           (recur urls host 0 sampled-list))))))

(defn update-df
  "Each XPath's df score is incremented"
  [xpaths-hrefs]
  (let [xpaths     (map first xpaths-hrefs)
        xpaths-cnt (map (fn [x] {x 1}) xpaths)]
    (doseq [xpath-cnt xpaths-cnt]
      (swap! *xpath-df* utils/atom-merge-with +' xpath-cnt))))

(defn update-hrefs
  "Union of explored hrefs and observed hrefs"
  [xpaths-hrefs]
  (swap! *xpath-hrefs*
         utils/atom-merge-with
         set/union
         (into {} xpaths-hrefs)))

(defn novelty
  [a-diff]
  (reduce
   +
   0
   (map
    #(-> % second)
    a-diff)))

(defn explore-xpath-and-update
  "Explore an xpath and its href links."
  [xpath hrefs host signature in-host-xpath-hrefs]
  (info :exploring-xpath xpath)
  (let [sampled-uris (sample hrefs host (Math/ceil
                                         (/
                                          (count hrefs)
                                          4)))]
    (doall
     (map
      (fn [sampled]
       
        (let [body             (utils/get-and-log sampled {:xpath xpath})
             
              xpaths-hrefs'    (if body
                                 (try
                                   (dom/minimum-maximal-xpath-set body sampled)
                                   (catch Exception e
                                     (do (error "Failed to parse: " sampled)
                                         (error "Error caused by: " xpath))))
                                 nil)
             
              in-host-map      (into
                                {}
                                (filter
                                 (fn [[xpath hrefs]]
                                   (some (fn [a-href]
                                           (= (uri/host sampled)
                                              (uri/host a-href)))
                                         hrefs))
                                 xpaths-hrefs'))
                               
              in-host-xpath-hs (map first in-host-map)
              
              signature2       (into
                                {} (map (fn [an-xpath]
                                          [an-xpath (count (in-host-map an-xpath))])
                                        in-host-xpath-hs))
              
              ;; similarity of page w/ source page
              page-sim         (page/signature-similarity2 signature signature2)

              ;; xpaths of the source
              src-xpaths       (filter
                                #(not= % xpath)
                                (map first in-host-xpath-hrefs))
             
              diff             (map vector
                                    src-xpaths
                                    (map
                                     (fn [xpath]
                                       (count
                                        (set/difference
                                         (set (in-host-xpath-hrefs xpath))
                                         (set (in-host-map xpath)))))
                                     src-xpaths))]
         
          (when xpaths-hrefs'
            (do
              (update-df xpaths-hrefs')
              (update-hrefs xpaths-hrefs')
              (swap! *visited* conj sampled)
              (println sampled)
              (println page-sim)
              (if (> page-sim *page-sim-thresh*)
                {:enum-candidate true
                 :page-sim       page-sim
                 :url            sampled
                 :novelty        (novelty diff)
                 :hrefs-table    xpaths-hrefs'}
                {:enum-candidate false
                 :url            sampled
                 :novelty        (novelty diff)
                 :hrefs-table    xpaths-hrefs'})))))
      sampled-uris))))            

(defn is-enum-candidate?
  [{xpath :xpath explorations :explorations}]
  (some
   (fn [an-exploration]
     (-> an-exploration
         :enum-candidate))
   explorations))

(defn enum-candidate-info
  "Uses the information sent and the snapshot info"
  [{xpath :xpath explorations :explorations}]
  (let [df          (@*xpath-df* xpath)
        hrefs       (@*xpath-hrefs* xpath)
        avg-novelty (/ (apply + (map #(-> % :novelty) explorations))
                       (count explorations))]
    {:xpath       xpath
     :df          df
     :hrefs       (map #(StringEscapeUtils/unescapeHtml %) hrefs)
     :avg-novelty avg-novelty}))

(defn explorations->href-table
  [explorations]
  (filter
   identity
   (map #(-> % :explorations :href-table) explorations)))

(defn process-link
  [url]
  (let [body                  (visit-and-record-page url)
        
        xpaths-hrefs          (dom/minimum-maximal-xpath-set
                                 body url)

        _                     (update-df xpaths-hrefs)

        _                     (update-hrefs xpaths-hrefs)
        
        xpaths                (map first xpaths-hrefs)
        
        in-host-xpath-hrefs   (filter
                               (fn [[xpath hrefs]]
                                 (some (fn [a-href]
                                         (= (uri/host url)
                                            (uri/host a-href)))
                                       hrefs))
                               xpaths-hrefs)

        ih-xp-map             (into {} in-host-xpath-hrefs)
        
        in-host-xpaths        (map first in-host-xpath-hrefs)

        signature             (into
                               {} (map (fn [an-xpath]
                                         [an-xpath (count (ih-xp-map an-xpath))]) in-host-xpaths))
        
        explorations          (map
                               (fn [[xpath hrefs]]
                                 {:xpath        xpath
                                  :explorations (explore-xpath-and-update
                                                 xpath
                                                 (map #(StringEscapeUtils/unescapeHtml %) hrefs)
                                                 (uri/host url)
                                        ;in-host-xpaths
                                                 signature
                                                 (into {} in-host-xpath-hrefs))})
                               in-host-xpath-hrefs)

        hrefs-table           (explorations->href-table explorations)
        
        enum-candidates       (filter is-enum-candidate? explorations)

        enum-candidates-info  (map enum-candidate-info enum-candidates)]
    (println hrefs-table)
    (rank/rank-enum-candidates enum-candidates-info)))

(defn reset
  "Flush all global atoms"
  []
  (utils/reset *xpath-hrefs* {})
  (utils/reset *xpath-df* {})
  (utils/reset *xpath-tf* {})
  (utils/reset *visited* [])
  (utils/reset *url-documents* {}))
