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

(def *visited*       (atom
                      (set [])))  ; set of visited documents
(def *url-documents* (atom {}))   ; url -> body map

(defn add-xpath
  [url-xpaths url xpath]
  (merge-with concat url-xpaths {url [xpath]}))

(defn visit-and-record-page

  ([a-link]
     (visit-and-record-page a-link {}))
  
  ([a-link info]
     (let [body (utils/get-and-log a-link info)]
       (swap! *visited* conj a-link)
       (swap!
        *url-documents* utils/atom-merge-with set/union {a-link (set [body])})
       body)))

(defn sample
  ([urls host n]
     (let [candidates (filter
                       (fn [url]
                         (and
                          (= host (uri/host url))
                          (not (some #{url} @*visited*))))
                       (into [] urls))]
       (if (<= (count candidates) 5)
         candidates
         (sample urls host n []))))
  
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

(defn novelty
  [a-diff]
  (reduce
   + 0 (map #(-> % second) a-diff)))

(defn in-host-xpaths-hrefs
  [xpaths-hrefs host]
  (into {} (filter
            (fn [[xpath hrefs]]
              (some
               (fn [a-href]
                 (= host (uri/host a-href)))
               hrefs))
            xpaths-hrefs)))

(defn explore-xpath
  "Explore an xpath. This version holds
values and doesn't do any of that ugly global
nonsense"
  [xpath hrefs host signature in-host-xpath-hrefs]
  (let [sampled-uris     (sample
                          hrefs host (Math/ceil
                                      (/ (count hrefs) 4)))]
    (println "Xpath:" xpath)
    (map
     (fn [sampled]
       (let [body             (visit-and-record-page sampled {:xpath xpath})
            
             xpaths-hrefs'    (when body
                                (try
                                  (dom/minimum-maximal-xpath-set body sampled)
                                  (catch Exception e
                                    (do (error "Failed to parse: " sampled)
                                        (error "Error caused by: " xpath)))))
             
             in-host-map      (in-host-xpaths-hrefs
                               xpaths-hrefs' (uri/host sampled))

             in-host-xpath-hs (map first in-host-map)

             pg-signature     (page/page-signature in-host-xpath-hs in-host-map)

             similarity       (page/signature-similarity2 signature pg-signature)

             src-xpaths       (filter
                               #(not= % xpath) (map first in-host-xpath-hrefs))

             diff             (map vector
                                   src-xpaths
                                   (map
                                    (fn [xpath]
                                      (count
                                       (set/difference
                                        (set (in-host-map xpath))
                                        (set (in-host-xpath-hrefs xpath)))))
                                    src-xpaths))]
         (println "URI:" sampled)
         (println similarity)
         (when xpaths-hrefs'
           (do
             (swap! *visited* conj sampled)
             {:url         sampled
              :similarity  similarity
              :novelty     (novelty diff)
              :hrefs-table in-host-map}))))
     sampled-uris)))

(defn is-enum-candidate?
  "Average-similarity must be over threshold"
  [{xpath :xpath explorations :explorations}]
  (let [similarities (map (fn [an-exploration]
                            (-> an-exploration
                                :similarity))
                          explorations)]
    (when (and (seq explorations) (< 0 (count explorations)))
      (try (> (/ (apply + similarities) (count explorations)) *page-sim-thresh*)
           (catch Exception e nil)))))

(defn enum-candidate-info
  "Uses the information sent and the snapshot info"
  [dfs hrefs xpath]
  {:xpath xpath
   :df    (dfs xpath)
   :hrefs (hrefs xpath)})

(defn df-table
  [explorations]
  (reduce
   (fn [acc v]
     (merge-with + acc v))
   {}
   (map
    (fn [{xpath :xpath xpath-explorations :explorations}]
      (reduce
       (fn [acc v]
         (merge-with + acc v))
       {}
       (map
        (fn [a-href-table]
          (reduce
           (fn [acc v]
             (merge-with + acc v))
           {}
           (map (fn [an-xpath] {an-xpath 1}) (keys a-href-table))))
        (map #(-> % :hrefs-table) xpath-explorations))))
    explorations)))

(defn href-table
  [explorations]
  (reduce
   (fn [acc v]
     (merge-with set/union acc v))
   {}
   (map
    (fn [{xpath :xpath xpath-explorations :explorations}]
      (reduce
       (fn [acc v]
         (merge-with set/union acc v))
       {}
       (map #(-> % :hrefs-table) xpath-explorations)))
    explorations)))

(defn novelty-table
  [explorations]
  (reduce
   merge
   {}
   (filter
    identity
    (map
     (fn [explorations]
       (try
         (let [xpath              (:xpath explorations)
               xpath-explorations (:explorations explorations)]
           {xpath
            (if (or (not xpath-explorations)
                    (zero? (count xpath-explorations)))
              0
              (/ (apply
                  + (map #(:novelty %) xpath-explorations))
                 (count xpath-explorations)))})
         (catch Exception e nil)))
     explorations))))

(defn enum-candidate-info
  [enum-candidate dfs hrefs novelties]
  {:xpath       enum-candidate
   :df          (dfs enum-candidate)
   :hrefs       (hrefs enum-candidate)
   :avg-novelty (novelties enum-candidate)})

(defn process-link

  ([url]
     (process-link url {} {}))
  
  ([url *xpath-df* *xpath-hrefs*]
   (let [body                  (visit-and-record-page url)
        
         xpaths-hrefs          (dom/minimum-maximal-xpath-set
                                body url)

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
                                          [an-xpath (count (ih-xp-map an-xpath))])
                                        in-host-xpaths))
        
         explorations          (map
                                (fn [[xpath hrefs]]
                                  {:xpath        xpath
                                   :explorations (explore-xpath
                                                  xpath
                                                  (map #(StringEscapeUtils/unescapeHtml %) hrefs)
                                                  (uri/host url)
                                                  signature
                                                  (into {} in-host-xpath-hrefs))})
                                in-host-xpath-hrefs)

         dfs                   (merge-with
                                +
                                (reduce
                                 (fn [acc v] (merge-with + acc v))
                                 {}
                                 (map (fn [x] {x 1}) in-host-xpaths))
                                (df-table explorations))

         hrefs                 (merge-with set/union ih-xp-map (href-table explorations))

         novelties             (novelty-table explorations)

         enum-candidates       (map #(-> % :xpath)
                                    (filter is-enum-candidate? explorations))

         enum-candidates-info  (map (fn [x]
                                      (enum-candidate-info x dfs hrefs novelties))
                                    enum-candidates)]
     
     (rank/rank-enum-candidates enum-candidates-info))))

