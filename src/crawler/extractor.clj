;;;; Soli Deo Gloria
;;;; spalakod@cs.cmu.edu

(ns crawler.extractor
  "Code to operate on a webpage and lookup URLs"
  (:require [clj-http.client :as client]
            [clojure.set :as set]
            [crawler.cluster :as cluster]
            [crawler.dom :as dom]
            [crawler.page :as page]
            [crawler.rank :as rank]
            [crawler.records :as records]
            [crawler.utils :as utils]
            [org.bovinegenius [exploding-fish :as uri]])
  (:use [clojure.tools.logging :only (info error)]
        [clj-logging-config.log4j])
  (:import (org.apache.commons.lang StringEscapeUtils)))

(utils/global-logger-config)

(def *sample-fraction* (/ 1 4))   ; fraction of links to look at
                                        ; before sampling

(def *page-sim-thresh* 0.9)

(def *visited* (atom (set [])))  ; set of visited documents

(def *url-documents* (atom {}))   ; url -> body map

(defn add-xpath
  [url-xpaths url xpath]
  (merge-with concat url-xpaths {url [xpath]}))

(defn visit-and-record-page

  ([a-link]
     (visit-and-record-page a-link {}))
  
  ([a-link info]
     (let [body (utils/get-and-log a-link info)]
       (swap! *visited* utils/cons-aux a-link)
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
       (if (<= (count candidates) 10)
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
           (recur urls host (dec n) (utils/cons-aux sampled-list sampled))
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

(defn update
  [original-hrefs current-pg-hrefs]
  (let [orig-set (set original-hrefs)
        curr-set (set current-pg-hrefs)]
    (try (count (set/difference curr-set orig-set))
         (catch Exception e nil))))

(defn explore-xpath
  "Explore an xpath. This version holds
values and doesn't do any of that ugly global
nonsense"
  [xpath hrefs host signature in-host-xpath-hrefs]
  (let [sampled-uris     (sample
                          hrefs host (Math/ceil
                                      (/ (count hrefs) 4)))]
    (map
     (fn [sampled]
       (let [body             (visit-and-record-page sampled {:xpath xpath})

             xpaths-hrefs-tks (when body
                                (try
                                  (dom/minimum-maximal-xpath-set body sampled)
                                  (catch Exception e
                                    (do (error "Failed to parse: " sampled)
                                        (error "Error caused by: " xpath)))))
             
             xpaths-hrefs'    (into
                               {}
                               (map
                                (fn [[xpath links-text]]
                                  [xpath (set (map first links-text))])
                                xpaths-hrefs-tks))
             
             in-host-map      (in-host-xpaths-hrefs
                               xpaths-hrefs' (uri/host sampled))

             in-host-xpath-hs (map first in-host-map)

             pg-signature     (page/page-signature in-host-xpath-hs in-host-map)

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
                                    src-xpaths))

             original-hrefs   (in-host-xpath-hrefs xpath)

             current-hrefs    (in-host-map xpath)]
         (when xpaths-hrefs'
           (do
             (swap! *visited* utils/cons-aux sampled)
             {:url                 sampled
              :novelty             (novelty diff)
              :hrefs-table         in-host-map
              :in-host-hrefs-table in-host-map
              :update              (update original-hrefs current-hrefs)}))))
     sampled-uris)))

(defn exploration-xpaths
  [{url :url novelty :novelty hrefs-table :hrefs-table}]
  (map first hrefs-table))

(defn is-enum-candidate?
  "Currently the feature used is that at most 1 guy
needs to be above the threshold."
  [{xpath :xpath xpath-explorations :explorations} signature threshold]
  (let [similarities (filter
                      (fn [a-sim-score] (> a-sim-score threshold))
                      (map
                       #(let [xpaths      (exploration-xpaths %)
                              xpath-hrefs (-> % :hrefs-table)
                              expl-sign   (page/page-signature xpaths xpath-hrefs)]
                          (page/signature-similarity signature expl-sign))
                       xpath-explorations))]
    (< 0 (count similarities))))

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

(defn update-table
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
                  + (filter
                     identity
                     (map #(:update %) xpath-explorations)))
                 (count xpath-explorations)))})
         (catch Exception e nil)))
     explorations))))

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
                  + (filter
                     identity
                     (map #(:novelty %) xpath-explorations)))
                 (count xpath-explorations)))})
         (catch Exception e nil)))
     explorations))))

(defn enum-candidate-info
  [enum-candidate dfs hrefs novelties updates]
  {:xpath       enum-candidate
   :df          (dfs enum-candidate)
   :hrefs       (hrefs enum-candidate)
   :avg-novelty (novelties enum-candidate)
   :avg-update  (updates enum-candidate)})

(defn process-link
  "Process a link and provide all explorations in an
array"
  ([url config]
     (process-link url {} {} config))
  
  ([url *xpath-df* *xpath-hrefs* config]
   (let [body                  (visit-and-record-page url)
        
         xpaths-hrefs-text     (dom/minimum-maximal-xpath-set
                                body url)

         xpaths-hrefs          (into
                                {}
                                (map
                                 (fn [[xpath links-text]]
                                   [xpath (set (map first links-text))])
                                 xpaths-hrefs-text))

         xpaths                (map first xpaths-hrefs)

         ;; WTF?
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
                                          [an-xpath (count (xpaths-hrefs an-xpath))])
                                        xpaths))
        
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

         hrefs                 (merge-with
                                set/union ih-xp-map (href-table explorations))

         novelties             (novelty-table explorations)

         updates               (update-table explorations)]
     
     {:src-link             url
      :explorations         explorations
      :xpaths-hrefs         xpaths-hrefs
      :signature            signature
      :dfs                  dfs
      :hrefs                hrefs
      :novelties            novelties
      :updates              updates
      :in-host-xpaths-hrefs in-host-xpath-hrefs})))

(defn enum-candidates
  "Returns enumeration info"
  [{url                  :src-link
    explorations         :explorations
    xpaths-hrefs         :xpaths-hrefs
    signature            :signature
    dfs                  :dfs
    hrefs                :hrefs
    novelties            :novelties
    updates              :updates
    in-host-xpaths-hrefs :in-host-xpaths-hrefs}

   config]
  
  (let [in-host-xpaths        (map first in-host-xpaths-hrefs)
        enum-candidates       (filter
                               #(is-enum-candidate? % signature (-> config :sim-thresh))
                               explorations)
        
        enum-candidates-xs    (map #(-> % :xpath) enum-candidates)
        
        enum-candidates-info  (map
                               (fn [x]
                                 (enum-candidate-info x dfs hrefs novelties updates))
                               enum-candidates-xs)]
    
    enum-candidates-info))

(defn exploration->exploration-ds
  "An exploration is the value of :explorations in the output of process-link.
   An exploration-ds contains a flat structure for clustering and so on"
  [explorations]
  (flatten
   (map
    (fn [{xpath :xpath xpath-expls :explorations}]
      (flatten
       (map
        (fn [x]
          (let [hrefs-table (-> x :hrefs-table)
                xpaths      (map first hrefs-table)]
            {:r1             (into
                              {}
                              (page/page-signature
                               xpaths hrefs-table))
             :incoming-xpath xpath
             :url            (-> x :url)}))
        xpath-expls)))
    explorations)))

(defn cluster-member-single?
  "Single linkage clustering of explorations"
  [a-cluster x]
  (some
   (fn [a-member]
     (>= (page/signature-edit-distance-similarity
          (-> a-member :r1)
          (-> x :r1)
          0.25
          0.75)
         *page-sim-thresh*))
   a-cluster))

(defn cluster-member-maximum?
  "Cluster explorations using maximum linkage"
  [a-cluster x]
  (every?
   (fn [a-member]
     (>= (page/signature-edit-distance-similarity
          (-> a-member :r1)
          (-> x :r1)
          0.25
          0.75)
         *page-sim-thresh*))
   a-cluster))

(defn cluster-member-average?
  "CLuster explorations using average linkage"
  [a-cluster x]
  (let [sim-scores (map
                    (fn [a-member]
                      (page/signature-edit-distance-similarity
                       (-> a-member :r1)
                       (-> x :r1)
                       0.25
                       0.75)
                      *page-sim-thresh*)
                    a-cluster)]
    
    (/ (apply + sim-scores)
       (count sim-scores))))

(def cluster-member? cluster-member-single?)

(defn cluster-explorations
  "Explorations are converted to an exploration-ds and
   these are clustered together. We don't assign the
   source URL into the clusters (is this a good idea?)"
  ([an-exploration]

     (cluster-explorations
      an-exploration
      (exploration->exploration-ds (-> an-exploration
                                       :explorations))))
  
  ([{url                  :src-link
     explorations         :explorations
     xpaths-hrefs         :xpaths-hrefs
     signature            :signature
     dfs                  :dfs
     hrefs                :hrefs
     novelties            :novelties
     updates              :updates
     in-host-xpaths-hrefs :in-host-xpaths-hrefs}

    explorations-ds]
     
     (reduce
      (fn [clusters x]
        (cluster/assign
         (into [] clusters) x cluster-member?))
      []
      explorations-ds)))

(defn get-enum-xpaths
  "Process the explorations and clusters to fetch the enumeration XPath"
  [{url                  :src-link
    explorations         :explorations
    xpaths-hrefs         :xpaths-hrefs
    signature            :signature
    dfs                  :dfs
    hrefs                :hrefs
    novelties            :novelties
    updates              :updates
    in-host-xpaths-hrefs :in-host-xpaths-hrefs}

   clusters]

  (let [self-cluster          (cluster/assign-where?
                               clusters
                               {:r1 signature
                                :incoming-xpath nil
                                :url url}
                               cluster-member?)
        
        enum-candidate-xpaths (set
                               (map
                                (fn [an-exploration]
                                  (-> an-exploration
                                      :incoming-xpath))
                                (nth clusters self-cluster)))

        enum-candidate-objs   (filter
                               (fn [{xpath :xpath
                                    xpath-expls :explorations}]
                                 (some #{xpath} enum-candidate-xpaths))
                               explorations)

        enum-candidates-info  (map
                               (fn [x]
                                 (enum-candidate-info
                                  x dfs hrefs novelties updates))
                               enum-candidate-xpaths)]

    (rank/rank-enum-candidates enum-candidates-info)))

(defn cluster-info
  "Collects features about clusters"
  [a-cluster]
  {:size (count a-cluster)
   :incoming-xpaths (distinct
                     (map
                      (fn [x]
                        (-> x :incoming-xpath))
                      a-cluster))
   :urls (map
          (fn [x]
            (-> x :url))
          a-cluster)})

(defn process-explorations-cluster
  "An explorations cluster is a
   set of exploration-ds pages clustered.
   From this enumeration and content XPaths are selected
   Args:
    an-exploration, the-explorations-ds, clusters, xpaths-hrefs

   Returns:
    enum-xpath + content-xpaths if any"

  [explorations clusters]
  
  (let [total-expl   (-> clusters flatten count)
        enums-ranked (get-enum-xpaths explorations clusters)]

    {:enum-candidates-ranked enums-ranked}
    {:content-candidates-ranked nil}))
