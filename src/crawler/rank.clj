;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.rank
  "Ranking utils"
  (:require [crawler.utils :as utils]))

(def *href-prior* 10)

(defn enum-candidate-score
  [{xpath :xpath df :df hrefs :hrefs avg-novelty :avg-novelty avg-update :avg-update}]
  (* (/ (Math/log (+ (count hrefs)
                     *href-prior*))
        df)
     avg-novelty
     avg-update))

(defn enum-candidate-score-no-df
  [{xpath :xpath df :df hrefs :hrefs avg-novelty :avg-novelty}]
  (* (Math/log (+ (count hrefs)
                  *href-prior*))
     avg-novelty))

(defn rank-enum-candidates
  "Enumeration candidates are xpaths with the following
info:
1. average tf
2. df
3. total number of unique hrefs"
  [enum-candidates-info]
  (->> enum-candidates-info
       (sort-by enum-candidate-score)
       reverse))

(defn rank-enum-candidates-no-df
  "Enumeration candidates are xpaths with the following
info:
1. average tf
2. df
3. total number of unique hrefs"
  [enum-candidates-info]
  (->> enum-candidates-info
       (sort-by enum-candidate-score-no-df)
       reverse))

(defn rank-cluster-url-entry-point
  "Counts the number of unique URLs that lead
   to this cluster and how unique the URLs are"
  [a-cluster]
  (let [entry-urls (distinct
                    (map #(-> % :url) a-cluster))]
    (count entry-urls)))


(defn rank-content-xpaths
  [{means :means
    variances :variances
    counts :counts}]
  (let [xpath-means (into {} means)
        xpath-vars  (into {} variances)
        xpath-counts (into {} counts)
        xpaths      (map first means)]

    (reverse
     (sort-by
      second
      (map
       vector
       xpaths
       (map
        #(* (xpath-means %)
            (xpath-vars %)
            (xpath-counts %))
        xpaths))))))

(defn rank-by-uniqueness
  "A simple ranker that ranks by exploration function"
  [xpaths-hrefs-text]
  (let [xpaths-tokens    (map
                          (fn [[xpath nodes-and-info]]
                            {:xpath  xpath
                             :tokens (map
                                      (fn [a-node]
                                        {:path-tokens (-> a-node :href utils/tokenize-url)
                                         :text-tokens  (-> a-node :text utils/tokenize)})
                                      nodes-and-info)})
                          xpaths-hrefs-text)
        
        xpath-mean       (into
                          {}
                          (map
                           (fn [{xpath :xpath tokensets :tokens}]
                             {xpath
                              (/ (reduce
                                  +
                                  (concat
                                   (map
                                    #(-> % :text-tokens set count) tokensets)
                                   (map
                                    #(-> % :path-tokens set count) tokensets)))
                                 (count tokensets))})
                           xpaths-tokens))

        xpath-vars       (into
                          {}
                          (map
                           (fn [{xpath :xpath tokensets :tokens}]
                             {xpath
                              (/ 
                               (reduce
                                +
                                (concat
                                 (map
                                  #(Math/pow
                                    (- (-> % :text-tokens set count)
                                       (xpath-mean xpath)) 2) tokensets)
                                 (map
                                  #(Math/pow
                                    (- (-> % :path-tokens set count)
                                       (xpath-mean xpath)) 2) tokensets)))
                               (count tokensets))})
                           xpaths-tokens))

        xpath-score      (into
                          {}
                          (map
                           (fn [[xpath mean]]
                             [xpath (* mean (xpath-vars xpath))])
                           xpath-mean))]
    (reverse (sort-by second xpath-score))))
