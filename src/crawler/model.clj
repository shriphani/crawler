(ns crawler.model
  "Contains code to read and process a
   sampled model"
  (:require [clojure.java.io :as io]
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

(defn plan
  "Plan of execution for the model"
  [a-model]
  (let [total (apply + (map second a-model))]
    (sort-by (juxt
              #(-> % first count)
              #(- total (second %))) a-model)))

(defn cluster-actions
  [infos]
  (let [state-actions (map (fn [x] [(first x) (-> x second :state-action)]) infos)
        action-sets   (map
                       (fn [[u s]]
                         [u (set (map :xpath s))])
                       (map (fn [[u x]] [u (-> x :xpath-nav-info)]) state-actions))]
    (reduce
     (fn [acc [u x]]
       (let [assign (.indexOf
                     (map
                      (fn [acc-x]
                        (or (= (clojure.set/intersection acc-x x)
                               acc-x)
                            (= (clojure.set/intersection acc-x x)
                               x)))
                      (map :action acc))
                     true)]
         (if (<= 0 assign)
           (assoc acc assign {:action (:action (nth acc assign))
                              :urls   (conj (:urls (nth acc assign)) u)})
           (conj acc {:action x :urls [u]}))))
     []
     action-sets)))

(defn refine-xpath
  [x urls infos url-clusters]
  (let [num-clusters (count url-clusters)
        proc-bodies  (map dom/html->xml-doc (map :body infos))]
    (map
     (fn [[u b]]
       [x
        (dom/refine-xpath-with-position b u x num-clusters)])
     (map vector urls proc-bodies))))

(defn cluster-results
  "Do we need to add position info
   to refine a particular model"
  [a-model corpus]
  (let [grouped (group-by
                 (fn [[url s]] (:src-xpath s))
                 corpus)

        cluster-cands (filter
                       (fn [[p n]]
                         (< 1 n))
                       (map
                        (fn [[p xs]]
                          (let [clustered (cluster-actions xs)]
                            [p
                             (count clustered)
                             (map :urls clustered)]))
                        grouped))]
    (map
     (fn [[x n us]]
       (let [items
             (map
              (fn [[p xs]]
                (let [action (first p)
                      urls   (distinct
                              (map
                               (fn [[url info]]
                                 (:src-url info))
                               xs))
                      url-infos (map
                                 #(corpus %)
                                 urls)]
                  (refine-xpath action urls url-infos us)))
              (filter
               #(= (first %) x)
               grouped))]
         items))
     cluster-cands)))

(defn refine-same-action-model
  "Discover paths that exhibit pagination
   or filter like characteristics

   2 consecutive same actions for instance
   are a pagination or a filter candidate"
  [a-model a-corpus]
  (map
   (fn [[action-seq n]]
     (let [reversed (reverse action-seq)
           consecs  (map vector reversed (rest reversed))
           posn     (+ 1 (.indexOf
                          (map (fn [[x y]] (= x y)) consecs)
                          true))]
       posn))

   ; retrieve only repetitions
   (filter
    (fn [[action-seq n]]
      (let [reversed (reverse action-seq)
            consecs  (map vector reversed (rest reversed))]
        (some (fn [[x y]] (= x y)) consecs)))
    a-model)))

