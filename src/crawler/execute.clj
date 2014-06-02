(ns crawler.execute
  "Execute a provided model using an im-memory crawler"
  (:require [crawler.dom :as dom]
            [crawler.crawl :as crawl]
            [crawler.utils :as utils]
            [clojure.java.io :as io]
            [crawler.rich-char-extractor :as extractor]
            (org.bovinegenius [exploding-fish :as uri])))

(defn plan-model
  [action-seqs]
  (let [total (apply + (map second action-seqs))]
   (sort-by
    (juxt
     #(-> % first count)
     #(- total (second %)))
    action-seqs)))

(defn generate-leaf?
  [action-seq]
  (fn [url-ds]
    (= action-seq
       (:src-xpath url-ds))))

(defn stop?
  [{n :queue-size}]
  (zero? n))

(defn execute-model
  ([start-url action-seq pagination]
     (execute-model start-url
                    action-seq
                    pagination
                    (set [])
                    {}))
  
  ([start-url action-seq pagination blacklist old-corpus]
     (utils/sayln :executing-model action-seq)
     (let [paging-actions (:paging-actions pagination)
           paging-refined (:refine pagination)

           leaf? (generate-leaf? (:actions action-seq))

           {visited :visited
            num-threads :num-leaves
            corpus :corpus}
           (crawl/crawl-model start-url
                              leaf?
                              stop?
                              action-seq
                              pagination
                              blacklist
                              old-corpus)]
       (do (utils/sayln :downloaded-discussions-count num-threads)
           {:blacklist visited
            :corpus corpus}))))

(defn budget-stop?
  [budget]
  (fn [{n :queue-size
       v :visited}]
    (utils/sayln "Deciding to stop")
    (utils/sayln "Budget is:" budget)
    (utils/sayln "Visited: " v)
    (or (zero? n)
        (>= v budget))))

(defn execute-model-budget
  "Experiment to check how good the crawler
   is at fetching threads even with a super-limited
   budget"
  ([start-url action-seq pagination budget]
     (execute-model-budget start-url
                           action-seq
                           pagination
                           (set [])
                           {}))

  ([start-url action-seq pagination budget seen corpus]
     (utils/sayln :executing-model-budget action-seq)
     (let [leaf? (generate-leaf? (:actions action-seq))
           
           {visited :visited
            num-threads :num-leaves
            corpus :corpus}
           (crawl/crawl-model start-url
                              leaf?
                              (budget-stop? budget)
                              action-seq
                              pagination
                              seen
                              {})]
       {:visited visited
        :corpus corpus})))
