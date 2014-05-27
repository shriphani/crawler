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
  [start-url action-seq pagination]
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
                           pagination)]
    (utils/sayln :downloaded-discussions-count num-threads)))
