(ns crawler.execute
  "Execute a provided model using an im-memory crawler"
  (:require [crawler.dom :as dom]
            [crawler.crawl :as crawl]
            [crawler.rich-char-extractor :as extractor]))

(defn plan-model
  [action-seqs]
  (let [total (apply + (map second action-seqs))]
   (sort-by
    (juxt
     #(-> % first count)
     #(- total (second %)))
    action-seqs)))

(defn leaf?
  [action-seq url-ds]
  (= action-seq (:src-xpath url-ds)))

(defn stop?
  [{num-leaves :num-leaves}]
  (<= 1000000 num-leaves))

(defn execute-model
  [start-url action-seqs pagination limit]
  (crawl/crawl-model start-url
                     leaf?
                     stop?
                     action-seqs
                     pagination))

(defn execute
  "A model contains keys like so:
   :action-seq (a series of actions)
   :pagination (what pagination to take for an action seq)"
  ([start-url m]
     (execute start-url m 1000))
  
  ([start-url {action-seqs :action-seq pagination :pagination} limit]
     (let [planned-model (plan-model action-seqs)]
       (execute-model start-url (first (first planned-model)) pagination limit))))
