(ns crawler.execute
  "Execute a provided model using an im-memory crawler"
  (:require [crawler.dom :as dom]))

(defn plan-model
  [action-seqs]
  (let [total (apply + (map second action-seqs))]
   (sort-by
    (juxt
     #(-> % first count)
     #(- total (second %)))
    action-seqs)))

(defn execute
  "A model contains keys like so:
   :action-seq (a series of actions)
   :pagination (what pagination to take for an action seq)"
  [start-url {action-seqs :action-seq pagination :pagination}]
  ())
