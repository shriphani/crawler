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

(defn leaf?
  [action-seq url-ds]
  (= (reverse action-seq) (:src-xpath url-ds)))

(defn stop?
  [{num-leaves :num-leaves}]
  (<= 1000000 num-leaves))

(defn execute-model
  [start-url action-seqs pagination blacklist limit]
  (let [corpus (crawl/crawl-model start-url
                                  leaf?
                                  stop?
                                  action-seqs
                                  pagination
                                  blacklist)
        wrtr (io/writer (str (uri/host start-url)
                             ".corpus")
                        :append true)]
    (do (clojure.pprint/pprint (:corpus corpus) wrtr)
        {:num-leaves (:num-leaves corpus)
         :visited (:visited corpus)})))

(defn execute
  "A model contains keys like so:
   :action-seq (a series of actions)
   :pagination (what pagination to take for an action seq)"
  ([start-url m]
     (execute start-url m 1000))
  
  ([start-url {action-seqs :action-seq pagination :pagination} limit]
     (let [planned-model (plan-model action-seqs)]
       (reduce
        (fn [acc [a-seq estimate]]
          (do
            (utils/sayln :executing a-seq)
            (let [{num-leaves :num-leaves
                   visited :visited}
                  (execute-model start-url
                                 (reverse a-seq)
                                 pagination
                                 (:visited acc)
                                 limit)]
              {:num-leaves (+ (:num-leaves acc) num-leaves)
               :visited (clojure.set/union visited (:visited acc))})))
        {:num-leaves 0
         :visited []}
        planned-model))))
