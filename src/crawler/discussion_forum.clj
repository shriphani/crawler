(ns crawler.discussion-forum
  "Discussion forum crawler"
  (:require [crawler.rich-char-extractor :as rc-extractor]
            [crawler.utils :as utils]
            [detect-leaf.features :as features]
            [crawler.corpus :as corpus])
  (:use [svm.core]
        [clojure.pprint :only [pprint]]))

(def path-to-classifier "gaps_between_links.train.libsvm.model")

(defn load-classifier
  []
  (read-model path-to-classifier))

(defn retrieve-leaf-paths
  "Expects a set of clusters (maintain a running list during crawl)
   Use these to produce stuff"
  [doc-clusters corpus]
  (let [largest-cluster  (last
                          (sort-by count doc-clusters))]
    (distinct
     (map
      (fn [u]
        (-> u corpus :path))
      largest-cluster))))

(defn estimate-model
  [x]
  (let [leaf-clusters  (:leaf-clusters x)
        sampled-corpus (:corpus x)

        leaf-paths     (distinct
                        (retrieve-leaf-paths leaf-clusters sampled-corpus))

        pagination-and-refinements (corpus/detect-pagination sampled-corpus)

        refined-actions (map
                         (fn [l]
                           {:actions l
                            :refined (corpus/refine-action-seq l
                                                               sampled-corpus)})
                         leaf-paths)]
    
    {:actions refined-actions
     :pagination pagination-and-refinements}))

(defn stop?
  [x num-leaves]
  (let [estimated-model (estimate-model x)]
    (pprint estimated-model)
    (pprint
     [:estimate
      (map
       (fn [l]
         (corpus/estimate-yield l
                                (:pagination estimated-model)
                                (:corpus x)))
       (:actions estimated-model))

      :expected
      (map
       (fn [l]
         (* (Math/pow 5 (count l)) 10))
       (:actions estimated-model))])
    (>= (:observed x) num-leaves)))

(defn leaf?
  [model url-ds]
  (let [anchor (:anchor-text url-ds)
        url    (:src-url url-ds)
        body   (:body url-ds)
        label  nil
        xs (features/compute-features-map {:anchor-text anchor
                                           :url url
                                           :body body
                                           :label label})]
    (pos?
     (predict model xs))))

(def extractor rc-extractor/state-action)
