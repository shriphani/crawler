(ns crawler.discussion-forum
  "Discussion forum crawler"
  (:require [crawler.rich-char-extractor :as rc-extractor]
            [crawler.utils :as utils]
            [detect-leaf.features :as features])
  (:use [svm.core]))

(def path-to-classifier "gaps_between_links.train.libsvm.model")

(defn load-classifier
  []
  (read-model path-to-classifier))

(defn stop?
  [{visited :visited} num-leaves]
  (<= num-leaves visited))

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
