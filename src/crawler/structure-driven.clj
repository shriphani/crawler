(ns crawler.structure-driven
  (:require [crawler.rich-char-extractor :as rc-extractor]
            [crawler.similarity :as similarity]))

(defn leaf?
  [body example-leaf]
  (let [sim (similarity/tree-edit-distance-html
             body
             example-leaf)]
    (<= 0.8 sim)))

(def extractor rc-extractor/state-action)