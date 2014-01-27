(ns crawler.structure-driven
  "An implementation of the structure driven crawler.
   Requires an entry point and a target page.
   The structure driven crawler fetches a navigation
   pattern = XPath seq"
  (:require [crawler.rich-char-extractor :as rc-extractor]
            [crawler.similarity :as similarity]))

(defn stop?
  [{visited :visited}]
  (<= 200 visited))

(defn leaf?
  [body example-leaf]
  (let [sim (similarity/tree-edit-distance-html
             body
             example-leaf)]
    (do
      (println :similarity sim)
      (<= 0.8 sim))))

(def extractor rc-extractor/state-action)
