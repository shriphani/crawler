(ns crawler.structure-driven
  "An implementation of the structure driven crawler.
   Requires an entry point and a target page.
   The structure driven crawler fetches a navigation
   pattern = XPath seq"
  (:require [crawler.rich-char-extractor :as rc-extractor]
            [crawler.similarity :as similarity]))

(defn stop?
  [{visited :visited}]
  (<= 100 visited))

(defn leaf?
  ([body example-leaf]
     (leaf? body example-leaf 0.8))
  
  ([body example-leaf thresh]
     (let [sim (similarity/tree-edit-distance-html
                body
                example-leaf)]
       (do
         (println :similarity sim)
         (<= thresh sim)))))

(def extractor rc-extractor/state-action)
