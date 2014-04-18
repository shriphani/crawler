(ns crawler.structure-driven
  "An implementation of the structure driven crawler.
   Requires an entry point and a target page.
   The structure driven crawler fetches a navigation
   pattern = XPath seq"
  (:require [crawler.rich-char-extractor :as rc-extractor]
            [structural-similarity.xpath-text :as similarity]
            [crawler.utils :as utils]))

(defn stop?
  [{visited :visited} num-leaves]
  (<= num-leaves visited))

(defn leaf?
  [body example-leaf]
  (similarity/similar? body example-leaf))

(def extractor rc-extractor/state-action)
