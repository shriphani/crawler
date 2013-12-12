(ns crawler.rich-extractor
  "Implementation of the rich URL extractor"
  (:require [crawler.dom :as dom]
            [crawler.rank :as rank]
            [org.bovinegenius [exploding-fish :as uri]]))

;;;; A rich extractor performs an initial score assignment based
;;;; on how expressive a URL is.

(defn extract
  [page-src url]
  (let [processed-pg      (dom/html->xml-doc page-src)
        xpaths-hrefs-text (dom/xpaths-hrefs-tokens processed-pg url)
        host              (uri/host url)
        in-host-xhrefs    (into
                           {}
                           (filter
                            #(-> % second empty? not)
                            (map
                             (fn [[xpath nodes]]
                               [xpath (filter
                                       (fn [a-node]
                                         (or (= host (uri/host (:href a-node)))
                                             (nil? (uri/host (:href a-node)))))
                                       nodes)])
                             xpaths-hrefs-text)))]
    (rank/rank-by-uniqueness in-host-xhrefs)))
