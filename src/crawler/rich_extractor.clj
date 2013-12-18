(ns crawler.rich-extractor
  "Implementation of the rich URL extractor"
  (:require [crawler.dom :as dom]
            [crawler.rank :as rank]
            [crawler.utils :as utils]
            [clojure.set :as clj-set]
            [org.bovinegenius [exploding-fish :as uri]]))

;;;; A rich extractor performs an initial score assignment based
;;;; on how expressive a URL is. A decision to follow is based upon
;;;; some statistics.

(defn tokenize-anchor-url
  [nodes]
  (map
   (fn [{_ :node href :href text :text}]
     (let [url-tokens  (set (utils/tokenize-url href))
           text-tokens (set (utils/tokenize text))]
       {:url-tokens url-tokens
        :text-tokens text-tokens}))
   nodes))

(defn state-action
  "Args:
    page-src: the page body
    url: the source link
    blacklist: links that are already visited or addressed."
  ([page-src url]
     (state-action page-src url []))
  
  ([page-src url blacklist]
     (let [processed-pg      (dom/html->xml-doc page-src)
           xpaths-hrefs-text (dom/xpaths-hrefs-tokens processed-pg url blacklist)
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
       in-host-xhrefs)))

(defn tokenize-actions
  [in-host-xhrefs]
  (into
   {} (map
       (fn [[xpath nodes]]
         [xpath (tokenize-anchor-url nodes)])
       in-host-xhrefs)))

(defn extract-richest
  "Retrieve from possible actions, the xpath with the highest score
   Args:
    page-src: the page source
    url: the page's url
    blacklist: what decision is made"
  ([page-src url]
     (extract-richest page-src url (set [])))

  ([page-src url blacklist]
     (let [in-host-xpaths-hrefs (state-action page-src url blacklist)
           xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)
           xpaths-scored        (score-actions xpaths-tokenized)
           decision             (first (first (reverse (sort-by second xpaths-scored))))]
       (println :url url)
       (println :decision decision)
       {:links (distinct (map #(-> % :href) (in-host-xpaths-hrefs decision)))
        :action-score (xpaths-scored decision)})))
