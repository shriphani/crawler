(ns crawler.rich-extractor
  "Implementation of the rich URL extractor"
  (:require [crawler.dom :as dom]
            [crawler.rank :as rank]
            [crawler.similarity :as sim]
            [crawler.utils :as utils]
            [clj-http.client :as client]
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
  "Args:
    in-host-xhrefs: {xpath -> url/text}"
  [in-host-xhrefs]
  (into
   {} (map
       (fn [[xpath nodes]]
         [xpath (tokenize-anchor-url nodes)])
       in-host-xhrefs)))

(defn score-actions
  [xpaths-tokens]
  (rank/score-xpaths-1 xpaths-tokens))

(defn get-and-wait
  [a-link]
  (do
    (Thread/sleep 2000)
    (client/get a-link)))

(defn sample-item
  "Samples a set of links and decides if it is a
   potential pagination clause"
  [xpath links body]
  (let [pages (map
               #(fn [a-link]
                  (let [body (-> % get-and-wait)]
                   {:link a-link
                    :body body
                    :decisions (state-action body a-link)}))
               links)
        same  (reduce
               (fn [acc page]
                 (if (sim/tree-edit-distance-html body page)
                   (inc acc) acc))
               0
               pages)]
    {:same-signature-count same
     :items pages}))

(defn explore-pagination
  "Pagination is detected using:
    -> pages that have similar structure
    -> exploration is in direction based on inverse of score"
  [page-src url blacklist]
  (let [in-host-xpaths-hrefs (state-action page-src url blacklist)
        xpaths-hrefs         (into
                              {} (map
                                  (fn [[xpath nodes]]
                                    [xpath (map
                                            #(-> % :href)
                                            nodes)])
                                  in-host-xpaths-hrefs))
        xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)
        xpaths-scored        (score-actions xpaths-tokenized)
        candidates-ordered   (map first (sort-by second xpaths-scored))
        to-sample-links      (map
                              vector
                              candidates-ordered
                              (map
                               (fn [xpath]
                                 (take 20 (distinct
                                           (filter
                                            (fn [x]
                                              (not (some #{x} (set blacklist))))
                                            (xpaths-hrefs xpath)))))
                               candidates-ordered))]
    to-sample-links))

(defn score-xpaths
  [page-src url blacklist]
  (let [in-host-xpaths-hrefs (state-action page-src
                                           url
                                           blacklist)
        xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)]
    (score-actions xpaths-tokenized)))

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
           decision             (first
                                 (first
                                  (reverse
                                   (sort-by second xpaths-scored))))]
       (println :url url)
       (println :decision decision)
       {:links (distinct (map #(-> % :href) (in-host-xpaths-hrefs decision)))
        :action-score (xpaths-scored decision)})))

(defn extract-above-average-richest
  ([page-src url]
     (extract-above-average-richest page-src url (set [])))

  ([page-src url blacklist]
     (let [in-host-xpaths-hrefs (state-action page-src url blacklist)
           xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)
           xpaths-scored        (score-actions xpaths-tokenized)
           mean-richness        (/ (apply + (map second xpaths-scored))
                                   (count xpaths-scored))
           decision             (filter
                                 (fn [[xpath score]]
                                   (> score mean-richness))
                                 (reverse
                                  (sort-by second xpaths-scored)))
           decision-links       (flatten
                                 (map
                                  (fn [[a-decision score]]
                                    (map
                                     #(-> % :href)
                                     (in-host-xpaths-hrefs a-decision)))
                                  decision))

           decision-scores      (/ (reduce
                                    (fn [acc [a-decision score]]
                                      (+ acc score))
                                    0
                                    decision)
                                   (count decision))]
       {:links decision-links
        :action-score decision-scores
        :xpaths (map first decisions)})))
