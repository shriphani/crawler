(ns crawler.rich-extractor
  "Implementation of the rich URL extractor"
  (:require [crawler.dom :as dom]
            [crawler.rank :as rank]
            [org.bovinegenius [exploding-fish :as uri]]))

;;;; A rich extractor performs an initial score assignment based
;;;; on how expressive a URL is. A decision to follow is based upon
;;;; some statistics.

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
                             xpaths-hrefs-text)))

        ranked-xpaths     (rank/rank-by-uniqueness in-host-xhrefs)]
    (map
     (fn [[xpath score]]
       {:xpath xpath
        :node (map (fn [a-node]
                     {:href (-> a-node :href)
                      :text (-> a-node :text)})
                   (in-host-xhrefs xpath))
        :score score})
     ranked-xpaths)))

(defn follow-naive?
  "Decide if an Xpath's links are worth following based on a naive score of richness.
   The score passed in currently is a trivial product of mean # of tokens and variance
   of the # of tokens

   Args:
    state: A set of actions. {Xpath, url}
    action: An xpath
    details-map: {xpath -> information}"
  [state action details-map]
  (< 0 (details-map action)))
