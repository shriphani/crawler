(ns crawler.rich-extractor
  "Implementation of the rich URL extractor"
  (:require [crawler.dom :as dom]
            [crawler.rank :as rank]
            [org.bovinegenius [exploding-fish :as uri]]))

;;;; A rich extractor performs an initial score assignment based
;;;; on how expressive a URL is. A decision to follow is based upon
;;;; some statistics.

(defn state-action
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
    in-host-xhrefs))

(defn follow-naive?
  "Decide if an Xpath's links are worth following based on a naive score of richness.
   The score passed in currently is a trivial product of mean # of tokens and variance
   of the # of tokens. NO LOOKAHEADS ARE SUPPLIED TO THIS CRAWLER.

   Args:
    state: A set of actions. {Xpath, url}
    action: An xpath
    details-map: {xpath -> information}"
  [state action details-map]
  (< 0 (details-map action)))
