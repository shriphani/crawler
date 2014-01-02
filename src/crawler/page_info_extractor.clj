(ns crawler.page-info-extractor
  "Extract links based on navigational info statistics"
  (:require [crawler.dom :as dom]
            [crawler.utils :as utils]
            [crawler.rich-extractor :as rich-extractor])
  (:use [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]]))

(def *leaf-nav-info-fraction* 0.5)

(defn stats
  "Produce navigational info statistics.
   If an xpath is not supplied, we produce global nav stats"
  ([body]
     (stats body ".//a")) ; this measures out of domain potential as
                          ; well. so it is not really accurate. The
                          ; supplied extractors have their own way of
                          ; computing this global statistic
  
  ([body an-xpath]
   (let [processed-document (dom/html->xml-doc body)
         
         pg-text  (-> ".//html"
                      ($x:text+ processed-document)
                      first)
         nav-text ($x:text+ an-xpath processed-document)

         pg-toks  (utils/tokenize pg-text)
         nav-toks (->> nav-text
                       (map utils/tokenize)
                       (apply concat))

         nav-information-ratio (/ (count nav-toks)
                                  (count pg-toks))]
     {:global-text-toks pg-toks
      :nav-info-ratio nav-information-ratio})))

(defn decision-space
  [body url-ds blacklist]
  (let [url (-> url-ds :url)

        in-host-xpaths-hrefs (rich-extractor/state-action body url blacklist)
        xpaths-tokenized     (rich-extractor/tokenize-actions in-host-xpaths-hrefs)

        xpaths-text-tokens   (into
                              {} (map
                                  (fn [[an-xpath ts]]
                                    [an-xpath
                                     (reduce
                                      concat
                                      (map
                                       (fn [token-info]
                                         (-> token-info :text-tokens))
                                       ts))])
                                  xpaths-tokenized))

        global-nav-stats     (stats body)

        global-text-tokens   (:global-text-toks global-nav-stats)
        xpaths-info-stats    (map
                              (fn [[an-xpath tokens]]
                                [an-xpath (/ (count tokens)
                                             (count global-text-tokens))])
                              xpaths-text-tokens)]
    {:ranked       (reverse (sort-by second xpaths-info-stats))
     :nav-fraction (-> global-nav-stats :nav-info-ratio)
     :global-toks  (-> global-nav-stats :global-text-toks)}))

(defn leaf? [info-for-nav]
  (> *leaf-nav-info-fraction* info-for-nav))
