(ns crawler.main
  "Run from here"
  (:require [clojure.tools.cli :as cli]
            [crawler.crawl :as crawl]
            [crawler.utils :as utils]
            [crawler.structure-driven :as structure-driven])
  (:use [clojure.pprint :only [pprint]]))

(def crawler-options
  [[nil "--structure-driven" "Use the structure driven crawler"]
   [nil "--start START" "Entry point for the structure driven crawler"]
   [nil "--example EXAMPLE" "Example leaf page url for the structure driven crawler"]
   [nil
    "--leaf-sim-thresh LEAF_SIMILARITY_THRESHOLD"
    "Specify the similarity threshold for the RTDM algorithm"
    :default 0.8
    :parse-fn #(Double/parseDouble %)
    :validate [#(and (< 0 %) (>= 1 %)) "Must be between 0 and 1"]]])

(defn structure-driven-crawler
  [start-url example-body leaf-sim-thresh]
  (let [structure-driven-leaf? (fn [x]
                                 (structure-driven/leaf?
                                  x example-body leaf-sim-thresh))]
    (crawl/build-sitemap start-url
                         structure-driven-leaf?
                         structure-driven/extractor
                         structure-driven/stop?)))

(defn -main
  [& args]
  (let [options (-> args
                    (cli/parse-opts crawler-options)
                    :options)]
    (cond (:structure-driven options)
          (let [stuff (structure-driven-crawler (-> options :start)
                                                (-> options
                                                    :example
                                                    utils/download-with-cookie)
                                                (-> options :leaf-sim-thresh))]
            (pprint stuff))

          :else
          (println "Pick one bruh"))))
