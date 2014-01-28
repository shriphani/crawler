(ns crawler.main
  "Run from here"
  (:require [clojure.tools.cli :as cli]
            [crawler.crawl :as crawl]
            [crawler.utils :as utils]
            [crawler.structure-driven :as structure-driven]))

(def crawler-options
  [[nil "--structure-driven" "Use the structure driven crawler"]
   [nil "--start START" "Entry point for the structure driven crawler"]
   [nil "--example EXAMPLE" "Example leaf page url for the structure driven crawler"]])

(defn structure-driven-crawler
  [start-url example-body]
  (let [structure-driven-leaf? (fn [x]
                                 (structure-driven/leaf?
                                  x example-body))]
    (crawl/sample-sitemap-lookahead start-url
                                    structure-driven-leaf?
                                    structure-driven/extractor
                                    structure-driven/stop?)))

(defn -main
  [& args]
  (let [options (-> args
                    (cli/parse-opts crawler-options)
                    :options)]
    (cond (:structure-driven options)
          (structure-driven-crawler (-> options :start)
                                    (-> options
                                        :example
                                        utils/download-with-cookie))

          :else
          (println "Pick one bruh"))))
