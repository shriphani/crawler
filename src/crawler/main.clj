(ns crawler.main
  "Run from here"
  (:require [clojure.tools.cli :as cli]
            [crawler.crawl :as crawl]
            [crawler.utils :as utils]
            [crawler.structure-driven :as structure-driven]))

(defn crawler-options
  [[nil "--structure-driven" "Use the structure driven crawler"]
   [nil "--start" "Entry point for the structure driven crawler"]
   [nil "--example" "Example leaf page url for the structure driven crawler"]])

(defn -main
  [& args]
  (let [options (-> args
                    (cli/parse-opts crawler-options)
                    :options)]
    (cond (:structure-driven options)
          (let [start-url    (-> options :start)
                example-body (-> options
                                 :example
                                 download-with-cookie)

                structure-driven-leaf?     (fn [x]
                                             (structure-driven/leaf?
                                              x example-body))]
            (crawl/sample-sitemap-lookahead start-url
                                            structure-driven-leaf?
                                            structure-driven/extractor))

          :else
          (println "Pick one bruh"))))
