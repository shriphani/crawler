(ns crawler.main
  "Run from here"
  (:require [clojure.tools.cli :as cli]
            [crawler.crawl :as crawl]
            [crawler.model :as crawler-model]
            [crawler.rich-char-extractor :as rc-extractor]
            [crawler.structure-driven :as structure-driven]
            [crawler.utils :as utils])
  (:use [clojure.pprint :only [pprint]]))

(def crawler-options
  [[nil "--structure-driven" "Use the structure driven crawler"]
   [nil "--execute" "Execute a site model"]
   [nil "--start START" "Entry point for the structure driven crawler"]
   [nil "--example EXAMPLE" "Example leaf page url for the structure driven crawler"]
   [nil
    "--leaf-sim-thresh LEAF_SIMILARITY_THRESHOLD"
    "Specify the similarity threshold for the RTDM algorithm"
    :default 0.8
    :parse-fn #(Double/parseDouble %)
    :validate [#(and (< 0 %) (>= 1 %)) "Must be between 0 and 1"]]

   [nil "--model MODEL" "Specify a model file (a model learned by the crawler)"]
   [nil
    "--num-docs N"
    "Specify a number of documents you want"
    :default 500
    :parse-fn #(Integer/parseInt %)]])

(defn structure-driven-crawler
  [start-url example-body leaf-sim-thresh]
  (let [structure-driven-leaf? (fn [x]
                                 (structure-driven/leaf?
                                  x example-body leaf-sim-thresh))]
    (crawl/crawl start-url
                 structure-driven-leaf?
                 structure-driven/extractor
                 structure-driven/stop?)))

(defn execute-model-crawler
  [start-url model num-leaves]
  (let [stop-fn    (fn [{visited :visited}]
                     (<= num-leaves visited))

        leaf-fn    (fn [x] false) ;; FIX

        model      (crawler-model/read-model model)
        
        extract-fn (fn [page-src url-ds template-removed blacklist]
                     (rc-extractor/state-action-model (first model)
                                                      page-src
                                                      url-ds
                                                      template-removed
                                                      blacklist))]
    (crawl/crawl start-url
                 leaf-fn
                 extract-fn
                 stop-fn)))

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

          (:execute options)
          (let [model-file (-> options :model)
                model      (crawler-model/read-model model-file)

                start-url  (-> options :start)
                num-leaves (-> options :num-docs)]
            (execute-model-crawler start-url
                                   model
                                   num-leaves))
          
          :else
          (println "Pick one bruh"))))
