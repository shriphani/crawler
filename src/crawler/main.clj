(ns crawler.main
  "Run from here"
  (:require [clojure.tools.cli :as cli]
            [crawler.crawl :as crawl]
            [clojure.java.io :as io]
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
   [nil "--model MODEL" "Specify a model file (a model learned by the crawler)"]
   [nil
    "--num-leaves N"
    "Specify a number of documents you want"
    :default 500
    :parse-fn #(Integer/parseInt %)]
   [nil "--refine" "Refine a model file"]
   [nil "--corpus CORPUS" "Specify a corpus"]])

(defn dump-state-model-corpus
  "Creates a dated file _prefix_-yr-month-day-hr-min.corpus/state/model"
  ([state model corpus]
     (dump-state-model-corpus state model corpus "crawler"))

  ([state model corpus prefix]
     (let [date-file-prefix (utils/dated-filename prefix "")

           create #(str date-file-prefix %)
           
           state-file  (create ".state")
           model-file  (create ".model")
           corpus-file (create ".corpus")]
       (with-open [state-wrtr  (io/writer state-file)
                   model-wrtr  (io/writer model-file)
                   corpus-wrtr (io/writer corpus-file)]
         (do (pprint state state-wrtr)
             (pprint model model-wrtr)
             (pprint corpus corpus-wrtr))))))

(defn dump-corpus
  ([corpus]
     (dump-corpus corpus "crawler"))

  ([corpus prefix]
     (let [date-file-prefix (utils/dated-filename prefix "")
           create #(str date-file-prefix %)
           model-file  (create date-file-prefix ".corpus")]
       (with-open [corpus-wrtr (io/writer)]
         (pprint corpus corpus-wrtr)))))

(defn structure-driven-crawler
  [start-url example-body num-leaves]
  (let [structure-driven-leaf? #(structure-driven/leaf? example-body %)
        structure-driven-stop? #(structure-driven/stop? % num-leaves)
        {state :state model :model corpus :corpus prefix :prefix}
        (crawl/crawl start-url
                     structure-driven-leaf?
                     structure-driven/extractor
                     structure-driven-stop?)]
    (dump-state-model-corpus state
                             model
                             corpus
                             prefix)))

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
    (let [{state :state model :model corpus :corpus prefix :prefix}
          (crawl/crawl start-url
                       leaf-fn
                       extract-fn
                       stop-fn)]
      (dump-corpus corpus))))

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
                                                (-> options :num-leaves))]
            (pprint stuff))

          (:execute options)
          (let [model-file (-> options :model)
                model      (crawler-model/read-model model-file)

                start-url  (-> options :start)
                num-leaves (-> options :num-docs)]
            (execute-model-crawler start-url
                                   model
                                   num-leaves))

          (:refine options)
          (let [model-file (:model options)
                corpus     (:corpus options)]
            (crawler-model/plan-dump-model model-file corpus))
          
          :else
          (println "Pick one bruh"))))
