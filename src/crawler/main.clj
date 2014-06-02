(ns crawler.main
  "Run from here"
  (:require [clojure.tools.cli :as cli]
            [crawler.corpus :as corpus]
            [crawler.crawl :as crawl]
            [crawler.discussion-forum :as discussion]
            [crawler.execute :as execute]
            [clojure.java.io :as io]
            [crawler.model :as crawler-model]
            [crawler.rich-char-extractor :as rc-extractor]
            [crawler.structure-driven :as structure-driven]
            [crawler.utils :as utils]
            [me.raynes.fs :as fs])
  (:use [clojure.pprint :only [pprint]]))

(def crawler-options
  [[nil "--structure-driven" "Use the structure driven crawler"]
   [nil "--execute" "Execute a site model"]
   [nil "--execute-budget" "Execute a model with a budget"]
   [nil "--start START" "Entry point for the structure driven crawler"]
   [nil "--example EXAMPLE" "Example leaf page url for the structure driven crawler"]
   [nil "--model MODEL" "Specify a model file (a model learned by the crawler)"]
   [nil
    "--num-leaves N"
    "Specify a number of documents you want"
    :default 300
    :parse-fn #(Integer/parseInt %)]
   [nil
    "--budget B"
    "budget willing to expend"
    :default 1000
    :parse-fn #(Integer/parseInt %)]
   [nil "--refine" "Refine a model file"]
   [nil "--corpus CORPUS" "Specify a corpus"]
   [nil "--corpus-to-json CORPUS" "Convert corpus file to json file"]
   [nil "--fix-model D" "Fix a model with better :only, :avoid clauses"]
   [nil "--discussion-forum" "Execute a discussion forum crawl at entry point"]])

(defn dump-state-model-corpus
  "Creates a dated file _prefix_-yr-month-day-hr-min.corpus/state/model"
  ([state model corpus]
     (dump-state-model-corpus state model corpus "nameless-model"))

  ([state model corpus prefix]
     (let [date-file-prefix (utils/dated-filename "" "")

           create #(str prefix "/" date-file-prefix %)
           
           state-file  (create ".state")
           model-file  (create ".model")
           corpus-file (create ".corpus")]
       (do
         (utils/safe-mkdir prefix)
         (with-open [state-wrtr  (io/writer state-file)
                     model-wrtr  (io/writer model-file)
                     corpus-wrtr (io/writer corpus-file)]
           (do (pprint state state-wrtr)
               (pprint model model-wrtr)
               (pprint corpus corpus-wrtr)))))))

(defn dump-state-model-corpus-leaves
  "Creates a dated file _prefix_-yr-month-day-hr-min.corpus/state/model"
  ([state model corpus leaves]
     (dump-state-model-corpus state model corpus leaves "nameless-model"))

  ([state model corpus leaves prefix]
     (let [date-file-prefix (utils/dated-filename "" "")

           create #(str prefix "/" date-file-prefix %)
           
           state-file  (create ".state")
           model-file  (create ".model")
           corpus-file (create ".corpus")
           leaves-file (create ".leaves")]
       (do
         (utils/safe-mkdir prefix)
         (with-open [state-wrtr  (io/writer state-file)
                     model-wrtr  (io/writer model-file)
                     corpus-wrtr (io/writer corpus-file)
                     leaves-wrtr (io/writer leaves-file)]
           (do (pprint state state-wrtr)
               (pprint model model-wrtr)
               (pprint corpus corpus-wrtr)
               (pprint leaves leaves-wrtr)))))))

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

(defn discussion-forum-crawler
  [start-url num-leaves]
  (let [discussion-forum-classifier (discussion/load-classifier)
        discussion-forum-leaf? (fn [url-ds]
                                 (discussion/leaf? discussion-forum-classifier
                                                   url-ds))
        
        discussion-forum-stop? #(discussion/stop? % num-leaves)
        {state :state
         model :model
         corpus :corpus
         prefix :prefix}
        (crawl/crawl-example start-url
                             discussion-forum-leaf?
                             discussion/extractor
                             discussion-forum-stop?)]
    (dump-state-model-corpus state
                             model
                             corpus
                             (str "example-scheduler-" prefix))))

(defn discussion-forum-crawler-2
  [start-url num-leaves]
  (let [discussion-forum-classifier (discussion/load-classifier)
        discussion-forum-leaf? (fn [url-ds]
                                 (try (discussion/leaf? discussion-forum-classifier
                                                        url-ds)
                                      (catch Exception e nil)))
        
        discussion-forum-stop? #(discussion/stop? % num-leaves)
        {state :state
         model :model
         corpus :corpus
         prefix :prefix}
        (crawl/crawl-random start-url
                            discussion-forum-leaf?
                            discussion/extractor
                            discussion-forum-stop?)]
    (dump-state-model-corpus state
                             model
                             corpus
                             (str "example-scheduler-" prefix))))

(defn discussion-forum-crawler-3
  [start-url num-leaves]
  (let [discussion-forum-classifier (discussion/load-classifier)
        discussion-forum-leaf? (fn [url-ds]
                                 (try (discussion/leaf? discussion-forum-classifier
                                                        url-ds)
                                      (catch Exception e nil)))
        
        discussion-forum-stop? #(discussion/stop? % num-leaves)

        discussion-forum-build-model discussion/estimate-model
        {state :state
         model :model
         corpus :corpus
         prefix :prefix
         leaf-clusters :leaf-clusters}
        (crawl/crawl-with-estimation-example start-url
                                             discussion-forum-leaf?
                                             discussion/extractor
                                             discussion-forum-stop?
                                             discussion-forum-build-model)]
    (dump-state-model-corpus-leaves state
                                    model
                                    corpus
                                    leaf-clusters
                                    (str "example-scheduler-estimate-stopper-" prefix))))

(defn execute-model-crawler
  [start-url model-file]
  (let [model  (crawler-model/read-model model-file)
        corpus-file (crawler-model/associated-corpus-file model-file)
        crawled-corpus (corpus/read-corpus-file corpus-file)

        actions    (:actions model)
        pagination (:pagination model)

        planned-model 
        (sort-by
         (juxt #(-> % :actions count)
               #(-
                 (:yield
                  (corpus/estimate-yield %
                                         pagination
                                         crawled-corpus))))
         actions)]

    (reduce
     (fn [{blacklist :blacklist corpus :corpus} as]
       (let [{new-blacklist :blacklist
              new-corpus :corpus}
             (execute/execute-model start-url as pagination blacklist corpus)]

         {:blacklist (clojure.set/union new-blacklist
                                        blacklist)
          :corpus (merge new-blacklist
                         new-corpus)}))
     {:blacklist (set [])
      :corpus {}}
     planned-model)))

(defn execute-model-budget-crawler
  [start-url model-file budget]
  (let [model  (crawler-model/read-model model-file)
        corpus-file (crawler-model/associated-corpus-file model-file)
        crawled-corpus (corpus/read-corpus-file corpus-file)

        actions    (:actions model)
        pagination (:pagination model)

        planned-model 
        (sort-by
         (juxt #(-> % :actions count)
               #(-
                 (:yield
                  (corpus/estimate-yield %
                                         pagination
                                         crawled-corpus))))
         actions)]


    (reduce
     (fn [{budget-spent :budget-spent
          corpus-downloaded :corpus} as]
       (if (<= (- budget budget-spent)
               0)
         {:budget-spent budget-spent
          :corpus corpus-downloaded}
         (let [{corpus  :corpus
                visited :visited}
               (execute/execute-model-budget start-url
                                             as
                                             pagination
                                             (- budget budget-spent)
                                             [] ; no blacklist for now
                                             corpus-downloaded)]
           {:budget-spent (+ budget-spent (count visited))
            :corpus (merge corpus corpus-downloaded)})))
     {:budget-spent 0
      :corpus {}}
     planned-model)
    
    ;; (reduce
    ;;  (fn [{budget-expended :budget-expended
    ;;       corpus :corpus}
    ;;      as]
    ;;    (let [{corpus :corpus
    ;;           leaves-downloaded :leaves}]
    ;;     (execute/execute-model-budget start-url
    ;;                                   as
    ;;                                   pagination
    ;;                                   (- budget budget-expended)
    ;;                                   blacklist
    ;;                                   corpus))
       
    ;;    {:blacklist (clojure.set/union new-blacklist
    ;;                                   blacklist)
    ;;     :corpus (merge new-blacklist
    ;;                    new-corpus)})
    ;;  {:budget-expended 0
    ;;   :corpus {}}
    ;;  planned-model)
    ))

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

          (:corpus-to-json options)
          (corpus/corpus->json (:corpus-to-json options))

          (:discussion-forum options)
          (discussion-forum-crawler-3 (:start options)
                                      (:num-leaves options))
          
          (:execute-budget options)
          (let [model-file (-> options :model)
                model      (crawler-model/read-model model-file)

                start-url  (-> options :start)
                budget     (-> options :budget)]
            (execute-model-budget-crawler start-url
                                          model
                                          budget))
          
          ;; (:fix-model options)
          ;; (let [model-file (:fix-model options)

          ;;       associated-corpus-file (:fix-model)])
          
          ;; (:fix-model options)
          ;; (let [directory (:fix-model options)
          ;;       model-file (.getAbsolutePath
          ;;                   (first
          ;;                    (filter
          ;;                     #(re-find #"\.model$" (.getAbsolutePath %))
          ;;                     (file-seq (io/file directory)))))
          ;;       corpus-file (.getAbsolutePath
          ;;                    (first
          ;;                     (filter
          ;;                      #(re-find #"\.corpus$" (.getAbsolutePath %))
          ;;                      (file-seq (io/file directory)))))

          ;;       fixed-model
          ;;       (crawler-model/fix-model model-file
          ;;                                corpus-file)]
          ;;   (pprint fixed-model (io/writer (str (:fix-model options)
          ;;                                       "pruned-model"))))
          
          :else
          (println "Pick one bruh"))))
