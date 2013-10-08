;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.test-system
  (:gen-class :main true)
  (:require [clojure.tools.cli :as cli]
            [crawler.dom :as dom]
            [crawler.records :as records])
  (:use     [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]]))

(defn rank-of-clueweb-string
  [strs]
  (.indexOf
   (map
    #(not= nil (re-find #"clueweb" %)) strs)
   true))

(defn test-xpath
  []
  (println
   "<html>
       <body>
        Positive Examples:
        Best record is 1.5 x away from the mean
        <ul>")
  (doseq [f (rest
            (file-seq
             (clojure.java.io/file "resources/date-indexed-data/positive")))]
    (let [link
          (->> f
                (.getName)
                (list
                 "https://rawgithub.com/shriphani/crawler/master/resources/date-indexed-data/positive/")
                (clojure.string/join ""))

          results (-> (slurp f)
                      records/record-signatures)
          
          outlier-xpaths (-> results
                             first
                             :outlier-xpaths)
          rank (+ (rank-of-clueweb-string (rest results))
                  (count (seq outlier-xpaths)))
          fail  (if (or (some
                         #(re-find #"clueweb" %)
                         (seq outlier-xpaths))
                        (zero? rank))
                  "Success!"
                  (format "Failed. Correct value @ rank: %d" rank))]
      (println
       (format
        "
        <li>
         <p><a href=\"%s\">%s</a></p>
         <p>Records Discovered? %s</p>
        </li>
        " link (.getName f) fail))))
  (println
   "</ul>
      </body>
      </html>"))

(defn -main
  [& args]
  (test-xpath))
