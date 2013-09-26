;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.test-system
  (:gen-class :main true)
  (:require [clojure.tools.cli :as cli]
            [crawler.dom :as dom]))

(defn -main
  [& args]
  (println
   "<html>
       <body>
        Positive Examples:
        Date-detection success threshold: 0.7
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

          xpath (try (-> (slurp f)
                         dom/page-model
                         :records-xpath)
                     (catch Exception e nil))

          fail  (if
                    (and
                     xpath
                     (re-find #"clueweb" xpath))
                  "Success"
                  "Failed")]
      (println
       (format
        "
        <li>
         <p><a href=\"%s\">%s</a></p>
         <p>XPath: %s</p>
         <p>Records Discovered? %s</p>
        </li>
        " link (.getName f) xpath fail))))
  (println
   "</ul>
      </body>
      </html>"))
