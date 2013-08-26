;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu

(ns crawler.gplus.core
  (:require [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [crawler.core :as core]
            [crawler.misc :as misc]
            [crawler.utils :as utils]))

(def log-file "gplus-download.log")

(defn handle-gplus-profile
  [profile-page-response link]
  (if (:body profile-page-response)
    (clojure.pprint/pprint (:body profile-page-response))
    (binding [*out* (io/writer log-file :append true)]
      (println (str "FAILED: " link))))
  (flush))

(defn -main
  [& args]
  (let [[optional [gplus-profile-link-list outfile] banner]
        (cli/cli args
                 ["-c" "--compress" :flag true])
        
        work
        (fn [] (doseq [link (misc/file->lines gplus-profile-link-list)]
                (handle-gplus-profile (utils/download-page
                                       link
                                       core/lemurproject-header)
                                      link)
                (. Thread sleep 3000)))]
    (if (:compress optional)
      (with-open [outfile-writer (misc/gzip-writer outfile)]
        (binding [*out* outfile-writer]
          (work)))
      (with-open [outfile-writer (io/writer outfile)]
        (binding [*out* outfile-writer]
          (work))))))
