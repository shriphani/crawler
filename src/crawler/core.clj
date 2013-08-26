;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu

(ns crawler.core
  (:require [clj-robots.core :as robots]
            [clojure.tools.cli :as cli]
            [org.bovinegenius [exploding-fish :as uri]]

            [crawler.page-utils :as page-utils]
            [crawler.utils :as utils])
  (:use [clojure.pprint]))

(def lemurproject-ua-string
  (str
   "Mozilla/5.0 (compatible; mandalay admin@lemurproject.org;"
   " "
   "+http://boston.lti.cs.cmu.edu/crawler/clueweb12pp/"))

(def lemurproject-header
  {"User-Agent" lemurproject-ua-string})

(defn handle-response
  "Response is a value returned by our wrapper around
clj-http's get call. We have a couple of statuses that we use"
  [response doc-writer]
  (when (not (= :download-failed (:status response)))
    (do
      (->> (:body response)
         (.write doc-writer))
      (page-utils/get-links-out (:body response)))))

(defn write-response
  [response doc-writer link]
  (binding [*out* doc-writer]
    (pprint {:link link
             :body (-> response :body)})
    (flush)))

(defn random-sample
  "Randomly sample a website. This is done by
dequeuing from the front or the end (picked at random)
of the to-visit queue.
Args:
 site       : the seed in question
 to-visit   : queue of unvisited URLs
 num-docs   : number of documents to obtain
 doc-writer : a file-descriptor of your choice" 
  [to-visit num-docs visited doc-writer]
  (let [[next-link remaining] (utils/random-dequeue to-visit)

        next-link-response    (utils/download-page
                               next-link
                               {"User-Agent"
                                lemurproject-ua-string})

        out-links             (filter
                               #(not (some #{%} visited))
                               (map
                                #(if (utils/relative? %)
                                   (uri/resolve-uri next-link %)
                                   %)
                                (filter
                                 #(utils/in-domain? next-link %)
                                 (page-utils/get-links-out
                                  (:body next-link-response)))))]
    (do
      (write-response next-link-response doc-writer next-link)
      (when (not (= 1 num-docs))
        (recur
         (concat remaining out-links)
         (dec num-docs)
         (cons next-link visited)
         doc-writer)))))

(defn -main
  [& args]
  (let [[optional [site num-docs-str output-file] banner] (cli/cli args)]
    (random-sample
     [site]
     (java.lang.Integer/parseInt num-docs-str)
     []
     (clojure.java.io/writer output-file))))