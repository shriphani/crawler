;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.test-system
  (:gen-class :main true)
  (:require [clojure.tools.cli :as cli]
            [crawler.dom :as dom]
            [crawler.extractor :as extractor]
            [crawler.records :as records])
  (:use     [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]]))

;; positive examples : page contains enumeration
(def *enumerate-positives*
  [{:url   "http://www.fmylife.com"
    :xpath "//html/body[contains(@id,'top')]/div[contains(@id,'background')]/div[contains(@id,'content')]/div[contains(@class,'wrapper')]/div[contains(@class,'pagination')]/ul[contains(@class,'left')]/li/a"}

   {:url   "https://boards.4chan.org/vg/"
    :xpath "//html/body[contains(@class,'board')]/div[contains(@class,'pagelist')]/div[contains(@class,'pages')]/a"}

   {:url   "https://www.phpbb.com/community/viewforum.php?f=69"
    :xpath "//html/body[contains(@id,'phpbb') and contains(@class,'section')]/div[contains(@id,'wrap')]/div[contains(@id,'page')]/div[contains(@class,'topic')]/div[contains(@class,'pagination')]/span/a"}

   {:url   "https://www.phpbb.com/community/viewtopic.php?f=69&t=1870815"
    :xpath "//html/body[contains(@class,'questions')]/div[contains(@class,'container')]/div[contains(@id,'content')]/div[contains(@id,'mainbar')]/div[contains(@class,'pager')]/a"}

   {:url   "http://stackoverflow.com/questions?pagesize=50&sort=newest"
    :xpath "//html/body[contains(@class,'questions')]/div[contains(@class,'container')]/div[contains(@id,'content')]/div[contains(@id,'mainbar')]/div[contains(@class,'pager')]/a"}

   {:url   "http://gis.stackexchange.com/questions?pagesize=50&sort=newest"
    :pagination-links ["http://gis.stackexchange.com/questions?page=2&sort=newest"
                       "http://gis.stackexchange.com/questions?page=3&sort=newest"]}

   {:url              "http://carsandetc.tumblr.com/"
    :pagination-links ["http://carsandetc.tumblr.com/page/2"]}

   {:url              "http://www.topix.com/forum/city/carrizo-springs-tx"
    :pagination-links ["http://www.topix.com/forum/city/carrizo-springs-tx/p2"]}

   {:url              "http://politicalticker.blogs.cnn.com/"
    :pagination-links ["http://politicalticker.blogs.cnn.com/2013/10/01/"]}

   {:url              "http://grails.1312388.n4.nabble.com/"
    :pagination-links ["http://grails.1312388.n4.nabble.com/Grails-f1312388i35.html"]}])

(defn test-enumeration
  []
  (map
   (fn [{url :url pagination-links :pagination-links}]
     (do
       (extractor/reset)
       (println "Processing:" url)
       (flush)
       (let [ranked-pagination        (extractor/process-link url)
             ranked-pagination-hrefs  (map #(-> % :hrefs) ranked-pagination)
             ranked-pagination-detect (.indexOf
                                       (map
                                        (fn [a-href-set]
                                          (some
                                           (fn [a-link]
                                             #{a-link}
                                             (set a-href-set))
                                           pagination-links))
                                        ranked-pagination-hrefs)
                                       true)]
         (println (map #(-> % :xpath) ranked-pagination))
         (flush)
         ranked-pagination-detect)))
   *enumerate-positives*))

(defn -main
  [& args]
  (test-enumeration))
