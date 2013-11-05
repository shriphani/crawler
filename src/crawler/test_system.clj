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
    :xpath "//html/body[contains(@id,'phpbb') and contains(@class,'section')]/div[contains(@id,'wrap')]/div[contains(@id,'page')]/div[contains(@class,'topic')]/div[contains(@class,'pagination')]/span/a"}

   {:url   "http://stackoverflow.com/questions?pagesize=50&sort=newest"
    :xpath "//html/body[contains(@class,'questions')]/div[contains(@class,'container')]/div[contains(@id,'content')]/div[contains(@id,'mainbar')]/div[contains(@class,'pager')]/a"}

   {:url   "http://gis.stackexchange.com/questions?pagesize=50&sort=newest"
    :xpath "//html/body[contains(@class,'questions')]/div[contains(@class,'container')]/div[contains(@id,'content')]/div[contains(@id,'mainbar')]/div[contains(@class,'pager')]/a"}

   {:url   "http://carsandetc.tumblr.com/"
    :xpath "//html/body/div[contains(@id,'wrapper')]/div[contains(@id,'content')]/div[contains(@id,'navigation')]/a"}

   {:url   "http://www.topix.com/forum/city/carrizo-springs-tx/"
    :xpath "//html/body[contains(@id,'stream')]/div[contains(@id,'content')]/div[contains(@id,'content') and contains(@class,'xtra')]/div[contains(@id,'onetwocombo')]/div[contains(@class,'onetwosub')]/div/div/a"}

   {:url   "http://grails.1312388.n4.nabble.com/"
    :xpath "//HTML/body/div[contains(@id,'nabble') and contains(@class,'nabble')]/span[contains(@class,'pages')]/span[contains(@class,'page')]/a"}])

(def blogs-test-case
  ["http://www.bbcamerica.com/anglophenia/"
   "http://www.charitywater.org/blog/"
   "http://magazine.enterprise.co.uk/"
   "http://www.hermanmiller.com/discover/"
   "http://www.thesartorialist.com/"
   "http://blogs.hbr.org/"])

(defn test-enumeration
  ([]
     (test-enumeration {:verbose false}))
  ([{verbose :verbose}]
     (if verbose
       (pmap
        (fn [{url :url xpath :xpath}]
          (extractor/process-link url))
        *enumerate-positives*)
       (pmap
        (fn [{url :url xpath :xpath}]
          (let [ranked-enums (extractor/process-link url)
                xpaths       (map #(-> % :xpath) ranked-enums)]
            (.indexOf xpaths xpath)))
        *enumerate-positives*))))

(defn -main
  [& args]
  (let [[optional _ _] (cli/cli args
                                ["-v" "--verbose" "Print out info as well" :default false :flag true]
                                ["-b" "--blogs" "Verbose test of the blogs framework" :default false :flag true])]
    (println optional)
    (if (:blogs optional)
      (clojure.pprint/pprint
       (pmap #(extractor/process-link %) blogs-test-case))
      (if (:verbose optional)
        (clojure.pprint/pprint (test-enumeration optional))
        (doseq [[rank url] (map
                            vector
                            (test-enumeration optional)
                            (map #(-> % :url) *enumerate-positives*))]
          (println "URL:" url)
          (println "Rank:" rank))))))
