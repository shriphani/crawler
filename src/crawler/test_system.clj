;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.test-system
  (:gen-class :main true)
  (:require [clojure.tools.cli :as cli]
            [crawler.dom :as dom]
            [crawler.extractor :as extractor]
            [crawler.page :as page]
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
  ["http://www.charitywater.org/blog/"
   "http://www.hermanmiller.com/discover/"
   "http://www.thesartorialist.com/"])

(def tumblr-test-case
  ["http://oldpeoplefacebook.tumblr.com/"
   "http://anthonybourdain.tumblr.com/"
   "http://1796foods.com/"
   "http://efccooking.tumblr.com/"
   "http://scandybars.tumblr.com/"
   "http://gastrogirl.tumblr.com/"
   "http://foodculturalist.tumblr.com/"
   "http://sexiestfoods.tumblr.com/"
   "http://artinmycoffee.com/"])

(defn test-enumeration
  ([]
     (test-enumeration {:verbose false}))
  ([{verbose :verbose}]
     (if verbose
       (pmap
        (fn [{url :url xpath :xpath}]
          (extractor/process-link url {}))
        *enumerate-positives*)
       (pmap
        (fn [{url :url xpath :xpath}]
          (let [ranked-enums (extractor/process-link url {})
                xpaths       (map #(-> % :xpath) ranked-enums)]
            (.indexOf xpaths xpath)))
        *enumerate-positives*))))

(defn test-similarity
  []
  (pmap
   (fn [{url :url xpath :xpath}]
     {:url url
      :similarities (let [{src-link :src-link
                           explorations :explorations
                           xpaths-hrefs :xpaths-hrefs
                           signature    :signature
                           dfs          :dfs
                           hrefs        :hrefs
                           novelties    :novelties
                           _            :in-host-xpaths-hrefs}
                          (extractor/process-link url {})]
                      (pmap
                       (fn [{xpath :xpath xpath-explorations :explorations}]
                         {:xpath
                          xpath

                          :similarities
                          (map
                           #(let [xpaths      (extractor/exploration-xpaths %)
                                  xpath-hrefs (-> % :hrefs-table)
                                  expl-sign   (page/page-signature xpaths xpath-hrefs)]
                              {:url                 (-> % :url)
                               :similarity         (* (page/signature-similarity-cosine
                                                       signature expl-sign)
                                                      (page/signature-similarity-cardinality
                                                       signature expl-sign))})
                           xpath-explorations)})
                       explorations))})
   *enumerate-positives*))

(defn dump-explorations
  []
  (pmap
   (fn [{url :url xpath :xpath}]
     (let [[explorations dfs hrefs novelties]
           (extractor/process-link url {})]
       explorations))
   *enumerate-positives*))

(defn -main
  [& args]
  (let [[optional _ _] (cli/cli args
                                ["--similarity-test"
                                 "Test similarity routines"
                                 :flag true
                                 :default false]
                                
                                ["--enumeration-test"
                                 "Test enumeration routine"
                                 :flag true
                                 :default false]

                                ["-v" "--verbose"
                                 "Verbose version"
                                 :flag true
                                 :default false])]
    
    (when (:similarity-test optional)
      (clojure.pprint/pprint (test-similarity)))

    (when (:enumeration-test optional)
      (clojure.pprint/pprint (test-enumeration
                              {:verbose (optional :verbose)})))))
