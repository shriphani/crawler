;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.test-system
  (:gen-class :main true)
  (:require [clojure.tools.cli :as cli]
            [crawler.dom :as dom]
            [crawler.page :as page]
            [crawler.rank :as rank]
            [crawler.utils :as utils])
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

(def nabble-test-case
  ["http://grails.1312388.n4.nabble.com/"
   "http://postgresql.1045698.n5.nabble.com/"
   "http://jenkins-ci.361315.n4.nabble.com/"
   "http://lucene.472066.n3.nabble.com/"
   "http://elasticsearch-users.115913.n3.nabble.com/"
   "http://windows-installer-xml-wix-toolset.687559.n2.nabble.com/"
   "http://dojo-toolkit.33424.n3.nabble.com/"
   "http://jmeter.512774.n5.nabble.com/"
   "http://abaqus-users.1086179.n5.nabble.com/"])

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

(defn test-xpaths-hrefs-tokens
  [url]
  (let [body      (utils/download-with-cookie url)]
    (dom/xpaths-hrefs-tokens body url)))
