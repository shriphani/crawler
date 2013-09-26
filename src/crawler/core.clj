;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu

(ns crawler.core
  (:require [clojure.tools.cli :as cli]
            [clj-http.client :as client]
            [org.bovinegenius [exploding-fish :as uri]]
            [crawler.page-utils :as page-utils]
            [crawler.utils :as utils])
  (:use [clojure.pprint]))

(def *lemurproject-ua-string*
  (str
   "Mozilla/5.0 (compatible; mandalay admin@lemurproject.org;"
   " "
   "+http://boston.lti.cs.cmu.edu/crawler/clueweb12pp/"))

(def *lemurproject-header*
  {"User-Agent" *lemurproject-ua-string*})

(def *delay-policies*
  {:min-wait-ms 3000})
