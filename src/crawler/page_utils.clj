;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu

(ns crawler.page-utils
  (:require [net.cgrand.enlive-html :as html]))

(defn get-links-out
  [page-body]
  (map
   #(-> %
       :attrs
       :href)
   (html/select
    (html/html-resource
     (java.io.StringReader. page-body))
    [:a])))