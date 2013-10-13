;;;; Soli Deo Gloria
;;;; spalakod@cs.cmu.edu

;;;; Crawler traces

(ns crawler.trace
  (:require [itsy.core :as itsy]
            [crawler.records :as records]))

(def *trace* (atom {}))
