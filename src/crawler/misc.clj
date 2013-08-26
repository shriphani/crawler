;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu
;;;;
;;;; Code that is not specific to the project itself goes here.


(ns crawler.misc
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io ByteArrayInputStream]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

(defn slurp-file->lines
  "Use this to handle small files. We read the file in 1 go
and then split it up into lines"
  [filename]
  (str/split
   (slurp filename)
   #"\s+"))

(defn file->lines
  "Use this for larger files. This leaves the open/close
business to the jvm"
  [filename]
  (line-seq (io/reader filename)))

(defn gzip-writer
  [filename]
  (-> filename
     io/output-stream
     GZIPOutputStream.
     io/writer))