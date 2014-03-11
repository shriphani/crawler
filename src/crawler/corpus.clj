(ns crawler.corpus
  (:require [cheshire.core :as json]
            [clojure.java.io :as io])
  (:use [clojure.pprint :only [pprint]])
  (:import [java.io PushbackReader]))

(defn corpus->json
  [a-corpus-file]
  (let [rdr (PushbackReader.
             (io/reader a-corpus-file))
        wrtr (io/writer
              (clojure.string/replace a-corpus-file #".corpus" ".json"))]
     (json/generate-stream
      (read rdr)
      wrtr)))
