(ns crawler.corpus
  "Tools for processing a downloaded corpus"
  (:require [cheshire.core :as json]
            [clojure.java.io :as io])
  (:use [clojure.pprint :only [pprint]])
  (:import [java.io PushbackReader]))

(defn corpus->json
  "Converts a corpus file which is essentially a pprinted map
   to a json file that python or someone else can handle"
  [a-corpus-file]
  (let [rdr (PushbackReader.
             (io/reader a-corpus-file))
        wrtr (io/writer
              (clojure.string/replace a-corpus-file #".corpus" ".json"))]
     (json/generate-stream
      (read rdr)
      wrtr)))
