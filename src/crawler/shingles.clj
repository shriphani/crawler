(ns crawler.shingles
  "Implementation of the shingles algorithm"
  (:require [crawler.dom :as dom]))

(defn html-doc-4-grams
  "Retrieves all 4-grams from a HTML document"
  [html-document]
  (let [processed-page  (dom/process-page html-document)

        page-text (.getText processed-page)

        cleaned-page-text (-> page-text
                              clojure.string/lower-case
                              (clojure.string/replace #"\p{Punct}+" " ")
                              (clojure.string/replace #"\s+" " ")
                              (clojure.string/trim))

        tokens-in-seq (clojure.string/split cleaned-page-text #"\s+")]
    (set
     (apply
      map
      vector
      (map
       (fn [i]
         (drop i tokens-in-seq))
       (range 4))))))

(defn near-duplicate-4-grams?
  "Uses jaccard similarity"
  [doc1-4s doc2-4s]
  (>=
   (/ (count (clojure.set/intersection doc1-4s
                                       doc2-4s))
      (count (clojure.set/union doc1-4s
                                doc2-4s)))
   0.9))

(defn near-duplicate?
  [doc1 doc2]
  (let [doc1-4grams (html-doc-4-grams doc1)
        doc2-4grams (html-doc-4-grams doc2)]
    (near-duplicate-4-grams? doc1-4grams
                             doc2-4grams)))
