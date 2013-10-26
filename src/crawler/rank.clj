;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.rank)

(defn enum-xpath-score
  [{xpath :xpath tf :tf df :df hrefs :hrefs}]
  (let [num-hrefs (count hrefs)
        avg-tf    (/ (apply + tf)
                     (count tf))]

    num-hrefs))

(defn rank-enum-candidates
  "Enumeration candidates are xpaths with the following
info:
1. average tf
2. df
3. total number of unique hrefs"
  [enum-candidates with-info?]
  (let [ranked (reverse
                (sort-by
                 enum-xpath-score enum-candidates))]
    (if with-info?
      ranked
      (map #(-> % :xpath) ranked))))


