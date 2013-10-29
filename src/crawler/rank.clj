;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.rank)

(def *href-prior* 10)

(defn enum-candidate-score
  [{xpath :xpath df :df hrefs :hrefs avg-novelty :avg-novelty}]
  (* (/ (Math/log (+ (count hrefs)
                     *href-prior*))
        df)
     avg-novelty))

(defn rank-enum-candidates
  "Enumeration candidates are xpaths with the following
info:
1. average tf
2. df
3. total number of unique hrefs"
  [enum-candidates-info]
  (->> enum-candidates-info
       (sort-by enum-candidate-score)
       reverse))