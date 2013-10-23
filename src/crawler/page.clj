;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.page
  (:require [crawler.dom :as dom]))

(defn rank-by-potential
  [xpath-hrefs]
  (reverse
   (sort-by
    (fn [[xpath hrefs]]
      (count hrefs))
    xpath-hrefs)))

(defn signature-similarity
  [sign1 sign2]
  (let [set-sign1 (set sign1)
        set-sign2 (set sign2)]
    (/ (count
        (clojure.set/intersection set-sign1 set-sign2))
       (* (Math/sqrt (count set-sign1)) (Math/sqrt (count set-sign2))))))
