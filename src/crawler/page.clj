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
  "Return cardinality of intersection"
  [sign1 sign2]
  (let [set-sign1 (set sign1)
        set-sign2 (set sign2)]
    (/ (count
        (clojure.set/intersection set-sign1 set-sign2))
       (* (Math/sqrt (count set-sign1)) (Math/sqrt (count set-sign2))))))

(defn signature-similarity-cosine
  "Cosine similarity"
  [sign1 sign2]
  (let [sign1-map (into {} sign1)
        sign2-map (into {} sign2)]
    (let [inner-prod (apply
                      + (for [k1 (keys sign1-map)]
                          (if (sign2-map k1)
                            (* (sign1-map k1) (sign2-map k1))
                            0)))

          mod1       (Math/sqrt
                      (apply + (map
                                #(-> % second (Math/pow 2)) sign1-map)))

          mod2       (Math/sqrt
                      (apply + (map
                                #(-> % second (Math/pow 2)) sign2-map)))]
      
      (/ inner-prod (* mod1 mod2)))))

(defn signature-similarity-manhattan
  [sign1 sign2]
  (let [sign1-map (into {} sign1)
        sign2-map (into {} sign2)]
    (let [inner-prod (apply
                      + (for [k1 (keys sign1-map)]
                          (if (sign2-map k1)
                            (* (sign1-map k1) (sign2-map k1))
                            0)))

          mod1       (Math/sqrt (apply + (map #(-> second (Math/pow 2)) sign1-map)))

          mod2       (Math/sqrt (apply + (map #(-> second (Math/pow 2)) sign2-map)))]

      (/ inner-prod (* mod1 mod2)))))

(defn signature-similarity-weighted-cosine
  "Expected signature:
   XPath: frequency
   weights-table:
   [Xpath -> #of items globally, df globally]"
  [sign1 sign2 weights-table]
  (let [weighted-sign  (fn [sgn]
                         (map
                          (fn [[xpath count] sign1]
                            [xpath (* count (weights-table xpath))])
                          sgn))

        weight-sign-1  (weighted-sign sign1)

        weight-sign-2  (weighted-sign sign2)]
    (signature-similarity-cosine weight-sign-1 weight-sign-2)))

(defn page-signature
  [xpaths xpath-hrefs]
  (map
   (fn [an-xpath]
     [an-xpath (count (xpath-hrefs an-xpath))])
   xpaths))
