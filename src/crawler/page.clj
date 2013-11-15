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

(defn signature-similarity-cardinality
  "Return cardinality of intersection"
  [sign1 sign2]
  (let [set-sign1 (set (map first sign1))
        set-sign2 (set (map first sign2))]
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
  (let [weights-map    weights-table
        weighted-sign  (fn [sgn]
                         (map
                          (fn [[xpath count]]
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

(defn weights-table
  [dfs-table hrefs-table]
  (let [dfs-map   (into {} dfs-table)
        hrefs-map (into {} hrefs-table)
        xpaths    (map first dfs-table)]
    (into
     {} (map
         (fn [an-xpath]
           [an-xpath (/ (Math/log (count (hrefs-map an-xpath)))
                        (dfs-map an-xpath))])
         xpaths))))

(defn signature-similarity
  [sign1 sign2]
  (let [cosine-sim      (signature-similarity-cosine sign1 sign2)
        cardinality-sim (signature-similarity-cardinality sign1 sign2)]
    (* cosine-sim cardinality-sim)))

(defn signature-edit-distance
  "Args:
    sign1: Signature of page1. Xpath: number of hrefs
    sign2: Signature of page2

    ins-cost: cost of inserting an XPath
    del-cost: cost of deleting an XPath

   Edit distance = XPath deletions and XPath additions from sign1 -> sign2"
  [sign1 sign2 ins-cost del-cost]
  (let [sign1-map     (into {} sign1)
        
        sign2-map     (into {} sign2)
        
        deletion-cost (apply
                       + (for [[x1 h1] sign1-map]
                           (cond (not (sign2-map x1))
                                 (* del-cost h1)
                                 
                                 (and (sign2-map x1)
                                      (< (sign2-map x1) h1))
                                 (* del-cost
                                    (- h1 (sign2-map x1)))

                                 :else 0)))

        insert-cost   (apply
                       + (for [[x2 h2] sign2-map]
                           (cond (not (sign1-map x2))
                                 (* ins-cost h2)

                                 (and (sign1-map x2) (< (sign1-map x2) h2))
                                 (* ins-cost (- h2 (sign1-map x2)))

                                 :else 0)))]
    (+ deletion-cost insert-cost)))

(defn signature-edit-distance-similarity
  [sign1 sign2 ins-cost del-cost]
  (let [best  (signature-edit-distance sign1 sign2 ins-cost del-cost)

        items (fn [sign]
                (reduce
                 (fn [acc [x h]]
                   (+ acc h))
                 0
                 sign))

        worst (+ (* del-cost (items sign1))
                 (* ins-cost (items sign2)))]
    (- 1 (/ best worst))))
