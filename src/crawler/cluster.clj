(ns crawler.cluster
  "Clustering utilities for explorations by the crawler")

(defn assign-where-single-linkage?
  "At least 1 cluster member satisfies the membership test"
  [clusters x membership-test]
  (.indexOf
   (map
    (fn [a-cluster]
      (membership-test a-cluster x))
    clusters)
   true))

(defn assign-single-linkage
  "Returns a new set of clusters
   after adding object x to them"
  [clusters x membership-test]
  (let [position (assign-where-single-linkage? clusters x membership-test)]
    (if (<= 0 position)
      (assoc clusters position (cons x (nth clusters position)))
      (cons [x] clusters))))

(defn assign-average-linkage [] '*)

(defn assign-maximum-linkage [] '*)
