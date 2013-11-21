(ns crawler.cluster
  "Clustering utilities for explorations by the crawler")

(defn assign-where?
  "At least 1 cluster member satisfies the membership test"
  [clusters x membership-test]
  (.indexOf
   (map
    (fn [a-cluster]
      (membership-test a-cluster x))
    clusters)
   true))

(defn assign
  "Returns a new set of clusters
   after adding object x to them"
  [clusters x membership-test]
  (let [position (assign-where? clusters x membership-test)]
    (if (<= 0 position)
      (assoc clusters position (cons x (nth clusters position)))
      (cons [x] clusters))))
