(ns crawler.cluster
  "Clustering utilities for explorations by the crawler")

(defn assign
  "Returns a new set of clusters
after adding object x to them"
  [clusters x membership-test]
  (let [position (.indexOf
                  (map
                   (fn [a-cluster]
                     (membership-test a-cluster x))
                   clusters)
                  true)]
    (if (<= 0 position)
      (assoc clusters position (cons x (nth clusters position)))
      (cons [x] clusters))))
