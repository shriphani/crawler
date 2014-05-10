(ns crawler.cluster
  "Clustering utilities for explorations by the crawler")

(defn merge?
  [cluster1 cluster2 similar?]
  (some
   identity
   (flatten
    (map
     (fn [x]
       (map
        (fn [y]
          (if (similar? x y)
            true))
        cluster2))
     cluster1))))

(defn assign
  [clusters x similar?]
  (let [is
        (filter
         identity
         (map
          (fn [[i c]]
            (if (merge? c x similar?)
              i
              nil))
          (map vector
               (range (count clusters))
               clusters)))]
    (if (empty? is)
      -1 (first is))))

(defn cluster
  "Points: a set of points
   similar? : are x and y similar?"
  [points similar?]
  (cond (-> points first vector?)
        (let [merged (set
                      (reduce
                       (fn [acc x]
                         (let [assign-to (assign acc x similar?)]
                           (if (neg? assign-to)
                             (into [] (cons x acc))
                             (assoc (into [] acc) assign-to (into
                                                             []
                                                             (concat (nth acc assign-to)
                                                                     x))))))
                       []
                       points))]
          (if (= merged points)
            merged
            (recur merged similar?)))

        :else
        (recur (set (map (fn [x] [x]) points))
               similar?)))

(defn cluster-n-iters
  "Args:
    points: a set of points
    similar? : are x and y similar?"
  [points similar? iters]
  (cond (zero? iters)
        points
        
        (-> points first vector?)
        (let [merged (set
                      (reduce
                       (fn [acc x]
                         (let [assign-to (assign acc x similar?)]
                           (if (neg? assign-to)
                             (into [] (cons x acc))
                             (assoc (into [] acc) assign-to (into
                                                             []
                                                             (concat (nth acc assign-to)
                                                                     x))))))
                       []
                       points))]
          (if (= merged points)
            merged
            (recur merged similar? (dec iters))))

        :else
        (recur (set (map (fn [x] [x]) points))
               similar?
               iters)))
