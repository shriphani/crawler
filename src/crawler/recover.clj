(ns crawler.recover
  "Module containing code to recover a sitemap")

(defn crawl-stream
  [corpus-file]
  (let [stream (-> corpus-file
                   clojure.java.io/reader
                   (java.io.PushbackReader.))]
    (take-while
     identity
     (repeatedly
      #(try
         (read stream)
         (catch Exception e nil))))))

(defn web-graph
  [stream]
  (map
   #(dissoc % :body)
   stream))

(defn xpath-histogram
  [xpaths]
  (reduce
   (fn [acc a-node]
     (merge-with +' acc {(:src-xpath a-node) 1}))
   {}
   xpaths))

(defn choose-xpaths
  [xpath-histogram]
  (let [scores-sum (reduce
                    (fn [acc [xpath n]]
                      (+ acc n))
                    0
                    xpath-histogram)

        normalized (reverse
                    (sort-by
                     second
                     (map
                      (fn [[xpath n]]
                        [xpath (/ n scores-sum)])
                      xpath-histogram)))
        hist-stuff (reduce
                    (fn [acc [xpath count]]
                      (if (>= (:count acc) 0.9)
                        acc
                        {:count (+ (:count acc) count)
                         :xpaths (conj (:xpaths acc) xpath)}))
                    {:count  0
                     :xpaths []}
                    normalized)]
    (:xpaths hist-stuff)))

(defn recover-leaves
  "For recovery, we want to recover close to 90% of
   threads."
  ([corpus-file]
     (recover-leaves nil (-> corpus-file crawl-stream web-graph)))

  ([srcs corpus-stream]
     (let [source-set    (set srcs)
           
           stream        (web-graph corpus-stream)
           leaves        (if (not srcs)
                           (filter
                           (fn [x] (:leaf? x))
                           stream)
                           (filter
                            (fn [x]
                             (some #{(:src-url x)} source-set))
                            stream))
           
           histogram     (xpath-histogram leaves)
           chosen-xpaths (choose-xpaths histogram)
           src-links     (map
                          (fn [x] (:src-url x))
                          (filter
                           #(some #{(:src-xpath %)} (set chosen-xpaths))
                           leaves))]
       
       src-links)))
