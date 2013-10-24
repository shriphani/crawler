;;;; Soli Deo Gloria
;;;; Author : spalakod@cs.cmu.edu

(ns crawler.utils
  (:require [clj-http.client :as client]
            [org.bovinegenius [exploding-fish :as uri]]))

(def relative? #(not (uri/absolute? %))) ; relative-url?

(def delay-ms 3000)

(defn in-domain?
  "Is the target link in the same domain as the site.
If the target is a relative link, then the answer is yes.
If not, then just the netlocs are compared"
  [site target]
  (or (relative? target)
     (= (uri/host site) (uri/host target))))

(defn random-dequeue
  "Return first or last in queue and the rest of the queue.
Chosen op is based on a coin-toss. The runtime of this guy
is linear unfortunately"
  [sequence]
  (let [nonce (rand-int 2)]
    (if (zero? nonce)
      (list (first sequence) (rest sequence))
      (list (last sequence) (butlast sequence)))))

(defn cross-product
  "Compute the cross product of 2 lists"
  [l1 l2]
  (reduce
   concat
   []
   (map
    (fn [l1-item]
      (map 
       (fn [l2-item]
         (list l1-item l2-item))
       l2))
    l1)))

(defn file-exists?
  [a-filename]
  (-> (java.io.File. a-filename)
      (.exists)))

(def regex-char-esc-smap
  (let [esc-chars "()*&^%$#!"]
    (zipmap esc-chars
            (map #(str "\\" %) esc-chars))))

(defn str->pattern
  "re-pattern doesn't generate the correct
regex patterns we need. Use this to correctly
escape characters. then call re-pattern on it"
  [s]
  (->> s
       (replace regex-char-esc-smap)
       (reduce str)))

(defn abs
  [n]
  (if (pos? n) n (- n)))

(defn prefix-match
  [s1 s2]
  (count
   (take-while
    (fn [[x y]] (= x y))
    (map vector s1 s2))))

(defn atom-merge-with
  [atom-value f other-value]
  (merge-with f atom-value other-value))

(defn sample-proportional
  [items-scores]
  (let [sorted-items (sort-by second items-scores)
        total-score  (apply + (map second sorted-items))
        normalized   (map
                      (fn [[x s]]
                        [x (/ s total-score)])
                      sorted-items)

        sampled      (rand)]
    (-> (reduce
         (fn [acc v]
           (if (>= (second acc) sampled)
             acc
             [v (+ (second acc) (second v))]))
         ["" 0]
         normalized)
        first
        first)))

(defn and-fn
  "Reduce using and"
  [x y]
  (and x y))

(defn or-fn
  "Reduce using or"
  [x y]
  (or x y))
