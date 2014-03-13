;;;; Soli Deo Gloria
;;;; Author : spalakod@cs.cmu.edu

(ns crawler.utils
  (:require [clj-http.client :as client]
            [clj-http.cookies :as cookies]
            [clj-time.core :as time]
            [org.bovinegenius [exploding-fish :as uri]])
  (:use [clojure.tools.logging :only (info error)]
        [clj-logging-config.log4j]))

(defn global-logger-config
  []
  (set-logger!
   :level :debug
   :out   (org.apache.log4j.FileAppender.
           (org.apache.log4j.EnhancedPatternLayout.
            org.apache.log4j.EnhancedPatternLayout/TTCC_CONVERSION_PATTERN)
           "logs/crawl.log"
           true)))

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
  (for [x l1
        y l2]
    (list x y)))

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

(defn get-and-log
  ([a-link]
     (get-and-log a-link {}))
  ([a-link info]
     (try (-> (client/get a-link) :body)
          (catch Exception e
            (do (error :fetch-failed info)
                (error :url a-link)
                (error (.getMessage e)))))))

(defn reset
  [an-atom value]
  (swap! an-atom (fn [an-obj] value)))

(defn find-in
  "Args:
    m: map
    key: duh

   Returns:
    key that is somewhere in the nested map"
  [m key]
  (cond (not (keys m))         ; leaf
        nil
        
        (some #{key} (keys m)) ; found it
        (m key)

        :else                  ; search
        (first
         (map
          #(when (map? (m %))
             (find-in
              (m %) key))
          (keys m)))))

;; This set of routines has their arguments reversed
;; so I can use them with swap! and atoms
(defn cons-aux
  [coll x]
  (cons x coll))

;; Reading explorations from a file
(defn read-data-file
  [a-file]
  (-> a-file
      clojure.java.io/reader
      java.io.PushbackReader.
      read))

(defn cluster-urls
  [clusters]
  (reverse
   (sort-by
    count
    (map
     (fn [a-cluster]
       (map
        (fn [x]
          (-> x :url))
        a-cluster))
     clusters))))

(defn tokenize
  "Simplistic english tokenizer"
  [a-string]
  (let [string-split (clojure.string/split a-string #"\s+")]
    (filter
     #(not= % "")
     (map
      (fn [a-token]
        (-> a-token
            (.toLowerCase)))
      string-split))))

(defn tokenize-url
  "Split on slugs. Slug urls typically contain - and _ elements"
  [a-url]
  (let [url-path (-> a-url uri/path)]
    (if url-path (last (map tokenize (clojure.string/split url-path #"/"))) "")))

(def my-cs (cookies/cookie-store)) ; for removing that SID nonsense

(defn download-with-cookie
  [a-link]
  (try (-> a-link (client/get {:cookie-store my-cs}) :body)
       (catch Exception e nil)))

(def *document-cache* (atom {}))

(defn download-cache-with-cookie
  [a-link]
  (if-not (@*document-cache* a-link)
    (do (let [document (download-with-cookie a-link)]
          (sayln :cache-miss)
          (swap! *document-cache*
                 merge
                 {a-link document})
          document))

    (do (sayln :cache-hit)
        (@*document-cache* a-link))))

(defn positions-at
  ".indexOf variant that finds multiple instances"
  [collection item]
  (let [v-collection (into [] collection)]
    (reduce
     (fn [acc i]
       (if (= item (nth v-collection i))
         (cons i acc)
         acc))
     []
     (range
      (count v-collection)))))

(defn positions-at-f
  ".indexOf variant that finds multiple instances"
  [collection f]
  (let [v-collection (into [] collection)]
    (reduce
     (fn [acc i]
       (if (f (nth v-collection i))
         (cons i acc)
         acc))
     []
     (range
      (count v-collection)))))

(defn sayln
  "println that prints to stderr"
  [& stuff]
  (binding [*out* *err*]
    (apply println stuff)))

(defn dated-filename
  ([] (dated-filename "" ""))
  ([prefix suffix]
     (let [cur-time (time/now)

           day (time/day cur-time)
           mon (time/month cur-time)
           yr  (time/year cur-time)
           hr  (time/hour cur-time)
           min (time/minute cur-time)]
       (str prefix
            "-"
            day
            "-"
            mon
            "-"
            yr
            "-"
            hr
            "-"
            min
            suffix))))
