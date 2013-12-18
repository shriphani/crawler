(ns crawler.crawl
  "Initial crawl setup"
  (:require [clj-http.client :as client]
            [crawler.rich-extractor :as extractor]))


(defn write
  [content]
  (println :write (content :url)))

(defn crawl-richest
  "Main crawl routine
   First, order all the links by their exploration potential.
   The sampler extracts them in this order.
   Next, we remove XPaths that are exact repeats of content
   Next, we resolve pagination routines.

   Representation for content XPath is important. What kind
   of representation to use? Target-cluster?"
  [queue visited num-decisions sum-score limit]
  (Thread/sleep 2000)
  (if (and (not (zero? limit))
           (seq queue))
    (let [body       (-> queue first client/get :body)
          decision   (extractor/extract-richest body (first queue) visited)
          links      (:links decision)
          score      (:action-score decision)]
      (do
        (if (< (* 0.75 (/ sum-score num-decisions)) score)
          (let [new-queue    (do
                               (println :links links)
                               (println)
                               (concat (rest queue)
                                       (filter
                                        (fn [a-link]
                                          (and (not (some #{a-link} visited))
                                               (not (some #{a-link} (set queue)))))
                                        links)))
                new-visited  (conj visited (first queue))
                new-num-dec  (inc num-decisions)
                new-scr      (+ score sum-score)
                new-lim      (dec limit)]
            (recur new-queue
                   new-visited
                   new-num-dec
                   new-scr
                   new-lim))
          (do
            (println :no-links-chosen)
            (println)
            (let [new-queue   (rest queue)
                  new-visited (conj visited (first queue))
                  new-num-dec num-decisions
                  new-scr     sum-score
                  new-lim     (dec limit)]
              (recur new-queue
                     new-visited
                     new-num-dec
                     new-scr
                     new-lim))))))
    {:visited visited}))
