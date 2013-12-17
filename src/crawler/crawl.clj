(ns crawler.crawl
  "Initial crawl setup"
  (:require [clj-http.client :as client]
            [crawler.rich-extractor :as extractor]))


(defn write
  [content]
  (println :received-content (content :url)))

(defn crawl-richest
  "Main crawl routine
   First, order all the links by their exploration potential.
   The sampler extracts them in this order.
   Next, we remove XPaths that are exact repeats of content
   Next, we resolve pagination routines.

   Representation for content XPath is important. What kind
   of representation to use? Target-cluster?"
  [queue visited num-decisions sum-score limit]
  (if (and (not (zero? limit))
           (seq queue))
    (let [body       (-> queue first client/get :body)
          decision   (extractor/extract-richest body (first queue))
          links      (:links decision)
          score      (:action-score decision)]
      (println queue)
      (do
        (write {:content body
                :url (first queue)})
        (recur (if (< (/ sum-score num-decisions)
                      score)
                 (concat (rest queue)
                         (filter
                          (fn [a-link]
                            (not (some #{a-link} visited)))
                          links))
                 (rest queue))
               (conj visited (first queue))
               (inc num-decisions)
               (+ score sum-score)
               (dec limit))))
    {:visited visited}))
