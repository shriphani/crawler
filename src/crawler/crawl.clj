(ns crawler.crawl
  "Initial crawl setup"
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [crawler.rich-extractor :as extractor])
  (:use [clojure.pprint :only [pprint]]))


(defn write
  [content filename]
  (println (-> content :url class))
  (spit filename (with-out-str (prn content)) :append true))

(defn distinct-by-key
 [coll k]
 (reduce
  (fn [acc v]
    (if (some #{(v k)} (set (map #(k %) acc)))
      acc
      (cons v acc)))
  []
  coll))

(defn crawl-richest
  "Main crawl routine
   First, order all the links by their exploration potential.
   The sampler extracts them in this order.
   Next, we remove XPaths that are exact repeats of content
   Next, we resolve pagination routines.

   Representation for content XPath is important. What kind
   of representation to use? Target-cluster?

   A queue looks like: [{:url <url>
                      :content-decision <content-xpath>
                      :pag-decision <pag-xpath>
                      :paginated?}
                      .
                      .
                      .]
   We have pagination and content queues.
   A decision looks like:

   [XPath {:links []}]

   globals: use this to store global info for the crawler"
  [queues visited num-decisions sum-score limit globals]
  (Thread/sleep 2000)
  (println globals)
  (if (and (not (zero? limit))
           (or (seq (-> queues :content))
               (seq (-> queues :pagination))))
    (let [content-q   (-> queues :content)         ; holds the content queues
          paging-q    (-> queues :pagination)      ; holds the pagination queues
          
          queue-kw    (cond
                       (empty? content-q)
                       :pagination

                       (empty? paging-q)
                       :content

                       :else
                       (if (even? (int (/ (count visited) 10)))
                         :pagination :content))

          queue       (queues queue-kw)

          url         (-> queue first :url)
          body        (-> url client/get :body)
          new-visited (conj visited url)
          paginated?  (-> queue first :pagination?)

          ;; extract from whatever needs extracting
          extracted   (extractor/extract-above-average-richest
                       body
                       (first queue)
                       (clojure.set/union visited queue))
          decision    (:decision extracted)
          xpaths      (map first decision)
          score       (:score extracted)]
      (do
        (println :total-score (/ sum-score num-decisions))
        (println :cur-page-score score)
        (if (or paginated?
                (< (* 0.75
                      (/ sum-score num-decisions))
                   score))
          (do
            (write
             {:url       url
              :body      body
              :src-url   (-> queue first :source)
              :src-xpath (-> queue first :src-xpath)}
             "crawl.json")
            (let [ ;; the decision made by the pagination component
                  paging-dec   (extractor/weak-pagination-detector
                                body
                                (first queue)
                                globals
                                (clojure.set/union visited
                                                   (map #(-> % :url) content-q)
                                                   (map #(-> % :url) paging-q)
                                                   [url]))
                  
                  ;; pagination's xpath and links
                  paging-xp-ls (into
                                {} (map
                                    (fn [[xpath info]]
                                     [xpath (:links info)])
                                    paging-dec))
                  
                  paging-vocab (reduce
                                clojure.set/union
                                (map
                                 (fn [[xpath info]]
                                   (:vocab info))
                                 paging-dec))
                  
                  new-globals  (merge-with clojure.set/union
                                           globals
                                           {:paging-vocab paging-vocab})
                  
                  ;; add to content-q if anything needs adding/removing
                  content-q'   (concat (if (= :content queue-kw)
                                         (rest content-q) content-q)
                                       (distinct-by-key
                                        (flatten
                                         (map
                                          (fn [[x links]]
                                            (filter
                                             identity
                                             (map
                                              (fn [a-link]
                                                (when-not (and (some #{a-link} visited)
                                                               (some #{a-link}
                                                                     (set
                                                                      (map #(-> % :url) content-q))))
                                                  {:url a-link
                                                   :source url
                                                   :src-xpath x}))
                                              links)))
                                          decision))
                                        :url))

                  ;; add to paging-q if anything needs adding/removing
                  paging-q'    (concat (if (= :pagination queue-kw)
                                         (rest paging-q) paging-q)
                                       (distinct-by-key
                                        (flatten
                                         (map (fn [[xpath links]]
                                                (map
                                                 (fn [a-link]
                                                   (when-not (and (some #{a-link} visited)
                                                                  (some #{a-link}
                                                                        (set
                                                                         (map #(-> % :url) content-q))))
                                                     {:url a-link
                                                      :source url
                                                      :src-xpath xpath
                                                      :pagination? true
                                                      :content-xpaths xpaths}))
                                                 links))
                                              paging-xp-ls))
                                        :url))

                
                  new-num-dec  (inc num-decisions)
                  new-scr      (+ score sum-score)
                  new-lim      (dec limit)]
              (recur {:content content-q'
                      :pagination paging-q'}
                     new-visited
                     new-num-dec
                     new-scr
                     new-lim
                     new-globals)))
          (do
            (println :no-links-chosen)
            (println)
            (write
             {:url       url
              :body      body
              :src-url   (-> queue first :source)
              :src-xpath (-> queue first :src-xpath)
              :leaf?     true}
             "crawl.json")
            (let [paging-dec   (extractor/weak-pagination-detector
                                body
                                (first queue)
                                globals
                                (clojure.set/union visited
                                                   (map #(-> % :url) content-q)
                                                   (map #(-> % :url) paging-q)
                                                   [url]))

                ;; pagination's xpath and links
                  paging-xp-ls (into
                                {} (map
                                    (fn [[xpath info]]
                                      [xpath (:links info)])
                                    paging-dec))
                  
                  paging-vocab (reduce
                                clojure.set/union
                                (map
                                 (fn [[xpath info]]
                                   (:vocab info))
                                 paging-dec))
                  
                  new-globals  (merge-with clojure.set/union
                                         globals
                                         {:paging-vocab paging-vocab})
                  
                  new-queue    (if (= :pagination queue-kw)
                                 {:content content-q
                                  :pagination (rest paging-q)}
                                 {:content (rest content-q)
                                  :pagination paging-q})
                  new-num-dec num-decisions
                  new-scr     sum-score
                  new-lim     (dec limit)]
              (recur new-queue
                     new-visited
                     new-num-dec
                     new-scr
                     new-lim
                     new-globals))))))
    {:visited visited}))
