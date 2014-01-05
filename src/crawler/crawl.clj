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
   of representation to use? Target-cluster?

   Queue looks like: [{:url <url>
                      :content-decision <content-xpath>
                      :pag-decision <pag-xpath>
                      :paginated?}
                      .
                      .
                      .]
   misc: {:num-pages-to-max ..}. Number of pages to follow overall.
   what is the base idea."
  [queue visited num-decisions sum-score limit options]
  (Thread/sleep 2000)
  (if (and (not (zero? limit))
           (seq queue))
    (let [url        (-> queue first :url)
          body       (-> url client/get :body)
          paginated? (-> queue first :pagination?)
          p-thus-far (-> options :pagination)
          decision   (extractor/extract-above-average-richest
                      body url (clojure.set/union visited queue))
          xpaths     (:xpaths decision)
          links      (:links decision)
          score      (:action-score decision)]
      (do
        (println :total-score (/ sum-score num-decisions))
        (println :cur-page-score score)
        (if (< (* 0.75 (/ sum-score num-decisions)) score)
          (let [paging-decis (extractor/weak-pagination-detector
                              body
                              (first queue)
                              (clojure.set/union visited queue))
                pagination-l (second paging-decis)

                new-queue    (if (<= (-> options :pagination) 10) ; switch order of insertion at 10 urls
                               (do
                                 (println :links links)
                                 (println :pagination paging-decis)
                                 (println)
                                 (concat (map
                                          (fn [a-link]
                                            {:url a-link
                                             :pagination? true
                                             :content-xpaths xpaths
                                             :paging-xpaths (first paging-decis)}) ; xpaths are what we picked on this page. no need to re-learn anything
                                          pagination-l)
                                         (rest queue)
                                         (map
                                          (fn [x] {:url x})
                                          (filter
                                           (fn [a-link]
                                             (and (not (some #{a-link} visited))
                                                  (not (some #{a-link} (set
                                                                        (map #(-> % :url) queue))))))
                                           links))))
                               (do
                                 (println :links links)
                                 (println :pagination paging-decis)
                                 (println)
                                 (concat
                                  (rest queue)
                                  (map
                                   (fn [x] {:url x})
                                   (filter
                                    (fn [a-link]
                                      (and (not (some #{a-link}
                                                      visited))
                                           (not (some #{a-link}
                                                      (set
                                                       (map #(-> % :url) queue))))))
                                           links))
                                  (map (fn [a-link]
                                         {:url a-link
                                          :pagination? true
                                          :content-xpaths xpaths
                                          :paging-xpaths (first paging-decis)}) ; xpaths are what we picked on this page. no need to re-learn anything
                                       pagination-l))))
                new-visited  (conj visited url)
                new-num-dec  (inc num-decisions)
                new-scr      (+ score sum-score)
                new-lim      (dec limit)
                new-opt      (if paginated?
                               (merge
                                options
                                {:pagination (+ (-> options :pagination) 1)})
                               (merge options {:pagination 0}))]
            (recur new-queue
                   new-visited
                   new-num-dec
                   new-scr
                   new-lim
                   new-opt))
          (do
            (println :no-links-chosen)
            (println)
                                        ; pagination must still be chosen though
            (let [new-queue   (rest queue)
                  new-visited (conj visited url)
                  new-num-dec num-decisions
                  new-scr     sum-score
                  new-lim     (dec limit)
                  new-opt     options]
              (recur new-queue
                     new-visited
                     new-num-dec
                     new-scr
                     new-lim
                     new-opt))))))
    {:visited visited}))
