(ns crawler.crawl
  "Initial crawl setup"
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [crawler.dom :as dom]
            [crawler.rich-extractor :as rich-extractor]
            [crawler.rich-char-extractor :as rich-char-extractor]
            [crawler.template-removal :as template-removal]
            [crawler.utils :as utils]
            [clj-http.cookies :as cookies])
  (:use [clojure.pprint :only [pprint]]))


(defn write
  [content filename]
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
          extracted   (rich-extractor/extract-above-average-richest
                       body
                       (first queue)
                       (clojure.set/union visited queue))
          decision    (:decision extracted)
          xpaths      (map first decision)
          score       (:score extracted)
          leaf?       (rich-extractor/leaf? sum-score num-decisions score)]
      (do
        (println :total-score (/ sum-score num-decisions))
        (println :cur-page-score score)
        (if (or paginated?
                (not leaf?))
          (do
            (write
             {:url       url
              :body      body
              :src-url   (-> queue first :source)
              :src-xpath (-> queue first :src-xpath)}
             "crawl.json")
            (let [ ;; the decision made by the pagination component
                  paging-dec   (rich-extractor/weak-pagination-detector
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
            (let [paging-dec   (rich-extractor/weak-pagination-detector
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

(defn cur-nav-fraction
  [body space]
  (let [processed    (dom/process-page body)
        text-content (apply
                      + (map
                         count
                         (-> processed
                             (.getText)
                             (clojure.string/split #"\s+"))))
        nav-content  (:total-nav-info space)]
    (/ nav-content text-content)))

(defn crawl-informative
  "Args:
    start: Good entry point

   Args alt:
    queues: pagination and content queues
    visited: visited links
    globals: global info
    to-eliminate: global decision-space pruning"
  ([start limit]
     (let [start-body   (utils/download-with-cookie start)
           to-eliminate (template-removal/all-xpaths start-body start utils/my-cs)]
       (crawl-informative {:url start}
                          (set [])
                          {}
                          to-eliminate
                          limit)))

  ([queues visited globals to-eliminate limit]
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
             body        (-> url download-with-cookie)
             new-visited (conj visited url)
             paginated?  (-> queue first :pagination?)  ; ignore this
                                        ; guy. Not yet necessary

             space       (rich-char-extractor/state-action
                          body url to-eliminate)

             prev-nav-num (-> queue first :prev-nav)
             cur-nav-num  (cur-nav-fraction body space)
             leaf?        (rich-char-extractor/leaf?
                           prev-nav-num
                           cur-nav-num)]
         leaf?))))

(defn sample-sitemap
  ([start]
     (let [start-body   (download-with-cookie start)
           to-eliminate (template-removal/all-xpaths start-body start my-cs)]
       (sample-sitemap [{:url start}] (set []) (set []) to-eliminate [])))

  ([queue visited observed to-eliminate leaf-paths]
     (if (seq queue)
       (do
        (Thread/sleep 2000)
        (println :url (-> queue first :url))
        (println :src-url (-> queue first :src-url))
        (println :src-xpath (-> queue first :src-xpath))
        (println :score-seq (-> queue first :src-nav-num))
        (let [url     (-> queue first :url)
              src-xp  (-> queue first :src-xpath)
              src-nav-num (-> queue first :src-nav-num)
              body    (utils/download-with-cookie url)

              
              ;; ask the rich extractor to sample and extract on this page.
              content    (try (rich-char-extractor/state-action
                               body url to-eliminate (clojure.set/union
                                                      (set visited)
                                                      (set [url])
                                                      (set observed)
                                                      (set (map :url queue))))
                              (catch Exception e nil))

              score   (try (cur-nav-fraction body content)
                           (catch Exception e nil))
              
              leaf?      (or (not body)
                             (rich-char-extractor/leaf?
                              (first src-nav-num)
                              score))

              ;; doing something stupid?
              _          (println :leaf? leaf?)
              _          (println :src-score (try (double (first src-nav-num))
                                                  (catch Exception e nil)))
              _          (println :target-score (try
                                                  (double score)
                                                  (catch Exception e nil)))
              mined      (when-not leaf?
                           (rich-char-extractor/filter-content
                            content))
              
              obs-links  (try
                           (distinct-by-key
                            (reverse
                             (filter
                              #(and (-> % :url)
                                    (not (some #{(-> % :url)} (clojure.set/union
                                                               (set visited)
                                                               (set [url])
                                                               (set (map (fn [x] (:url x)) queue))))))
                              (reduce
                               concat
                               (map
                                (fn [{xpath :xpath score :score hrefs :hrefs text :texts}]
                                  ;; take only a 25 % sample from here.
                                  (map
                                   (fn [h]
                                     {:url h
                                      :src-xpath (cons xpath src-xp)
                                      :src-url url
                                      :src-nav-num (cons (if body score 0) src-nav-num)})
                                   hrefs))
                                mined))))
                             :url)
                            (catch Exception e []))              
              
              mined-links (try
                            (distinct-by-key
                             (reverse
                              (filter
                              #(and (-> % :url)
                                    (not (some #{(-> % :url)} (clojure.set/union
                                                               (set visited)
                                                               (set [url])
                                                               (set (map (fn [x] (:url x)) queue))))))
                              (reduce
                               concat
                               (map
                                (fn [{xpath :xpath score :score hrefs :hrefs text :texts}]
                                  ;; take only a 25 % sample from here.
                                  (take
                                   (Math/ceil (/ (count hrefs) 4))
                                   (map
                                    (fn [h]
                                      {:url h
                                       :src-xpath (cons xpath src-xp)
                                       :src-url url
                                       :src-nav-num (cons (if body score 0) src-nav-num)})
                                    hrefs)))
                                mined))))
                             :url)
                            (catch Exception e []))]
          (cond
           (and (not leaf?) (seq mined-links))
           (recur (concat (rest queue)
                          mined-links)
                  (clojure.set/union visited
                                     (set [url]))
                  (clojure.set/union observed (set obs-links))
                  to-eliminate
                  leaf-paths)

           (and
            (not leaf?)
            (not
             (seq mined-links)))
           (recur
            (rest queue)
            (clojure.set/union visited (set [url]))
            observed
            to-eliminate
            leaf-paths)

           :else
           (recur
            (rest queue)
            (clojure.set/union visited (set [url]))
            observed
            to-eliminate
            (cons
             (cons nil src-xp)
             leaf-paths)))))
      (frequencies leaf-paths))))

(defn crawl
  [start crawler-type num-docs]
  (cond (= :richness crawler-type)
        (crawl-richest {:content [{:url start}]
                        :pagination []}
                       (set [])
                       1
                       0
                       num-docs
                       {})))
