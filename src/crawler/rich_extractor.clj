(ns crawler.rich-extractor
  "Implementation of the rich URL extractor"
  (:require [crawler.dom :as dom]
            [crawler.rank :as rank]
            [crawler.sample :as sample]
            [crawler.similarity :as similarity]
            [crawler.utils :as utils]
            [clj-http.client :as client]
            [clojure.set :as clj-set]
            [org.bovinegenius [exploding-fish :as uri]]))

;;;; A rich extractor performs an initial score assignment based
;;;; on how expressive a URL is. A decision to follow is based upon
;;;; some statistics.

(defn tokenize-anchor-url
  [nodes]
  (map
   (fn [{_ :node href :href text :text}]
     (let [url-tokens  (set (utils/tokenize-url href))
           text-tokens (set (utils/tokenize text))]
       {:url-tokens url-tokens
        :text-tokens text-tokens}))
   nodes))

(defn state-action
  "Args:
    page-src: the page body
    url: the source link
    blacklist: links that are already visited or addressed."
  ([page-src url]
     (state-action page-src url []))
  
  ([page-src url blacklist]
     (let [processed-pg      (dom/html->xml-doc page-src)
           xpaths-hrefs-text (dom/xpaths-hrefs-tokens-no-position
                              processed-pg url blacklist)
           host              (uri/host url)
           in-host-xhrefs    (into
                              {}
                              (filter
                               #(-> % second empty? not)
                               (map
                                (fn [[xpath nodes]]
                                  [xpath (filter
                                          (fn [a-node]
                                            (or (= host (uri/host (:href a-node)))
                                                (nil? (uri/host (:href a-node)))))
                                          nodes)])
                                xpaths-hrefs-text)))]
       in-host-xhrefs)))

(defn tokenize-actions
  "Args:
    in-host-xhrefs: {xpath -> url/text}"
  [in-host-xhrefs]
  (into
   {} (map
       (fn [[xpath nodes]]
         [xpath (tokenize-anchor-url nodes)])
       in-host-xhrefs)))

(defn score-actions
  [xpaths-tokens]
  (rank/score-xpaths-1 xpaths-tokens))

(defn get-and-wait
  [a-link]
  (do
    (Thread/sleep 2000)
    (client/get a-link)))

(defn explore-pagination
  "Pagination is detected using:
    -> pages that have similar structure
    -> exploration is in direction based on inverse of score"
  [page-src url blacklist]
  (let [in-host-xpaths-hrefs (state-action page-src url blacklist)
        xpaths-hrefs         (into
                              {} (map
                                  (fn [[xpath nodes]]
                                    [xpath (map
                                            #(-> % :href)
                                            nodes)])
                                  in-host-xpaths-hrefs))
        xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)
        xpaths-scored        (score-actions xpaths-tokenized)
        candidates-ordered   (map first (sort-by second xpaths-scored))
        to-sample-links      (map
                              vector
                              candidates-ordered
                              (map
                               (fn [xpath]
                                 (take 20 (distinct
                                           (filter
                                            (fn [x]
                                              (not (some #{x} (set blacklist))))
                                            (xpaths-hrefs xpath)))))
                               candidates-ordered))]
    (println candidates-ordered)
    to-sample-links))

(defn score-xpaths
  [page-src url blacklist]
  (let [in-host-xpaths-hrefs (state-action page-src
                                           url
                                           blacklist)
        xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)]
    (score-actions xpaths-tokenized)))

(defn extract-richest
  "Retrieve from possible actions, the xpath with the highest score
   Args:
    page-src: the page source
    url: the page's url
    blacklist: what decision is made"
  ([page-src url]
     (extract-richest page-src url (set [])))

  ([page-src url blacklist]
     (let [in-host-xpaths-hrefs (state-action page-src url blacklist)
           xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)
           xpaths-scored        (score-actions xpaths-tokenized)
           decision             (first
                                 (first
                                  (reverse
                                   (sort-by second xpaths-scored))))]
       (println :url url)
       (println :decision decision)
       {:links (distinct (map #(-> % :href) (in-host-xpaths-hrefs decision)))
        :action-score (xpaths-scored decision)})))

(defn extract-above-average-richest
  "Extract above-average richness XPaths"
  ([page-src url-ds]
     (extract-above-average-richest page-src url-ds (set [])))

  ([page-src url-ds blacklist]
     (let [url        (:url url-ds)
           paginated? (-> url-ds :pagination?)
           in-host-xpaths-hrefs (state-action page-src url blacklist)
           xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)
           xpaths-scored        (score-actions xpaths-tokenized)]
       
       (if-not paginated?
         (let [mean-richness        (/ (apply + (map second xpaths-scored))
                                       (count xpaths-scored))
               
                                        ; return xpaths and scores of items who clocked above the
                                        ; mean richness of the webpage
               
               decision             (filter
                                     (fn [[xpath score]]
                                       (>= score mean-richness))
                                     (reverse
                                      (sort-by second xpaths-scored)))
               
                                        ; links at the chosen XPaths
               decision-links       (into
                                     {}
                                     (map
                                      (fn [[a-decision score]]
                                        [a-decision
                                         (map
                                          #(-> % :href)
                                          (in-host-xpaths-hrefs a-decision))])
                                      decision))
               
                                        ; this guy is computing the score products of the decisions alone
               decision-scores      (/ (reduce
                                        (fn [acc [a-decision score]]
                                          (+ acc score))
                                        0
                                        decision)
                                       (count decision))]
          (do
            (println :url url)
            (println :decision decision)
            {:decision decision-links
             :score decision-scores}))
         
         (let [decision (-> url-ds :content-xpaths)
               decision-links (into
                               {} (map
                                   (fn [a-decision]
                                     [a-decision
                                      (map
                                       #(-> % :href)
                                       (in-host-xpaths-hrefs a-decision))])
                                decision))
               decision-scores (/ (reduce
                                   (fn [acc a-decision]
                                     (+ acc (xpaths-scored a-decision)))
                                   0
                                   decision)
                                  (count decision))]
           (do
             (println :url url)
             (println :decision decision)
             (println "PAGINATED")
             {:decision decision-links
              :score decision-scores}))))))

(defn pagination?
  "Args:
    {xpath -> [samples] ...}
   Returns:
    Xpaths which are samples"
  [page-src [xpath samples]]
  (let [similarities (pmap
                      #(similarity/tree-edit-distance-html page-src %)
                      samples)

        thresh-sims  (filter #(> % 0.8) similarities)]
    (> (count thresh-sims) (* 0.5 (count samples)))))

(defn updated
  [xpaths-hrefs1 xpaths-hrefs2]
  (let [xpaths (set (map first xpaths-hrefs1))
        upds   (map
                (fn [xpath] (count
                            (clojure.set/difference
                             (set (xpaths-hrefs1 xpath))
                             (set (xpaths-hrefs2 xpath)))))
                xpaths)]
    (apply + upds)))

(defn pick-paginator-updated
  "Picks one XPath for pagination
   Args:
    {xpath: sample-bodies,...}
   Returns:
    picked xpath"
  [src-xpaths-hrefs pagination-candidates blacklist]
  (let [candidate-xpaths (map first pagination-candidates)]
    (first ; don't return the score as well
     (last
      (sort-by
       second
       (map
        (fn [[xpath sampled]]
          (let [s (map
                   (fn [a-sampled-page]
                     (updated src-xpaths-hrefs a-sampled-page))
                   sampled)]
            [xpath (/ (apply + s) (count s))]))
        pagination-candidates))))))

(defn pick-paginator-weakest
  "A pagination that uses the least number of
   avg. tokens and the least number of total chars
   is picked for this purpose"
  [pagination-candidates]
  (sort-by
   second
   (map
    (fn [[a-candidate tokens]]
      [a-candidate
       (if (empty? tokens)
         0
         (/ (apply + (map count tokens))
            (count tokens)))])
    pagination-candidates)))

(defn weak-pagination-detector
  "Detects a pagination based on how weak things are.
   Weakness = smallest # of tokens.
   Args:
    page-src : body
    url-ds : url datastructure (bubbled with some info like are we paginated etc)
    global-info : stores some global statistics the crawler has computed.
    blacklist : what not to get

   Returns:
    Pagination candidates
    (one or more xpaths)."
  ([page-src url-ds global-info]
     (weak-pagination-detector page-src url-ds global-info (set [])))

  ([page-src url-ds global-info blacklist]
     (let [url        (-> url-ds :url)
           paginated? (-> url-ds :pagination?)
           xpath      (-> url-ds :src-xpath)

           vocabulary (-> global-info :paging-vocab)
           
           in-host-xpaths-hrefs (state-action page-src url blacklist)
           xpaths-hrefs         (into
                                 {} (map
                                     (fn [[xpath nodes]]
                                       [xpath (map #(-> % :href) nodes)])
                                     in-host-xpaths-hrefs))
           xpaths-tokenized     (tokenize-actions in-host-xpaths-hrefs)]
       (if-not paginated?
         (let [xpaths-scored        (sort-by second
                                             (score-actions xpaths-tokenized))
               
               mean-richness        (/ (apply + (map second xpaths-scored))
                                       (count xpaths-scored))
               
               xpaths-considered    (map
                                     (fn [[xpath _]]
                                       [xpath (xpaths-hrefs xpath)])
                                     (filter
                                      (fn [[xpath score]]
                                        (< score mean-richness))
                                      xpaths-scored))

               xpaths-considered'   (if (and vocabulary (not (empty? vocabulary)))
                                      (filter
                                       (fn [[xpath links]]
                                         (let [xpath-text-tokens (reduce
                                                                  clojure.set/union
                                                                  (map
                                                                   (fn [a-tok-map]
                                                                     (set (:text-tokens a-tok-map)))
                                                                   (xpaths-tokenized xpath)))]
                                           (not
                                            (empty?
                                             (clojure.set/intersection
                                              vocabulary xpath-text-tokens)))))
                                       xpaths-considered)
                                      xpaths-considered)
               
               xpaths-samples       (map
                                     (fn [[xpath links]]
                                       [xpath (sample/sample-some-links links blacklist)])
                                     xpaths-considered')
               
               xpaths-samples-map   (into {} xpaths-samples)


               pagination-cands     (map
                                     first
                                     (filter
                                      #(pagination? page-src %)
                                      xpaths-samples))
               
               pagination-xhref-ds  (map
                                     (fn [an-xpath]
                                       [an-xpath
                                        (map (fn [a-sample]
                                               (into
                                                {} (map
                                                    (fn [[xpath nodes]]
                                                      [xpath (map #(-> % :href) nodes)])
                                                    (dom/xpaths-hrefs-tokens-no-position
                                                     (dom/html->xml-doc a-sample)
                                                     url))))
                                             (xpaths-samples-map an-xpath))])
                                     pagination-cands)

               pagination-token-ds  (map
                                     (fn [an-xpath]
                                       [an-xpath (let [xp-toks (xpaths-tokenized an-xpath)]
                                                   (sort-by
                                                    count
                                                    (reduce
                                                     clojure.set/union
                                                     (map
                                                      (fn [a-tok-map]
                                                        (:text-tokens a-tok-map))
                                                      xp-toks))))])
                                     pagination-cands)

               picked-xpath         (pick-paginator-weakest pagination-token-ds)

               picked-links         (into
                                     {}
                                     (map
                                      (fn [x]
                                        [x {:links (xpaths-hrefs x)
                                            :vocab (reduce
                                                    clojure.set/union
                                                    (map
                                                     (fn [tok-info]
                                                       (set (-> tok-info :text-tokens)))
                                                     (xpaths-tokenized x)))}])
                                      (map first picked-xpath)))]
           
           picked-links)

         ;else just eval the submitted xpaths and deliver ze results
         (let [picked-links {xpath {:links (xpaths-hrefs xpath)
                                    :vocab (reduce
                                            clojure.set/union
                                            (map
                                             (fn [tok-info]
                                               (set (-> tok-info :text-tokens)))
                                             (xpaths-tokenized xpath)))}}]
           picked-links)))))

(defn leaf?
  [sum-score num-decisions cur-score]
  (>= (* 0.75
         (/ sum-score num-decisions))
      cur-score))
