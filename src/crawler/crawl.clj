(ns crawler.crawl
  "Initial crawl setup"
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [crawler.cluster :as cluster]
            [crawler.dom :as dom]
            [crawler.rich-extractor :as rich-extractor]
            [crawler.rich-char-extractor :as rich-char-extractor]
            [crawler.template-removal :as template-removal]
            [crawler.utils :as utils]
            [clj-http.cookies :as cookies]
            (org.bovinegenius [exploding-fish :as uri])
            [structural-similarity.xpath-text :as similarity])
  (:use [clojure.pprint :only [pprint]]))

(defn write
  [content filename]
  (spit filename (with-out-str (prn content)) :append true))

(defn prepare
  [xpaths-and-urls src-path url]
  (reduce
   (fn [acc {xpath :xpath hrefs-and-texts :hrefs-and-texts}]
     (let [stuff  (utils/distinct-by-key
                   (filter
                    identity
                    (map
                     (fn [{a-link :href text :text}]
                       (when-not (some #{a-link} (:visited acc))
                         {:url  a-link
                          :path (cons xpath src-path)
                          :src-url url
                          :src-text text}))                   
                     hrefs-and-texts))
                   :url)]
       (merge-with concat acc {:bodies  (utils/random-take
                                         10
                                         stuff)
                               :visited (utils/random-take
                                         10
                                         hrefs-and-texts)})))
   {}
   xpaths-and-urls))

(defn crawl
  "The base crawler routine
   Decisions made based on specified lookahead.
   Default lookahead = 1 (I don't see the point of 2 but whatevs bro).

   The meat of this routine expects:
   - body-queue : bodies crawled in the previous phase (and those
                  we selected
   - visited    : same old

   - leaf?      : leaf node detection routine must be supplied.
                  replace this with what is needed. For tomorrow's
                  meeting spin up a structure driven crawler.

   - extract    : extractor module

   Performs bfs traversal with 1 lookahead."
  ([entry-point leaf? extract stop?]
     (crawl entry-point 1 leaf? extract stop?))
  
  ([entry-point lookahead leaf? extract stop?]
     (let [body           (utils/download-with-cookie entry-point)
           body-queue-ele {:url  entry-point
                           :body body}]
       (crawl [body-queue-ele]
              [entry-point]
              lookahead
              leaf?
              extract
              stop?
              []
              50
              {})))

  ([url-queue visited lookahead leaf? extract stop? leaf-paths leaf-limit corpus]
     (do
       (Thread/sleep 1000)
       (utils/sayln :queue-size (count url-queue))
       (utils/sayln :visited (count visited))
       (utils/sayln :leaves-left leaf-limit)
       (utils/sayln :url (-> url-queue first :url))
       (utils/sayln :src-url (-> url-queue first :src-url))
       (utils/sayln :src-path (-> url-queue first :path))
       (let [url  (-> url-queue first :url)
             body (utils/download-with-cookie url)

             unaltered-state-action (try (extract body url {} [])
                                         (catch Exception e nil))]

         (cond (or (empty? url-queue)
                   (stop? {:visited    (count visited)
                           :leaf-left  leaf-limit
                           :body       body}))
               (do
                 (utils/sayln :crawl-done)
                 {:state  {:url-queue  url-queue
                           :visited    visited
                           :lookahead  lookahead
                           :leaf-paths leaf-paths
                           :leaf-limit leaf-limit}

                  :model  (frequencies leaf-paths)

                  :corpus corpus

                  :prefix (uri/host (uri/uri url))})

               ;; leaf reached. what do bruh
               (leaf? {:anchor-text (-> url-queue first :src-text)
                       :src-url     (-> url-queue first :src-url)
                       :body        body})
               (do
                 (utils/sayln :leaf-reached)
                 (let [{new-bodies  :bodies
                        new-visited :visited}
                       (-> body
                           (extract (-> url-queue first)
                                    {}
                                    (clojure.set/union (set visited)
                                                       (set [url])
                                                       (map
                                                        #(-> % :url)
                                                        url-queue)))
                           :xpath-nav-info
                           (prepare (-> url-queue first :path)
                                    url))]
                   
                   (recur (concat (rest url-queue)
                                  new-bodies)
                          (clojure.set/union
                           (set visited)
                           (set [url]))
                          lookahead
                          leaf?
                          extract
                          stop?
                          (cons
                           (-> url-queue first :path)
                           leaf-paths)
                          (dec leaf-limit)
                          (let [corpus-entry {url
                                              {:body         body
                                               :state-action unaltered-state-action
                                               :leaf?        true
                                               :src-url      (-> url-queue
                                                                 first
                                                                 :src-url)
                                               :src-xpath    (-> url-queue
                                                                 first
                                                                 :path)}}]
                            (merge corpus corpus-entry)))))
               
               :else
               (let [{new-bodies  :bodies
                      new-visited :visited}
                     (try
                       (-> body
                           (extract (first url-queue)
                                    {}
                                    (clojure.set/union
                                     (set visited)
                                     (set [url])
                                     (set (map
                                           #(-> % :url)
                                           url-queue))))
                           :xpath-nav-info
                           (prepare (-> url-queue first :path)
                                    url))
                       (catch NullPointerException e nil))]
                 (do
                   (recur (concat (rest url-queue) new-bodies)
                          (clojure.set/union visited (set [url]))
                          lookahead
                          leaf?
                          extract
                          stop?
                          leaf-paths
                          leaf-limit
                          (let [corpus-entry {url
                                              {:body         body
                                               :state-action unaltered-state-action
                                               :leaf?        false
                                               :src-url      (-> url-queue
                                                                 first
                                                                 :src-url)
                                               :src-xpath    (-> url-queue
                                                                 first
                                                                 :path)}}]
                            (merge corpus corpus-entry))))))))))

(defn xpath-to-pick
  [src-path action-seq]
  (nth action-seq (count src-path)))

(defn pagination-to-pick
  "Pick the next page (sort-of)
   Do not pass in pagination for more than 1 query!!"
  [url-ds-seq]
  (utils/sayln url-ds-seq)
  (first (sort-by :text url-ds-seq)))

(defn pick-next-pagination-candidate
  "First, we pick the deepest page to paginate from.
   Then we pick a source page to start off of (sort-of)"
  ([pagination-candidates]
     (pick-next-pagination-candidate pagination-candidates
                                     nil))

  ([pagination-candidates current-src-url]
     (if current-src-url
       (pagination-to-pick
        (filter
         #(= (-> % :src-url current-src-url))
         pagination-candidates))

       (let [xpath-to-candidate (reduce
                                 (fn [acc x]
                                   (merge-with concat acc {(:src-xpath x)
                                                           [x]}))
                                 pagination-candidates)]
         (first
          (second
           (last
            (sort-by
             #(-> % first count)
             xpath-to-candidate))))))))

(defn crawl-model
  "Args:
   entry-point: starting url
   leaf? : is current URL a leaf
   extract: extractor
   stop? : should the crawler stop right now
   action-seq: which sequence of actions are we executing at the moment"
  ([entry-point leaf? stop? action-seq pagination]
     (crawl-model entry-point
                  leaf?
                  stop?
                  action-seq
                  pagination
                  []))
  
  ([entry-point leaf? stop? action-seq pagination blacklist]
     (crawl-model {:content-queue [{:url entry-point}]
                   :paging-queue []}
                  (set [])
                  0
                  leaf?
                  stop?
                  action-seq
                  pagination
                  blacklist
                  {}))

  ([{content-q :content-queue
     paging-q :paging-queue}
    visited
    num-leaves
    leaf?
    stop?
    action-seq
    pagination
    blacklist
    corpus]
     (let [url  (-> content-q first :url)
           body (utils/download-cache-with-cookie url)
           src-xpath (-> content-q first :src-xpath)]
       (do
         (Thread/sleep 1000)
         (utils/sayln :left (count content-q))
         (utils/sayln :paging (count paging-q))
         (utils/sayln :src-xpath src-xpath)
         (utils/sayln :cur-url url)
         (cond (or (stop? {:visited (count visited)
                           :num-leaves num-leaves})
                   (and (empty? content-q)
                        (empty? paging-q)))
               (do (utils/sayln :crawl-done)
                   {:corpus corpus
                    :num-leaves num-leaves
                    :visited visited})
               
               (leaf? action-seq (first content-q))
               (do
                 (utils/sayln :reached-leaf)
                 (let [pagination-extracted (if-not (pagination src-xpath)
                                              nil
                                              (reduce
                                               concat
                                               (map
                                                (fn [p]
                                                  (try
                                                    (pagination-to-pick
                                                     (first
                                                      (vals
                                                       (dom/eval-anchor-xpath
                                                        p
                                                        (dom/html->xml-doc body)
                                                        (-> content-q first :url)
                                                        (clojure.set/union
                                                         visited
                                                         (set (map :url content-q))
                                                         (set (map :url paging-q))
                                                         (set blacklist))))))
                                                    (catch Exception e [])))
                                                (pagination src-xpath))))]
                   (utils/sayln pagination-extracted)
                   (recur {:content-queue (if-not (empty? pagination-extracted)
                                            (cons
                                             {:url (:href pagination-extracted)
                                              :src-xpath src-xpath
                                              :src-url url
                                              :pagination true}
                                             (rest content-q))
                                            (rest content-q))
                           :paging-queue  paging-q}
                          (conj visited (-> content-q first :url))
                          (+ 1 num-leaves)
                          leaf?
                          stop?
                          action-seq
                          pagination
                          blacklist
                          (merge corpus {(-> content-q first :url)
                                         body}))))
               
               (and (empty? content-q)
                    (not
                     (empty? paging-q)))
               (let [sorted-pagination-entries (sort-by
                                                #(-> % :src-xpath count)
                                                paging-q)
                     chosen-entry (last sorted-pagination-entries)
                     remaining-pagination (filter
                                           (fn [x] (not= x chosen-entry))
                                           paging-q)]
                 (recur {:content-queue (concat [chosen-entry]
                                                content-q)
                         :paging-queue remaining-pagination}
                        visited
                        num-leaves
                        leaf?
                        stop?
                        action-seq
                        pagination
                        blacklist
                        corpus))
               
               :else
               (let [action (xpath-to-pick src-xpath action-seq)
                     extracted (try
                                (first
                                 (vals
                                  (dom/eval-anchor-xpath
                                   action
                                   (dom/html->xml-doc body)
                                   (-> content-q first :url)
                                   (clojure.set/union
                                    visited
                                    (set (map :url content-q))
                                    (set (map :url paging-q))
                                    (set blacklist)))))
                                (catch Exception e []))
                     pagination-extracted (if (pagination src-xpath)
                                            (reduce
                                             concat
                                             (map
                                              (fn [p]
                                                (try
                                                  (first
                                                   (vals
                                                    (dom/eval-anchor-xpath
                                                     p
                                                     (dom/html->xml-doc body)
                                                     (-> content-q first :url)
                                                     (clojure.set/union
                                                      visited
                                                      (set (map :url content-q))
                                                      (set (map :url paging-q))
                                                      (set blacklist)))))
                                                  (catch Exception e [])))
                                              (pagination src-xpath)))
                                            [])
                     pagination-extracted-hrefs (map :href pagination-extracted)
                     extracted-hrefs (map :href extracted)

                     new-content-q (concat (rest content-q)
                                           (map
                                            (fn [x]
                                              {:url x
                                               :src-xpath (cons action src-xpath)})
                                            extracted-hrefs))
                     new-paging-q (concat paging-q (map
                                                    (fn [x]
                                                      {:url x
                                                       :src-xpath src-xpath})
                                                    pagination-extracted-hrefs))]
                 (utils/sayln :items (count extracted-hrefs))
                 (recur {:content-queue new-content-q
                         :paging-queue new-paging-q}
                        (conj visited url)
                        num-leaves
                        leaf?
                        stop?
                        action-seq
                        pagination
                        blacklist
                        corpus)))))))

(defn prepare-example
  "Given a state action, this routine
   pares it down to as few examples as
   possible per XPath.

   The clustering is done using single
   linkage and XPath-text structural
   similarity"
  [xpaths-and-urls src-path url leaf?]
  (reduce
   (fn [acc {xpath :xpath hrefs-and-texts :hrefs-and-texts}]
     (let [stuff  (utils/distinct-by-key
                   (filter
                    identity
                    (map
                     (fn [{a-link :href text :text}]
                       (when-not (some #{a-link} (:visited acc))
                         {:url  a-link
                          :path (cons xpath src-path)
                          :src-url url
                          :src-text text}))                   
                     hrefs-and-texts))
                   :url)

           ;; sample 10 or 25% of the links (whichever is bigger)
           links-and-texts (utils/random-take
                            (max 10 (int (/ (count stuff)
                                            4)))
                            stuff)

           sampled-corpus  (doall
                            (map
                             (fn [x]
                               (do
                                 (Thread/sleep 2000)
                                 (utils/sayln :downloading (:url x))
                                 (merge x {:body (-> x :url utils/download-cache-with-cookie)})))
                             links-and-texts))
           
           clusters        (doall
                            (cluster/cluster
                             sampled-corpus
                             (fn [x y] (similarity/similar?
                                       (:body x)
                                       (:body y)))))
           examples        (map rand-nth clusters)

           leaf-paths      (frequencies
                            (map
                             :path
                             (filter
                              (fn [x]
                                (leaf? {:anchor-text (:src-text x)
                                        :src-url     (:url x)
                                        :body        (:body x)}))
                              sampled-corpus)))]
       (merge-with concat acc {:leaf-paths leaf-paths
                               :examples examples
                               :corpus   sampled-corpus})))
   {}
   xpaths-and-urls))

(defn crawl-example
  "Implementation of the example based scheduler
   that I thought of. The example based scheduler
   operates in the following way:

   1. first, look out from this page
   2. cluster by structural similarity
   3. pick 1 example (for now) from each
      cluster
   4. continue

   This prevents us from wasting time with a BFS
   and allows our crawl to not waste too much time
   beating about the exact same paths"

  ([entry-point leaf? extract stop?]
     (crawl-example entry-point 1 leaf? extract stop?))
  
  ([entry-point lookahead leaf? extract stop?]
     (let [body           (utils/download-with-cookie entry-point)
           body-queue-ele {:url  entry-point
                           :body body}]
       (crawl-example [body-queue-ele]
                      [entry-point]
                      lookahead
                      leaf?
                      extract
                      stop?
                      []
                      50
                      {})))
  
  ([url-queue visited lookahead leaf? extract stop? leaf-paths leaf-limit corpus]
     (do
       (Thread/sleep 1000)
       (utils/sayln :queue-size (count url-queue))
       (utils/sayln :visited (count visited))
       (utils/sayln :leaves-left leaf-limit)
       (utils/sayln :url (-> url-queue first :url))
       (utils/sayln :src-url (-> url-queue first :src-url))
       (utils/sayln :src-path (-> url-queue first :path))
       (let [url  (-> url-queue first :url)
             body (-> url-queue first :body)
             
             unaltered-state-action (try (extract body url {} [])
                                         (catch Exception e nil))
             
             blacklist (clojure.set/union (set (map :url url-queue))
                                          (set visited)
                                          (set [url]))]
         
         (cond (or (empty? url-queue)
                   (stop? {:visited    (count visited)
                           :leaf-left  leaf-limit
                           :body       body}))
               (do
                 (utils/sayln :crawl-done)
                 ;; {:state  {:url-queue  url-queue
                 ;;           :visited    visited
                 ;;           :lookahead  lookahead
                 ;;           :leaf-paths leaf-paths
                 ;;           :leaf-limit leaf-limit}
                 
                 ;;  :model  (frequencies leaf-paths)
                 
                 ;;  :corpus corpus
                 
                 ;;  :prefix (uri/host (uri/uri url))}
                 
                 )
               
               ;; this is an initial test.
               ;; I personally prefer performing the
               ;; leaf test right at the beginning
               :else
               (do (utils/sayln :continuing-crawl)
                   (let [state-action (extract body
                                               (first url-queue)
                                               {}
                                               blacklist)
                         prepared (prepare-example (:xpath-nav-info state-action)
                                                   (-> url-queue first :path)
                                                   url
                                                   leaf?)]
                     prepared)))))))

