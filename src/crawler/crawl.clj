(ns crawler.crawl
  "Initial crawl setup"
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [crawler.dom :as dom]
            [crawler.rich-extractor :as rich-extractor]
            [crawler.rich-char-extractor :as rich-char-extractor]
            [crawler.template-removal :as template-removal]
            [crawler.utils :as utils]
            [clj-http.cookies :as cookies]
            (org.bovinegenius [exploding-fish :as uri]))
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

(defn dump-state-model-corpus
  "Creates a dated file _prefix_-yr-month-day-hr-min.corpus/state/model"
  ([state model corpus]
     (dump-state-model-corpus state model corpus "crawler"))

  ([state model corpus prefix]
     (let [date-file-prefix (utils/dated-filename prefix "")

           create #(str date-file-prefix %)
           
           state-file  (create ".state")
           model-file  (create ".model")
           corpus-file (create ".corpus")]
       (with-open [state-wrtr  (io/writer state-file)
                   model-wrtr  (io/writer model-file)
                   corpus-wrtr (io/writer corpus-file)]
         (do (pprint state state-wrtr)
             (pprint model model-wrtr)
             (pprint corpus corpus-wrtr))))))

(defn prepare
  [xpaths-and-urls src-path url]
  (reduce
   (fn [acc {xpath :xpath hrefs :hrefs texts :texts}]
     (let [stuff  (distinct-by-key
                   (filter
                    identity
                    (map
                     (fn [a-link]
                       (when-not (some #{a-link} (:visited acc))
                         {:url  a-link
                          :path (cons xpath src-path)
                          :src-url url}))                   
                     hrefs))
                   :url)]
       (merge-with concat acc {:bodies  stuff
                               :visited (take
                                         (Math/ceil
                                          (/ (count hrefs) 4))
                                         hrefs)})))
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
                 (dump-state-model-corpus {:url-queue  url-queue
                                           :visited    visited
                                           :lookahead  lookahead
                                           :leaf-paths leaf-paths
                                           :leaf-limit leaf-limit}

                                          (frequencies leaf-paths)

                                          corpus

                                          (uri/host (uri/uri url))))

               ;; leaf reached. what do bruh
               (leaf? body)
               (do
                 (utils/sayln :leaf-reached)
                 (let [{new-bodies  :bodies
                        new-visited :visited}
                       (-> body
                           (extract (-> url-queue first :url)
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
                           (extract url
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


