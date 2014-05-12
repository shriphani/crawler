(ns crawler.corpus
  "Tools for processing a downloaded corpus"
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [crawler.dom :as dom]
            [crawler.cluster :as cluster]
            [crawler.rich-char-extractor :as extractor]
            [structural-similarity.xpath-text :as xpath-text])
  (:use [clj-xpath.core :only [$x:node+]]
        [clojure.pprint :only [pprint]]
        [structural-similarity.xpath-text :only [similar? similarity-cosine-char-freq]])
  (:import [java.io PushbackReader]))

(defn read-corpus-file
  [a-corpus-file]
  (let [rdr (PushbackReader.
             (io/reader a-corpus-file))]
    (read rdr)))

(defn pagination-candidate?
  "Source page and target page bodies"
  [src-body target-body]
  (similar? src-body target-body))

(defn pagination-candidates
  [corpus]
  (map
   #(dissoc % :candidate?)
   (filter
    :candidate?
    (map
     (fn [[item-url item-details]]
       (let [src-body (-> item-details :src-url corpus :body)
             tgt-body (-> item-details :body)
             
             src-pg-xpath (-> item-details :src-url corpus :src-xpath)
             tgt-pg-xpath (-> item-details :src-xpath first)
             
             src-url (-> item-details :src-url)
             tgt-url item-url]
         {:src-xpath  src-pg-xpath
          :action     tgt-pg-xpath
          :candidate? (pagination-candidate? src-body tgt-body)}))
     corpus))))

(defn body-links
  [body]
  (let [processed (dom/html->xml-doc body)
        anchors   ($x:node+ ".//a" processed)]
    (set
     (map
      #(dom/node-attribute % "href")
      anchors))))

(defn pagination-in-corpus
  [corpus]
  (let [candidates (-> corpus pagination-candidates frequencies)
        sorted-candidates (sort-by #(-> % first :src-xpath count) candidates)

        ;; removes spurious leads with pagination.
        spurious-candidates-removed
        (filter
         (fn [[item freq]]
           (not
            (some (fn [[c freq]]
                    (= (:src-xpath item)
                       (cons (:action c)
                             (:src-xpath c))))
                  sorted-candidates)))
         sorted-candidates)]
    (reduce
     (fn [acc [{src-xpath :src-xpath action :action} n]]
       (merge-with concat acc {src-xpath [action]}))
     {}
     spurious-candidates-removed)))

(defn pagination-in-corpus-file
  [a-corpus-file]
  (let [corpus (read-corpus-file a-corpus-file)]
    (pagination-in-corpus corpus)))

(defn corpus->json
  "Converts a corpus file which is essentially a pprinted map
   to a json file that python or someone else can handle"
  [a-corpus-file]
  (let [wrtr (io/writer
              (clojure.string/replace a-corpus-file #".corpus" ".json"))]
     (json/generate-stream
      (read-corpus-file a-corpus-file)
      wrtr)))

(defn detect-pagination
  "Employs the simpler digit-based algorithm.
   Also refines the XPaths"
  [a-corpus]
  (let [digit-anchor-text (filter
                           (fn [[u x]]
                             (try (re-find #"^\d+$" (:src-text x))
                                  (catch Exception e nil)))
                           a-corpus)

        pagination-candidates (filter
                               (fn [[u x]]
                                 (let [src-url  (:src-url x)
                                       src-body (:body (a-corpus src-url))]
                                   (similar? src-body (:body x))))
                               digit-anchor-text)
        
        action-and-pagination (distinct
                               (map
                                (fn [[u x]]
                                  [(-> x :path rest) (-> x :path first)])
                                pagination-candidates))

        dest-page-test (fn [src-data dest-data]
                         (similar? (-> src-data second :body)
                                   (-> dest-data second :body)))]
    {:paging-actions (into {} action-and-pagination)
     
     :refine  (into
               {}
               (map
                (fn [[action-seq paging-action]]
                  (let [full-action-seq   (cons paging-action action-seq)
                        
                        docs-at-xpath     (filter
                                           (fn [[u x]]
                                             (= (:path x) full-action-seq))
                                           a-corpus)
                        
                        grouped-by-source (group-by
                                           (fn [[u x]]
                                             (:src-url x))
                                           docs-at-xpath)]
                    [[action-seq paging-action]
                     (first
                      (last
                       (sort-by
                        second
                        (frequencies
                         (map
                          (fn [[src-url xs]]
                            (refine-action [src-url (a-corpus src-url)]
                                           xs
                                           paging-action
                                           src-url
                                           dest-page-test))
                          grouped-by-source)))))]))
                action-and-pagination))}))

(defn refine-action-seq
  "Estimates the yield of a path from root to leaf"
  [an-action-seq corpus]
  (let [;; a list of steps from entry to discussion
        action-steps
        (rest
         (map
          reverse
          (reductions (fn [acc x]
                        (cons x acc))
                      []
                      (reverse an-action-seq))))]

    (reduce
     (fn [acc as]
       (let [docs-at-path (filter
                           (fn [[u x]]
                             (= (:path x)
                                as))
                           corpus)

             action-taken-at-doc (try (nth (reverse an-action-seq) (count as))
                                      (catch Exception e nil))

             sources (distinct
                      (map
                       #(-> % second :src-url)
                       docs-at-path))

             restrictions 
             (map
              (fn [a-source]
                (let [associated-docs (filter
                                       (fn [[u x]]
                                         (= (:src-url x)
                                            a-source))
                                       docs-at-path)
                      muscle (map
                              first
                              (filter
                               (fn [[u x]]
                                 (if action-taken-at-doc
                                   (let [state-action (:xpath-nav-info
                                                       (extractor/state-action (:body x)
                                                                               {:url u}
                                                                               {}
                                                                               []))
                                         record (filter
                                                 (fn [x]
                                                   (= action-taken-at-doc
                                                      (:xpath x)))
                                                 state-action)]
                                     (not (empty? record)))
                                   (:leaf x)))
                               associated-docs))

                      fat (map
                           first
                           (filter
                            (fn [[u x]]
                              (if action-taken-at-doc
                                (let [state-action (:xpath-nav-info
                                                    (extractor/state-action (:body x)
                                                                            {:url u}
                                                                            {}
                                                                            []))
                                      record (filter
                                              (fn [x]
                                                (= action-taken-at-doc
                                                   (:xpath x)))
                                              state-action)]
                                  (empty? record))
                                (not (:leaf x))))
                            associated-docs))]
                  (dom/refine-xpath-accuracy (last as)
                                             (:body (corpus a-source))
                                             a-source
                                             muscle
                                             fat)))
              sources)

             chosen-restriction (first
                                 (last
                                  (sort-by second (frequencies restrictions))))]
         (merge
          acc
          {[(if (empty? (rest as))
              nil
              (rest as))
            (first as)] chosen-restriction})))
     {}
     action-steps)))
