(ns crawler.corpus
  "Tools for processing a downloaded corpus"
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [crawler.dom :as dom]
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

(defn refine-segment?
  [action-segment corpus]
  (let [action-to-prev (if (= [] (rest action-segment))
                         nil
                         (rest action-segment))
        action-taken   (first action-segment)

        prev-nodes     (filter
                        (fn [[u item]]
                          (= (:src-xpath item)
                             action-to-prev))
                        corpus)

        yield-nodes    (filter
                        (fn [[u item]]
                          (let [item-state-action (:xpath-nav-info
                                                   (extractor/state-action
                                                    (:body item)
                                                    {:url u}
                                                    {}))

                                possible-actions (map :xpath item-state-action)]
                            (some (fn [x] (= x action-taken)) possible-actions)))
                        prev-nodes)]
    (and action-taken
         (< (count yield-nodes)
            (count prev-nodes)))))

(defn refine-action
  "Args:
    src-page: The body of the source web-page
    dest-pages: A set of destination pages from the src-page at
                the specified action (plz don't supply more)
    action: The actiont taken (not the SRC-action!!!!)
    dest-page-test: A routine that accepts the source page
                    [url info] and the dest page [url info] data
                    structures

  Returns:
    An {:only :avoid} data structure to augment the XPath"
  [src-data dest-datas action src-url dest-page-test]
  (let [muscle (map
                first
                (filter
                 #(dest-page-test src-data %)
                 dest-datas))

        fat (map
             first
             (filter
              #(not
                (dest-page-test src-data %))
              dest-datas))]
    (dom/refine-xpath-accuracy action
                               (-> src-data second :body)
                               src-url
                               muscle
                               fat)))

(defn detect-pagination
  "Employs the simpler digit-based algorithm.
   Also refines the XPaths"
  [a-corpus]
  (let [digit-anchor-text (filter
                           (fn [[u x]]
                             (re-find #"^\d+$" (:src-text x)))
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
    {:actions (into {} action-and-pagination)
     
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
