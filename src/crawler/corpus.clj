(ns crawler.corpus
  "Tools for processing a downloaded corpus"
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [crawler.dom :as dom]
            [crawler.rich-char-extractor :as extractor]
            [structural-similarity.xpath-text :as xpath-text])
  (:use [clj-xpath.core :only [$x:node+]]
        [clojure.pprint :only [pprint]]
        [structural-similarity.xpath-text :only [similar?]])
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

(defn seqs-to-refine
  [action-seq corpus]
  (let [action-segments (reductions
                         (fn [acc x]
                           (cons x acc))
                         nil
                         (reverse action-seq))]
    ;; the first one is a nil action at a nil
    ;; point
    (filter
     second
     (map
      (fn [segment]
        [segment (try (nth action-seq (count segment))
                      (catch IndexOutOfBoundsException e nil))])
      (filter
       (fn [segment] (refine-segment? segment corpus))
       action-segments)))))

(defn leaf-fix?
  [action-seq corpus]
  (let [documents (filter
                   (fn [[u x]]
                     (= (:src-xpath x)
                        action-seq))
                   corpus)

        leaves (filter
                (fn [[u x]]
                  (:leaf? x))
                documents)]
    (< (count leaves)
       (count documents))))

(defn repair-leaf-fuck-up
  "Try to repair the leaf node yield
   rate along the current action seq

   muscle URLs we want
   fat URLs we don't"
  [action-seq corpus muscle fat]
  (let [documents (filter
                   (fn [[u x]]
                     (and (:leaf? x)
                          (= (:src-xpath x)
                             action-seq)))
                   corpus)

        src-urls (map
                  (fn [[u x]]
                    (:src-url x))
                  documents)

        src-docs (map
                  #(corpus %)
                  src-urls)

        action-taken (first action-seq)]
    (map
     (fn [[u x]]
      (dom/refine-xpath
       action-taken
       (:body x)
       u
       muscle
       fat))
     src-docs)))

(defn refine-segment
  [segment action-to-take corpus]
  (let [muscle (map
                first
                (filter
                 (fn [[u x]]
                   (and
                    (if (:body x)
                      (let [action-space (map
                                          first
                                          (:xpath-nav-info
                                           (extractor/state-action
                                            (:body x)
                                            {:url u}
                                            {})))]
                        (some
                         (fn [x]
                           (= x action-to-take))
                         action-space))
                      false)
                    (= (:src-xpath x)
                       segment)))
                 corpus))
        fat     (filter
                 (fn [[u x]]
                   (and
                    (if (:body x)
                      (let [action-space (map
                                          first
                                          (:xpath-nav-info
                                           (extractor/state-action
                                            (:body x)
                                            {:url u}
                                            {})))]
                        (not
                         (some
                          (fn [x]
                            (= x action-to-take))
                          action-space)))
                      true)
                    (= (:src-xpath x)
                       segment)))
                 corpus)
        
        docs    (map
                 (fn [[u x]]
                   [u (:body x)])
                 (filter
                  (fn [[u x]]
                    (= (:src-xpath x)
                       segment))
                  corpus))]
    (first
     (first
      (max-key
       second
       (frequencies
        (map
         (fn [[u  b]]
           (dom/refine-xpath-accuracy segment
                                      b
                                      u
                                      muscle
                                      fat))
         docs)))))))

(defn refine-model-with-positions
  "Try to maximize the model yield with position info"
  [model corpus]
  (map
   (fn [[action-seq count]]
     (let [leaf-fix-decision (leaf-fix? action-seq corpus)

           leaf-muscle (map
                        first
                        (filter
                         (fn [[u x]]
                           (and (:leaf? x)
                                (= (:src-xpath x)
                                   action-seq)))
                         corpus))

           leaf-fat    (map
                        first
                        (filter
                         (fn [[u x]]
                           (and (not (:leaf? x))
                                (= (:src-xpath x)
                                   action-seq)))
                         corpus))

           leaf-src-urls (map
                          (fn [[u x]]
                            (:src-url x))
                          (filter
                           (fn [[u x]]
                             (= (:src-xpath x)
                                action-seq))
                           corpus))

           leaf-src-docs (map
                          (fn [u]
                            (:body (corpus u)))
                          leaf-src-urls)

           leaf-fix (first
                     (first
                      (max-key
                       second
                       (frequencies
                        (map
                         (fn [[u b]]
                           (if leaf-fix-decision
                             (dom/refine-xpath-accuracy (first action-seq)
                                                        b
                                                        u
                                                        leaf-muscle
                                                        leaf-fat)
                             {}))
                         (map vector leaf-src-urls leaf-src-docs))))))]
       {:action action-seq
        :segments-to-fix
        (cons
         [action-seq leaf-fix]
         (map
          (fn [[segment action]]
            (refine-segment segment action corpus))
          (seqs-to-refine action-seq
                          corpus)))
        :count count}))
   model))

(defn refine-pagination-with-positions
  "Pagination is a 2-tuple from an action seq
   to a pagination action you take at that point

   as a result the muscle is the portions marked by
   the crawler as pagination from a source link.

   the fat is the stuff that didn't make the cut"
  [action-seq paging-action corpus]
  (let [src-docs (filter
                  (fn [[u x]]
                    (= (:src-xpath x)
                       action-seq))
                  corpus)

        paging-docs (map
                     (fn [[u x]]
                       (let [actions (:xpath-nav-info
                                      (extractor/state-action (:body x)
                                                              {:url u}
                                                              {}))
                             links   (map
                                      (fn [l]
                                        [l (corpus l)])
                                      (first
                                       (map
                                        :hrefs
                                        (filter
                                         #(= (:xpath %)
                                             paging-action)
                                         actions))))

                             muscle  (map
                                      first
                                      (filter
                                       (fn [[u x2]]
                                         (and (:body x2) (xpath-text/similar? (:body x2)
                                                                              (:body x))))
                                       links))

                             fat     (map
                                      first
                                      (filter
                                       (fn [[u x2]]
                                         (or (nil? x2)
                                             (not (xpath-text/similar? (:body x2)
                                                                       (:body x)))))
                                       links))]
                         (do
                           (println :muscle muscle)
                           (println :fat fat)
                           (dom/refine-xpath-accuracy paging-action (:body x) u muscle fat))))
                     src-docs)]
    [paging-action
     (first
      (first
       (max-key
        second
        (frequencies paging-docs))))]))
