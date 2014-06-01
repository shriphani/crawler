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
                                  (let [src-url (:src-url x)
                                        src-bod (:body
                                                 (a-corpus src-url))]
                                    [[(if (nil? x)
                                        nil
                                        (-> x :path rest))
                                      (-> x :path first)]
                                     (similarity-cosine-char-freq (:body x)
                                                                  src-bod)]))
                                pagination-candidates))

        dest-page-test (fn [src-data dest-data]
                         (similar? (-> src-data second :body)
                                   (-> dest-data second :body)))

        paging-actions (reduce
                        (fn [acc [[src-axn pg-axn] n]]
                          (cond (not
                                 ((:similarities acc)
                                  [src-axn pg-axn]))
                                (merge
                                 acc
                                 {:similarities (merge (:similarities acc)
                                                       {[src-axn pg-axn] n})
                                  :actions (merge (:actions acc)
                                                  {src-axn pg-axn})})
                                
                                (< ((:similarities acc)
                                    [src-axn pg-axn]))
                                (merge
                                 acc
                                 {:similarities (merge (:similarities acc)
                                                       {[src-axn pg-axn] n})
                                  :actions (merge
                                            (:actions acc)
                                            {src-axn pg-axn})})
                                
                                :else
                                acc))
                        {:similarities {}
                         :actions {}}
                        action-and-pagination)]
    {:paging-actions (:actions paging-actions)
     
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
                (:actions paging-actions)))}))

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
                                (reverse as)))
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
          {[(if (empty? (rest (reverse as)))
              nil
              (rest (reverse as)))
            (first (reverse as))] chosen-restriction})))
     {}
     action-steps)))

(defn model-steps
  "Produces a set of path sequences
   leading to the dest"
  [a-model]
  (cons
   nil
   (rest
    (reductions (fn [acc x]
                  (cons x acc))
                []
                (reverse a-model)))))

(defn estimate-yield
  "Args:
    a-model: a single path to leaf + refinements
    pagination: a dictionary of pagination actions
    corpus: the corpus to estimate yield at"
  [a-model pagination corpus]
  ;(println :corpus-size (count corpus))
  (let [path-seqs (model-steps                   
                   (:actions a-model))

        refinements (:refined a-model)]
    (reduce
     (fn [acc step]
       ;(println :acc acc)
       ;(println :step step)
       ;; (pprint
       ;;  (map
       ;;   (fn [[u x]]
       ;;     (:path x))
       ;;   (filter
       ;;    (fn [[u x]]
       ;;      (:leaf x))
       ;;    corpus)))
       (let [associated-docs (if (= (:parent-set acc) nil)
                               (filter
                                (fn [[u x]]
                                  (and
                                   (:leaf x)
                                   (= (:path x)
                                      step)))
                                corpus)
                               (filter
                                (fn [[u x]]
                                  (and
                                   (some #{u} (:parent-set acc))
                                   (= (:path x)
                                      step)))
                                corpus))

             parent-set (set
                         (map
                          (fn [[u x]]
                            (:src-url x))
                          associated-docs))
;             _ (println :docs (map first associated-docs))
;             _ (println :parents parent-set)
             
             action-to-take (try (nth (reverse (:actions a-model)) (count step))
                                 (catch Exception e nil))
             
             yield-at-step (if (nil? (:parent-set acc))
                             1
                             (try (apply
                                   max
                                   (map
                                    (fn [[u x]]
                                      (let [state-action  (:xpath-nav-info
                                                           (extractor/state-action-xpath
                                                            action-to-take
                                                            (or (try (refinements [step action-to-take])
                                                                     (catch Exception e nil))
                                                                {})
                                                            (:body x)
                                                            {:url u}
                                                            {}
                                                            []))
                                            
                                            yield-record  (count
                                                           (:hrefs-and-texts
                                                            (first
                                                             (filter
                                                              #(= (:xpath %) action-to-take)
                                                              state-action))))]
                                        yield-record))
                                    associated-docs))
                                  (catch Exception e 0)))

             yield-with-paging (if ((:paging-actions pagination) step)
                                 (* 10 yield-at-step)
                                 yield-at-step)]
         {:parent-set parent-set
          :yield (* (or (:yield acc) 1) yield-with-paging)}))
     {}
     (reverse path-seqs))))
