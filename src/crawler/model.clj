(ns crawler.model
  "Contains code to read and process a
   sampled model"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crawler.corpus :as corpus]
            [crawler.rich-char-extractor :as extractor]
            [crawler.dom :as dom]
            [structural-similarity.xpath-text :as similarity])
  (:use [clojure.pprint :only [pprint]])
  (:import [java.io PushbackReader]))

(defn read-model
  [a-model-file]
  (with-open [rdr (io/reader a-model-file)]
    (-> rdr PushbackReader. read)))

(defn associated-corpus-file
  "Finds the corpus file that produced
   provided model file"
  [a-model-file]
  (string/replace a-model-file #".model" ".corpus"))

(defn associated-leaves-file
  "Finds the leaves file that produced
   provided model file"
  [a-model-file]
  (string/replace a-model-file #".model" ".leaves"))

(defn fix-model-restrictions-model-read
  [model a-corpus-file a-leaves-file]
  (let [downloaded-corpus (corpus/read-corpus-file a-corpus-file)
        leaf-cluster (last
                      (sort-by
                       count
                       (read-model a-leaves-file)))
        leaf-examples (map
                       (fn [l]
                         (-> l downloaded-corpus :body))
                       leaf-cluster)
        
        corpus-urls (set
                     (map
                      (fn [[u x]]
                        (-> x :src-url))
                      downloaded-corpus))
        
        ;; pagination refinements are trusted
        ;; always
        model-actions (:actions model)

        ;; actions-with-refinements (filter
        ;;                           (fn [{axns    :actions
        ;;                                refined :refined}]
        ;;                             (not
        ;;                              (empty?
        ;;                               (filter
        ;;                                (fn [[axn-pair refinement]]
        ;;                                  (not
        ;;                                   (or (nil? (:avoid refinement))
        ;;                                       (empty? (:avoid refinement))
        ;;                                       (nil? (:only refinement))
        ;;                                       (empty? (:only refinement)))))
        ;;                                refined))))
        ;;                           model-actions)
        ]
    (map
     (fn [{axns :actions
          refined :refined}]

;       (println :old-refined refined)
                                        ;       (println :new-refined
                                       ;       )
       (if (empty?
            (filter
             (fn [[axn-pair x]]
               (do (or
                    (not (empty? (:avoid x)))
                    (not (empty? (:only x))))))
             refined))
         (do ;(println :not-refining refined)
          {:actions axns
           :refined refined})
         (let [probed-refinement 
               (reduce
                (fn [acc [[src-xpath xpath-to-take] refinement]]
                  (let [docs-at-src-xpath (filter
                                           (fn [[u x]]
                                             (and (= (:path x)
                                                     src-xpath)
                                                  (some #{u} corpus-urls)))
                                           downloaded-corpus)
                        
                        action-to-take-at-target (if (= (inc (count src-xpath))
                                                        (count axns))
                                                   nil
                                                   (nth (reverse axns)
                                                        (inc
                                                         (count src-xpath))))
                        
                        new-refinements
                        
                        (map
                         (fn [[u x]]
                           (let [processed-doc (dom/html->xml-doc (:body x))]
                             (dom/probe-refinements-onlies
                              xpath-to-take
                              refinement
                              processed-doc
                              (fn [l bd]
                                (let [xpaths-hrefs (:xpath-nav-info
                                                    (extractor/state-action
                                                     bd
                                                     {:url l}
                                                     {}
                                                     []))]
                                  (not
                                   (empty?
                                    (filter
                                     (fn [{x :xpath}]
                                       (if action-to-take-at-target
                                         (= x action-to-take-at-target)
                                         (let [num-matched (count
                                                            (filter
                                                             (fn [e]
                                                               (similarity/similar? e bd))
                                                             leaf-examples))]
                                           (>= num-matched
                                               (/ (count leaf-examples)
                                                  2)))))
                                     xpaths-hrefs)))))
                              u)))
                         docs-at-src-xpath)]
                    ;; this is the new refinement
                    (merge
                     acc
                     {[src-xpath xpath-to-take]
                      (merge refinement
                             {:avoid
                              (first
                               (last
                                (sort-by
                                 second
                                 (frequencies new-refinements))))})})))
                {}
                (filter
                 (fn [[axn-pair refinement]]
                   (not
                    (and (nil? (:avoid refinement))
                         (empty? (:avoid refinement))
                         (nil? (:only refinement))
                         (empty? (:only refinement)))))
                 refined))]
           
           ;; now merge with the existing refinements
           (let [updated-refinements {:actions axns
                                      :refined (merge refined
                                                      probed-refinement)}]
             updated-refinements))))
     model-actions)))

(defn fix-model-restrictions
  "Inspects the only-avoid refinements
   and returns the newer only-avoid ideas.

   Focus on just avoids right now and trust
   the onlies.

   Actually not trusting the online too.
   Gotta fix that stuff"
  [a-model-file a-corpus-file a-leaves-file]
  (let [model (read-model a-model-file)]
    (fix-model-restrictions-model-read model
                                       a-corpus-file
                                       a-leaves-file)))
