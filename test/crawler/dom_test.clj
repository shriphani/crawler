;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu
;;;;
;;;; Code to test with dom.clj

(ns crawler.dom-test
  (:require [clojure.test :refer :all]
            [crawler.dom :refer :all]))

(deftest anchor-tag-test
  (let [page-src (slurp "resources/anchor-tags-test.html")]
   (testing "anchor-tags"
     (is (= (count (anchor-tags page-src)) 4)))))

(deftest path-root-seq-test
  (let [page-src       (slurp "resources/anchor-tags-test.html")
        processed-page (process-page page-src)
        anchor-tag-fst (first (anchor-tags page-src))
        path-to-root   (path-root-seq anchor-tag-fst)]
    (testing "path-root-seq"
      (is (= (map #(.getName %) path-to-root)
             ["html" "body" "a"])))))
