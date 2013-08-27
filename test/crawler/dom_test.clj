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

(deftest tag-id-class-test
  (let [page-src       (slurp "resources/tag-id-class-test.html")
        anchor-tag-fst (first (anchor-tags page-src))
        [tag id class] (tag-id-class anchor-tag-fst)]
    (testing "tag-id-class : tags names"
      (is (= tag "a"))
      (is (= id "hello"))
      (is (= class ["hello" "class-tag"])))))

(deftest tag-id-class->xpath-test
  (let [page-src       (slurp "resources/tag-id-class-test.html")
        anchor-tag-fst (first (anchor-tags page-src))]
    (testing "tag-id-class->xpath"
      (is
       (=
        (tag-id-class->xpath (tag-id-class anchor-tag-fst))
        "a[contains(@id,'hello') and contains(@class,'hello') or contains(@class,'class-tag')]")))))

(deftest tags->xpath-test
  (let [page-src       (slurp "resources/tag-id-class-test.html")
        anchor-tag-fst (first (anchor-tags page-src))
        tag-nodes-seq  (path-root-seq anchor-tag-fst)]
    (is (= (str "//html/body/"
                "a[contains(@id,'hello')"
                " and contains(@class,'hello')"
                " or contains(@class,'class-tag')]")
           (tags->xpath tag-nodes-seq)))))