(ns crawler.utils-test
  (:require [clojure.test :refer :all]
            [crawler.utils :refer :all]))

(deftest in-domain?-test
  (testing "Testing in-domain?"
    (is (and  (in-domain? "http://a/b/c" "/d")
            (in-domain? "http://a/b/c" "http://a/d")
            (not (in-domain? "http://a/b/c" "http://d/e"))))))