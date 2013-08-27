;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu
;;;;
;;;; Code to work with the DOM (XPath etc)

(ns crawler.dom
  (:require [clojure.string :as str]
            [crawler.core :as core])
  (:import (org.htmlcleaner HtmlCleaner)))

(defn process-page
  [page-src]
  (let [cleaner (new HtmlCleaner)]
    (.clean cleaner page-src)))

(defn anchor-tags
  "List of <a> tags on page"
  [page-src]
  (-> (process-page page-src)
     (.getElementsByName "a" true)))

(defn path-root-seq
  "A sequence of nodes from current node to root"
  ([a-tagnode]
     (path-root-seq a-tagnode []))
  
  ([a-tagnode cur-path]
     (let [parent (.getParent a-tagnode)]
       (if parent
         (recur parent (cons a-tagnode cur-path))
         (cons a-tagnode cur-path)))))

(defn format-attr
  "Cleans up the value of an id attribute"
  [attr-str]
  (try (str/replace attr-str #"\d+$" "")
       (catch Exception e nil)))

(defn tag-id-class
  "Returns a list containing a tag's name, its id
- formatted slightly - and its classes - formatted slightly"
  [a-tagnode]
  (let [silent-fail-split (fn [s regex] (try (str/split s regex)
                                            (catch Exception e nil)))]
   (list (.getName a-tagnode)
         (-> a-tagnode
            (.getAttributeByName "id")
            format-attr)
         (map
          format-attr
          (-> a-tagnode
             (.getAttributeByName "class")
             (silent-fail-split #"\s+"))))))

(defn tag-id-class->xpath
  "Args:
    a-tag-id-class : a tag, its id and a list of classes
   Returns:
    a component that fits in an xpath"
  [[tag id class-list]]
  (let [formatted-id    (format "contains(@id,'%s')" id)
        formatted-class (str/join
                         " or "
                         (map
                          #(format "contains(@class,'%s')" %)
                          class-list))]
    (cond (and id (not (empty? class-list))) (format "%s[%s and %s]"
                                    tag
                                    formatted-id
                                    formatted-class)

          (and id (empty? class-list)) (format "%s[%s]" tag formatted-id)

          (and (not (empty? class-list)) (not id)) (format "%s[%s]" tag formatted-class)

          :else tag)))

(defn tag-node->xpath
  [a-tagnode]
  (-> a-tagnode
     tag-id-class
     tag-id-class->xpath))

(defn tags->xpath
  "Given a sequence of tags, we construct an XPath
this basically means:
Args:
 html, body, a
Result:
 //html/body/a
id and class tag constraints are also added"
  [tag-nodes-seq]
  (str
   "//"
   (str/join
    "/"
    (map tag-node->xpath tag-nodes-seq))))

(defn anchor-tag-xpaths
  "Compute a list of paths to anchor tags"
  [page-src]
  (let [tags          (anchor-tags page-src)
        paths-to-root (map path-root-seq tags)]
    '*))

