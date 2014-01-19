;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu
;;;;
;;;; Code to work with the DOM (XPath etc)

(ns crawler.dom
  (:require [clj-time.core :as core-time]
            [clojure.set :as clj-set] 
            [clojure.string :as str]
            [crawler.core :as core]
            [crawler.utils :as utils]
            [misc.dates :as dates]
            [org.bovinegenius [exploding-fish :as uri]])
  (:use [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]])
  (:import (org.htmlcleaner HtmlCleaner DomSerializer CleanerProperties)
           (org.w3c.dom Document)
           (org.apache.commons.lang StringEscapeUtils)))

(defn process-page
  [page-src]
  (let [cleaner (new HtmlCleaner)
        props   (.getProperties cleaner)
        _       (.setPruneTags props "script, style")
        _       (.setOmitComments props true)]
    (.clean cleaner page-src)))

(defn anchor-tags
  "List of <a> tags on page"
  [page-src]
  (-> (process-page page-src)
     (.getElementsByName "a" true)))

(defn node-children
  [a-node]
  (let [child-node-list (.getChildNodes a-node)
        child-nodes-cnt (.getLength child-node-list)]
    (map #(.item child-node-list %) (range child-nodes-cnt))))

(defn node-siblings
  [a-node]
  (let [left-siblings (take-while identity (iterate (fn [x] (.getNextSibling x)) a-node))
        right-sibs    (take-while identity (iterate (fn [x] (.getPreviousSibling x)) a-node))]
    (concat left-siblings right-sibs)))

(defn node-attribute
  [a-node attr]
  (-> a-node
      (.getAttributes)
      (.getNamedItem attr)
      (.getValue)))

(defn path-root-seq
  "A sequence of nodes from current node to root"
  ([a-tagnode]
     (path-root-seq a-tagnode []))
  
  ([a-tagnode cur-path]
     (let [parent (.getParent a-tagnode)]
       (if parent
         (recur parent (cons a-tagnode cur-path))
         (cons a-tagnode cur-path)))))

(defn contained-anchor-tags
  [a-node]
  ($x:node+ ".//a" a-node))

(defn path-root-seq-nodes
  "Operates on node objects"
  ([a-w3c-node]
     (path-root-seq-nodes a-w3c-node []))

  ([a-w3c-node cur-path]
     (let [parent (.getParentNode a-w3c-node)]
       (if (not= (.getNodeName parent) "#document")
         (recur parent (cons a-w3c-node cur-path))
         (cons a-w3c-node cur-path)))))

(defn path-root-seq-nodes-2
  "FIX"
  ([a-w3c-node a-root]
     (path-root-seq-nodes a-w3c-node []))

  ([a-w3c-node a-root cur-path]
     (let [parent (.getParentNode a-w3c-node)]
       (if (and
            (not (.isSameNode a-root parent))
            (not= (.getNodeName parent) "#document"))
         (recur parent a-root (cons a-w3c-node cur-path))
         (cons a-w3c-node cur-path)))))

(defn distance
  [node1 node2]
  (let [path-to-root1 (reverse (path-root-seq-nodes node1))
        path-to-root2 (reverse (path-root-seq-nodes node2))]
    (.indexOf
     (for [x path-to-root1
           y path-to-root2]
       (.isSameNode x y))
     true)))

(defn format-attr
  "Cleans up the value of an id attribute"
  [attr-str]
  (try (first
        (str/split
         (str/replace attr-str #"\d+$" "")
         #"-|_"))
       (catch Exception e nil)))

(defn tag-id-class
  "Returns a list containing a tag's name, its id
- formatted slightly - and its classes - formatted slightly"
  [a-tagnode]
  (let [silent-fail-split (fn [s regex] (try (str/split s regex)
                                            (catch Exception e nil)))]
   (list (.getName a-tagnode)
         nil
         (map
          format-attr
          (-> a-tagnode
             (.getAttributeByName "class")
             (silent-fail-split #"\s+"))))))

(defn node-class
  "Value of the class attribute of a node"
  [a-node]
  (try (-> a-node
           (.getAttributes)
           (.getNamedItem "class")
           (.getValue))
       (catch Exception e nil)))

(defn child-position
  [parent a-w3c-node]
  (let [child-node-list (.getChildNodes parent)
        child-nodes-cnt (.getLength child-node-list)]
    (.indexOf
     (map
      #(.isSameNode
        a-w3c-node
        (.item child-node-list %))
      (range child-nodes-cnt))
     true)))

(defn is-first-child?
  [a-w3c-node]
  (let [parent (.getParentNode a-w3c-node)]
    (and parent
         (= 0 (child-position
               parent a-w3c-node)))))

(defn is-last-child?
  [a-w3c-node]
  (let [parent (.getParentNode a-w3c-node)]
    (and parent
         (=
          (-
           (.getLength
            (.getChildNodes parent))
           1)
          (child-position
           parent a-w3c-node)))))

(defn tag-id-class-node
  "Returns a list containing a tag's name, its id
- formatted slightly - and its classes - formatted slightly"
  [a-w3c-node]
  (let [silent-fail-split (fn [s regex] (try (str/split s regex)
                                            (catch Exception e nil)))

        silent-fail-attr  (fn [attr key] (try (.getValue
                                              (.getNamedItem attr key))
                                             (catch Exception e nil)))

        
        attributes        (.getAttributes a-w3c-node)

        is-first?         (is-first-child? a-w3c-node)

        is-last?          (is-last-child? a-w3c-node)]

    (list (.getNodeName a-w3c-node)               ; name
          (-> attributes
              (silent-fail-attr "id")
              format-attr)                        ; id
          (map
           format-attr
           (-> attributes
               (silent-fail-attr "class")
               (silent-fail-split #"\s+")))       ; class

          is-first?
          
          is-last?)))    

(defn tag-id-class->xpath
  "Args:
    a-tag-id-class : a tag, its id and a list of classes
   Returns:
    a component that fits in an xpath"
  [[tag id class-list is-first? is-last?]]
  (let [formatted-id      (format "contains(@id,'%s')" id)
        formatted-classes (map
                           #(format "contains(@class,'%s')" %)
                           class-list)]
    

    (if (not (empty? class-list)) 
      (map #(cond (and (not is-first?)
                       (not is-last?))             
                  (format "%s[%s]" tag %)

                  is-first?
                  (format "%s[%s][1]" tag %)

                  is-last?
                  (format "%s[%s][last()]" tag %)) formatted-classes)
      (cond (and (not is-first?)
                 (not is-last?))
            (list tag)

            is-first?
            (list (format "%s[1]" tag))

            is-last?
            (list (format "%s[last()]" tag))))))

(defn tag-node->xpath
  [a-tagnode]
  (-> a-tagnode
      tag-id-class
      tag-id-class->xpath))

(defn w3c-node->xpath
  [a-w3c-node]
  (-> a-w3c-node
      tag-id-class-node
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
  (reduce
   (fn [acc x]
     (map 
      #(str/join "/" %) 
      (utils/cross-product acc x)))
   ["/"]
   (map tag-node->xpath tag-nodes-seq)))

(defn nodes->xpath
  [nodes-seq]
  (reduce
   (fn [acc x]
     (map 
      #(str/join "/" %) 
      (utils/cross-product acc x)))
   ["/"]
   (map w3c-node->xpath nodes-seq)))

(defn xpath-to-custom-root
  [a-node the-root]
  (let [path-to-root (path-root-seq-nodes-2 a-node the-root)]
    (nodes->xpath path-to-root)))

(defn xpath-to-node
  [a-node]
  (let [path-to-root (path-root-seq-nodes a-node)]
    (nodes->xpath path-to-root)))

(defn anchor-tag-xpaths-nodes
  [nodes]
  (let [paths-to-root (map path-root-seq-nodes nodes)]
    (reduce
     (fn [acc v] (merge-with +' acc {v 1}))
     {}
     (flatten
      (map nodes->xpath paths-to-root)))))

(defn anchor-tag-xpaths-tags
  [tags]
  (let [paths-to-root (map path-root-seq tags)]
    (reduce
     (fn [acc v] (merge-with +' acc {v 1}))
     {}
     (flatten
      (map tags->xpath paths-to-root)))))

(defn anchor-tag-xpaths
  "Compute a list of paths to anchor tags"
  [page-src]
  (let [tags          (anchor-tags page-src)]
    (anchor-tag-xpaths-tags tags)))


(defn html->xml-doc
  "Take the html and produce an xml version"
  [page-src]
  (let [tag-node       (-> page-src
                           process-page)

        cleaner-props  (new CleanerProperties)

        dom-serializer (new DomSerializer cleaner-props)]
    
    (-> dom-serializer
        (.createDOM tag-node))))

(def file-format-ignore-regex #".jpg$|.css$|.gif$|.png$|.xml$")

(defn xpaths-hrefs-tokens
  "Args:
    a-processed-page: a processed page
    url: a url
    "
  ([a-processed-page url]
     (xpaths-hrefs-tokens a-processed-page url (set [])))
  
  ([a-processed-page url blacklist]
     (let [a-tags         ($x:node+ ".//a" a-processed-page) ; anchor tags
           
           a-tags-w-hrefs (filter
                                        ; the node must have a href
                           (fn [a-tag]
                             (and (-> a-tag
                                      (.getAttributes)
                                      (.getNamedItem "href"))
                                  (try
                                    (not=
                                     (-> a-tag
                                         (.getAttributes)
                                         (.getNamedItem "rel")
                                         (.getValue))
                                     "nofollow")
                                    (catch NullPointerException e true))
                                  
                                  (not=
                                   (uri/scheme
                                    (-> a-tag
                                        (.getAttributes)
                                        (.getNamedItem "href")
                                        (.getValue)))
                                   "javascript")
                                  (not
                                   (some
                                    #{(uri/resolve-uri
                                       url
                                       (-> a-tag
                                           (.getAttributes)
                                           (.getNamedItem "href")
                                           (.getValue)))}
                                    (set blacklist)))))
                           a-tags)
           
           nodes-xpaths   (map
                           (fn [x]
                             [(-> x :node xpath-to-node first) x])
                           (filter
                            (fn [x]
                              (= (uri/host url) (-> x :href uri/host)))
                            (map
                             (fn [x]
                               (let [link (-> x
                                              (.getAttributes)
                                              (.getNamedItem "href")
                                              (.getValue))]
                                 {:node x
                                  :href (try
                                          (uri/fragment
                                           (uri/resolve-uri url link)
                                           nil)
                                          (catch Exception e nil))
                                  :text (.getTextContent x)}))
                             a-tags-w-hrefs)))]
       
       (reduce
        (fn [acc [an-xpath node]]
          (merge-with
           concat acc {an-xpath [node]})) {} nodes-xpaths))))
