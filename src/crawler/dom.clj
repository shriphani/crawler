;;;; Soli Deo Gloria
;;;; Author: spalakod@cs.cmu.edu
;;;;
;;;; Code to work with the DOM (XPath etc)

(ns crawler.dom
  (:require [clojure.set :as clj-set] 
            [clojure.string :as str]
            [crawler.core :as core]
            [crawler.utils :as utils]
            [misc.dates :as dates])
  (:use [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]])
  (:import (org.htmlcleaner HtmlCleaner DomSerializer CleanerProperties)
           (org.w3c.dom Document)))

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
  (let [formatted-id      (format "contains(@id,'%s')" id)
        formatted-classes (map
                           #(format "contains(@class,'%s')" %)
                           class-list)]
    (cond (and id (not (empty? class-list))) 
          (map
           #(format "%s[%s and %s]"
                    tag
                    formatted-id
                    %)
           formatted-classes)

          (and id (empty? class-list)) 
          (list 
           (format "%s[%s]" tag formatted-id))

          (and (not (empty? class-list)) (not id)) 
          (map #(format "%s[%s]" tag %) formatted-classes)

          :else (list tag))))

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
  (reduce
   (fn [acc x]
     (map 
      #(str/join "/" %) 
      (utils/cross-product acc x)))
   ["/"]
   (map tag-node->xpath tag-nodes-seq)))

(defn anchor-tag-xpaths
  "Compute a list of paths to anchor tags"
  [page-src]
  (let [tags          (anchor-tags page-src)
        paths-to-root (map path-root-seq tags)]
    (reduce
     (fn [acc v] (merge-with +' acc {v 1}))
     {}
     (flatten
      (map tags->xpath paths-to-root)))))

(defn html->xml-doc
  "Take the html and produce an xml version"
  [page-src]
  (let [tag-node       (-> page-src
                           process-page)

        cleaner-props  (new CleanerProperties)

        dom-serializer (new DomSerializer cleaner-props)]
    
    (-> dom-serializer
        (.createDOM tag-node))))

(defn minimum-maximal-xpath-set
  "A maximal xpath set is a set of xpaths
sufficient to span all the anchor tags on a 
page. The minimum here refers to the cardinality
of the xpath set."
  [page-src]
  (let [a-tags        (anchor-tags page-src)

        ; forced to use links for maximal
        ; since the xpath lib doesn't like
        ; TagNode objects
        links         (set (map #(-> %
                                     (.getAttributeByName "href"))
                                a-tags))

        processed-pg  (html->xml-doc page-src)

        xpaths-a-tags (map
                       first
                       (reverse
                        (sort-by
                         second
                         (anchor-tag-xpaths page-src))))]
    (-> (reduce
         (fn [acc xpath]
           (let [xpath-links (set
                              (map #(-> % :attrs :href)
                                   ($x xpath processed-pg)))]
             (if (= (clj-set/intersection (:links acc) xpath-links)
                    xpath-links)
               acc
               (merge-with clj-set/union acc {:links  xpath-links
                                              :xpaths #{xpath}}))))
         {:xpaths (set [])
          :links  (set [])}
         xpaths-a-tags))))

(defn xpath-freq
  [xpath page-src]
  (let [processed (html->xml-doc page-src)]
    (count ($x xpath processed))))

(defn node-path-to-root
  "A version of path-root-seq for
org.w3c.dom.Node objects"
  ([a-node]
     (node-path-to-root a-node []))
  
  ([a-node cur-path]
     (let [parent (.getParentNode a-node)]
       (if parent
         (recur parent (cons a-node cur-path))
         (cons a-node cur-path)))))

(defn xpath->records
  "Nodes that are immediate children of the LCA 
of the nodes returned by an xpath.
Processed page is the output of html->xml-doc.
This routine is not correct and we don't really
care about its correctness for now"
  [xpath processed-page]
  (let [nodes ($x:node+ xpath processed-page)]
    (loop [cur-nodes nodes prev-nodes []]
      (let [potential-parent (reduce (fn [acc v]
                                       (if (and acc
                                                (.isSameNode acc v))
                                         acc
                                         nil))
                                     cur-nodes)]
        (if-not potential-parent
          (recur (map #(.getParentNode %) cur-nodes) cur-nodes)
          (reduce (fn [acc v] 
                    (if (empty? acc)
                      [v]
                      (if (.isSameNode (last acc) v)
                        acc
                        (conj acc v))))
                  []
                  prev-nodes))))))

(defn tooltips
  [a-node]
  ($x:text+ "//@title" a-node))

(defn relative-dates
  [a-node]
  (filter
   #(= (count %) 1)
   (map
    dates/dates-in-text
    (tooltips a-node))))

(defn neighborhood-text
  "Text from the neighborhood of the elements
returned by this xpath on our page"
  [xpath page-src]
  (map #(dates/dates-in-text
         (str/join
          " "
          (str/split (.getTextContent %) #"\s+")))
       (xpath->records xpath
                       (html->xml-doc page-src))))

(defn date-indexed-xpath?
  "Checks if the xpath returns objects
that are indexed by dates. This requires us to
get to the LCA of the xpath's nodes and "
  [xpath page-src]
  (let [num-records      (count 
                          (xpath->records 
                           xpath 
                           (html->xml-doc page-src)))

        records          (first
                          (map
                           #(list
                             (.getNodeName %)
                             (.getNamedItem (.getAttributes %) "id")
                             (.getNamedItem (.getAttributes %) "class"))
                           (xpath->records
                            xpath
                            (html->xml-doc page-src))))

        dategroups-found (count
                          (filter 
                           (fn [x] (> (count x) 0))
                           (neighborhood-text
                            xpath
                            page-src)))]
    (when-not (= num-records 0)
      {:ratio (/ dategroups-found num-records)
       :num-records num-records
       :records records})))

(defn xpaths-ranked
  [page-src]
  (let [xpaths (:xpaths (minimum-maximal-xpath-set page-src))]
    (filter
     #(> (:ratio (second %)) 0)
     (filter
      #(identity (second %))
      (map (fn [xpath] (list
                        xpath
                        (date-indexed-xpath? xpath page-src))) 
           xpaths)))))
