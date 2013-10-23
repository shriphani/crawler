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
         (-> a-tagnode
            (.getAttributeByName "id")
            format-attr)
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

(defn tag-id-class-node
  "Returns a list containing a tag's name, its id
- formatted slightly - and its classes - formatted slightly"
  [a-w3c-node]
  (let [silent-fail-split (fn [s regex] (try (str/split s regex)
                                            (catch Exception e nil)))

        silent-fail-attr  (fn [attr key] (try (.getValue
                                              (.getNamedItem attr key))
                                             (catch Exception e nil)))
        
        attributes        (.getAttributes a-w3c-node)]

    (list (.getNodeName a-w3c-node)
          (-> attributes
              (silent-fail-attr "id")
              format-attr)
          (map
           format-attr
           (-> attributes
               (silent-fail-attr "class")
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
   (concat (map w3c-node->xpath (drop-last nodes-seq))
           (list (list (.getNodeName (last nodes-seq)))))))

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

(def file-format-ignore-regex #".jpg$|.css$|.gif$|.png$")

(defn minimum-maximal-xpath-set-processed
  "This helper routine exists so we don't reparse
the page several times"
  [processed-pg]
  (let [a-tags        ($x:node+ ".//a" processed-pg)

        href-nodes    (filter
                       identity (map #(-> %
                                          (.getAttributes)
                                          (.getNamedItem "href"))
                                     a-tags))
        
        links         (set (map #(.getNodeValue %) href-nodes))

        xpaths-a-tags (map
                       first
                       (reverse
                        (sort-by
                         second
                         (anchor-tag-xpaths-nodes a-tags))))]

    (:xpaths (-> (reduce
                  (fn [acc xpath]
                    (let [xpath-links (set
                                       (filter
                                        #(not (re-find file-format-ignore-regex %))
                                        (filter
                                         identity
                                         (map #(try (uri/fragment (-> % :attrs :href) nil)
                                                    (catch Exception e nil))
                                              ($x xpath processed-pg)))))]
                      (if (= (clj-set/intersection (:links acc) xpath-links)
                             xpath-links)
                        acc
                        (merge-with clj-set/union acc {:links  xpath-links
                                                       :xpaths #{xpath}}))))
                  {:xpaths (set [])
                   :links  (set [])}
                  xpaths-a-tags)))))

(defn minimum-maximal-xpath-set
  "A maximal xpath set is a set of xpaths
sufficient to span all the anchor tags on a 
page. The minimum here refers to the cardinality
of the xpath set."
  [page-src src-link]
  (let [processed (html->xml-doc page-src)
        xpaths    (minimum-maximal-xpath-set-processed processed)]
    (reduce
     (fn [acc [xpath hrefs]]
       (merge-with clojure.set/union acc {xpath hrefs}))
     {}
     (map
      vector
      xpaths
      (map
       (fn [xpath]
         (set
          (filter
           #(not (re-find file-format-ignore-regex %))
           (filter
            identity
            (map
             (fn [res]
               (try
                 (uri/resolve-uri
                  src-link
                  (uri/fragment
                   (-> res
                       :attrs
                       :href)
                   nil))
                 (catch Exception e nil)))
             (filter
              identity ($x xpath processed))))))) xpaths)))))

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
  (try
    ($x:text+ ".//@title" a-node)
    (catch RuntimeException e '())))

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
  [xpath processed-page]
  (map #(dates/dates-in-text
         (str/join
          " "
          (str/split (.getTextContent %) #"\s+")))
       (xpath->records xpath processed-page)))

(defn xpath-records-dates-processed-page
  [xpath processed-page]
  (let [num-records      (count 
                          (xpath->records 
                           xpath processed-page))

        dategroups-found (count
                          (filter 
                           (fn [x] (> (count x) 0))
                           (neighborhood-text xpath processed-page)))]
    
    (when-not (= num-records 0)
      {:ratio (/ dategroups-found num-records)
       :num-records num-records})))

(defn xpath-records-dates
  "An xpath returns nodes. We check how many of these
result in nodes that are indexed by a clear date"
  [xpath processed-pg]
  (xpath-records-dates-processed-page xpath processed-pg))

(defn xpaths-ranked
  ([page-src]
     (xpaths-ranked page-src 0.70))
  ([page-src ratio]
     (let [processed-pg (html->xml-doc page-src)
           xpaths       (:xpaths (minimum-maximal-xpath-set-processed processed-pg))]
       (reverse
        (sort-by
         (fn [x] (:num-records (second x)))
         (filter
          #(> (:ratio (second %)) ratio)
          (filter
           #(identity (second %))
           (map (fn [xpath]
                  (list
                   xpath
                   (xpath-records-dates xpath processed-pg))) 
                xpaths))))))))

(defn same-node-set?
  "Checks if the nodes returned are the same"
  [node-set1 node-set2]
  (reduce
   (fn [out-acc a-node]
     (and
      out-acc
      (reduce
       (fn [acc another-node]
         (or acc (.isEqualNode a-node another-node)))
       false
       node-set2)))
   true
   node-set1))

(defn xpath-nodes
  [xpath page-src]
  (let [processed (html->xml-doc page-src)]
    ($x:node+ xpath processed)))

(defn equal-xpaths?
  "Xpaths are equal either if the strings are the same
or the returned records/nodes in the page are the same"
  [xpath1 xpath2 page-src]
  (or (= xpath1 xpath2)
      (same-node-set? (xpath-nodes xpath1 page-src)
                      (xpath-nodes xpath2 page-src))))

(defn resolve-anchor-nodes
  ([xpath node]
     (resolve-anchor-nodes xpath node node))
  ([xpath node stored]
   (let [parent-node                (.getParentNode node)
         anchor-children            ($x:node+ ".//a" parent-node)
         xpaths-to-anchor-children  (reduce
                                     (fn [acc x]
                                       (merge-with + acc {x 1}))
                                     {}
                                     (map #(first (xpath-to-node %)) anchor-children))]

     (cond (> (xpaths-to-anchor-children xpath) 1)
           [(first (xpath-to-node node)) node]

           (= (.getNodeName parent-node) "#document")
           [xpath stored]

           :else
           (recur xpath parent-node stored)))))

(defn minimum-maximal-xpath-records
  "Given a set of xpaths, we compute the xpaths that
are sufficient to generate all the distinct records"
  [xpaths processed-page]

  (let [xpath-nodes      (map #($x:node+ % processed-page) xpaths)
        xpaths-nodes-map (into {} (map vector xpaths xpath-nodes))
        potential-recs   (map (fn [[xpath nodes]]
                                (map (fn [node]
                                       (resolve-anchor-nodes xpath node)) nodes))
                              xpaths-nodes-map)]
    (into {}
          (map
           (fn [[xpath nodes]]
             [xpath (distinct nodes)])
           (reduce
            (fn [acc [xpath node]]
              (merge-with concat acc {xpath [node]}))
            {}
            (partition 2 (flatten potential-recs)))))))

(defn date-indexed-records-filter
  "records-sets: returned by minimum-maximal-xpath-records"
  [records-sets]
  (first
   (filter
    #(> (count %) 5)
    records-sets)))

(defn node-text
  "Text = text nodes + tooltips"
  [a-node]
  (apply
   str
   (.getTextContent a-node)
   (str/join " " (tooltips a-node))))
