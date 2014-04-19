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
        child-nodes-cnt (.getLength child-node-list)
        child-nodes     (filter
                         (fn [a-node]
                           (and (= (.getNodeName a-node)
                                   (.getNodeName a-w3c-node))
                                (= (node-class a-node)
                                   (node-class a-w3c-node))))
                         (map
                          #(.item child-node-list %)
                          (range child-nodes-cnt)))]
    (.indexOf
     (map
      #(.isSameNode % a-w3c-node)
      child-nodes)
     true)))

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

        position          (child-position
                           (.getParentNode a-w3c-node)
                           a-w3c-node)]

    (list (.getNodeName a-w3c-node)
          
          (first
           (map
            format-attr
            (-> attributes
                (silent-fail-attr "class")
                (silent-fail-split #"\s+"))))

          position)))

(defn tag-id-class->xpath
  "Args:
    a-tag-id-class : a tag, its id and a list of classes
   Returns:
    a component that fits in an xpath"
  [[tag class-list position]]
  (let [formatted-class (first
                         (map
                          #(format "contains(@class,'%s')" %)
                          class-list))]
    

    (if (not (empty? class-list)) 
      (if (not position)    
        (format "%s[%s]" tag formatted-class)
        (format "%s[%s][%d]" tag formatted-class position))
      (if (not position)
        (list tag)
        (list (format "%s[%d]" tag position))))))

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

(defn w3c-node->ds
  [a-w3c-node]
  (-> a-w3c-node
      tag-id-class-node))

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

(defn nodes->ds
  [nodes-seq]
  (map w3c-node->ds nodes-seq))

(defn xpath-to-custom-root
  [a-node the-root]
  (let [path-to-root (path-root-seq-nodes-2 a-node the-root)]
    (nodes->xpath path-to-root)))

(defn xpath-to-node
  [a-node]
  (let [path-to-root (path-root-seq-nodes a-node)]
    (nodes->xpath path-to-root)))

(defn ds-to-node
  [a-node]
  (let [path-to-root (path-root-seq-nodes a-node)]
    (nodes->ds path-to-root)))

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

(defn page-nodes-hrefs-text
  "An intermediate step in producing a space of
   decisions. Returns the path to a node, the href
   attrib value and the associated text"
  ([a-processed-page url]
     (page-nodes-hrefs-text a-processed-page url (set [])))
  
  ([a-processed-page url blacklist]
     (let [a-tags         (try ($x:node+ ".//a" a-processed-page)
                               (catch Exception e nil)) ; anchor tags
           
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
           
           nodes-paths    (map
                           (fn [x]
                             [(-> x :node ds-to-node) x])
                           (filter
                            (fn [x]
                              (and (= (uri/host url) (-> x :href uri/host))
                                   (not
                                    (some #{(:href x)} (set blacklist)))))
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
       
       ;; (reduce
       ;;  (fn [acc [an-xpath node]]
       ;;    (merge-with
       ;;     concat acc {an-xpath [node]})) {} nodes-xpaths)
       nodes-paths)))

(defn path->xpath
  [path positions]
  (clojure.string/join
   "/"
   (cons
    "/"
    (map
     (fn [i]
       (let [[tag class pos] (nth path i)]
         (if (some #{i} (set positions))
           (if class
             (format "%s[contains(@class, '%s')][%d]" tag class (inc pos))
             (format "%s[%d]" tag (inc pos)))
           (if class
             (format "%s[contains(@class, '%s')]" tag class)
             (format "%s" tag)))))
     (range
      (count path))))))

(defn path->xpath-no-position
  [path]
  (clojure.string/join
   "/"
   (cons
    "/"
    (map
     (fn [i]
       (let [[tag class pos] (nth path i)]
         (if class
           (format "%s[contains(@class, '%s')]" tag class)
           (format "%s" tag))))
     (range
      (count path))))))

(defn generate-xpath
  "Generates an xpath for a set of nodes by incoporating
   position information"
  [[positions nodes]]
  (map
   (fn [[path a-node]]
     [(path->xpath path positions) a-node])
   nodes))

(defn xpaths-hrefs-tokens-no-position
  "Grouped only on element position and classes.
   No position information used in the XPath generated"
  ([processed-body url]
     (xpaths-hrefs-tokens-no-position processed-body url (set [])))

  ([processed-body url blacklist]
     (let [nodes-paths (page-nodes-hrefs-text processed-body
                                              url
                                              blacklist)

           xpaths-nodes-paths (map
                               (fn [[path an-info]]
                                 [(path->xpath-no-position path) an-info])
                               nodes-paths)]
       (reduce
        (fn [acc [xpath an-info]]
          (merge-with concat
                      acc
                      {xpath [an-info]}))
        {}
        xpaths-nodes-paths))))

(defn eval-anchor-xpath
  ([xpath processed-body url]
     (eval-anchor-xpath xpath processed-body url []))
  
  ([xpath processed-body url blacklist]
     (let [anchor-nodes ($x:node+ xpath processed-body)

           a-tags-hrefs (filter
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
                         anchor-nodes)

           nodes-paths  (map
                         (fn [x]
                           [(-> x :node ds-to-node) x])
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
                           a-tags-hrefs)))

           xpaths-nodes-paths (map
                               (fn [[path an-info]]
                                 [(path->xpath-no-position path) an-info])
                               nodes-paths)]
       (reduce
        (fn [acc [xpath an-info]]
          (merge-with concat
                      acc
                      {xpath [an-info]}))
        {}
        xpaths-nodes-paths))))

(defn refine-xpath-with-position
  [processed-body url xpath num-clusters]
  (let [nodes-paths (page-nodes-hrefs-text processed-body
                                           url
                                           [])

        to-refine   (filter
                     (fn [[p x]]
                       (= (path->xpath-no-position p)
                          xpath))
                     nodes-paths)
        
        paths       (map first to-refine)

        positions-list (map (fn [p]
                              (map #(nth % 2) p))
                            paths)
        positions-hist (map
                        #(-> % distinct count)
                        (apply map vector positions-list))

        candidates     (utils/positions-at-f
                        (map
                         #(if (or (= % 1)
                                  (< 2 (Math/abs
                                        (- num-clusters %))))
                            nil
                            (Math/abs
                             (- num-clusters %)))
                         positions-hist)
                        identity)]
    (map
     (fn [c]
       [c (map
           (fn [[p n]]
             [(path->xpath p [c]) n])
           to-refine)])
     candidates)))

(defn xpaths-hrefs-tokens-with-position
  "Utilize an initial class-based grouping.
   Then add position information in the XPath to
   achieve a finer grouping."
  ([processed-body url]
     (xpaths-hrefs-tokens-with-position processed-body url (set [])))

  ([processed-body url blacklist]
     (let [nodes-paths   (page-nodes-hrefs-text processed-body
                                                url
                                                blacklist)

           node-objs     (map
                          (fn [[path an-obj]]
                            {:path (map drop-last path)
                             :obj  [path an-obj]})
                          nodes-paths)

           ;; initial grouping is by style and dom position
           grouped-nodes (group-by :path node-objs)

           ;; next grouping is done using a position frequency
           ;; method. This tells us in each node-group, where to
           ;; insert positions
           positions      (map
                           (fn [a-group]
                             (let [objs           (map :obj (second a-group))
                                   
                                   path-length    (-> a-group first count)

                                   poss           (map
                                                   (fn [an-obj]
                                                     (map last
                                                          (first an-obj)))
                                                   objs)
                                   
                                   num-as         (count poss)
                                   
                                   positions-freq (into
                                                   [] (map
                                                       #(-> % distinct count)
                                                       (apply
                                                        map vector poss)))

                                   positions-inc  (filter
                                                   #(and (not= % 1)
                                                         (not= % num-as))
                                                   positions-freq)

                                   positions-max  (or nil (when (seq positions-inc) (apply max positions-inc)))

                                   accum-scr      (reverse
                                                   (into [] (reductions * (reverse positions-freq))))
                                   
                                   where          (utils/positions-at
                                                   (map
                                                    (fn [i]
                                                      (and
                                                       (not= (nth positions-freq i) 1)
                                                       (< (nth accum-scr i) num-as)
                                                       (not= (nth positions-freq i) positions-max)))
                                                    (range
                                                     (count accum-scr)))
                                                   true)]
                               where))
                           grouped-nodes)

           pos-path-nodes (map
                           vector
                           positions
                           (map
                            #(map
                              :obj
                              (second %))
                            grouped-nodes))

           xpath-nodes    (reduce
                           concat
                           (map generate-xpath
                                pos-path-nodes))]
       (reduce
        (fn [acc v]
          (merge-with concat acc {(first v) [(second v)]}))
        {}
        xpath-nodes))))

(defn refine-xpath-accuracy
  "Xpath refined to maximize yield on
   a per-page basis"
  [action body url muscle fat]
  (let [processed-body (html->xml-doc body)
        
        nodes-paths    (page-nodes-hrefs-text processed-body
                                              url
                                              [])
        nodes-paths-x  (filter
                        (fn [[path an-info]]
                          (= action (path->xpath-no-position path)))
                        nodes-paths)

        muscle-nodes   (filter
                        (fn [[path an-info]]
                          (some
                           (fn [x] (= (:href an-info)
                                     x))
                           muscle))
                        nodes-paths-x)

        fat-nodes      (filter
                        (fn [[path an-info]]
                          (some
                           (fn [x] (= (:href an-info)
                                     x))
                           fat))
                        nodes-paths-x)
        
        paths-list     (map first nodes-paths-x)

        histogram      (if-not (empty? paths-list)
                         (map
                          #(-> % distinct count)
                          (apply map vector paths-list))
                         [])

        positions      (filter
                        (fn [x]
                          (not=
                           (nth histogram x)
                           1))
                        (range (count histogram)))

        stuff (map #(nth histogram %) positions)

        muscle-paths-list (map first muscle-nodes)

        muscle-histogram (if (empty? muscle-paths-list)
                           []
                           (map
                            #(-> % distinct count)
                            (apply map vector muscle-paths-list)))
        
        muscle-positions (filter
                          (fn [x]
                            (not=
                             (nth muscle-histogram x)
                             1))
                          (range (count muscle-histogram)))
        
        fat-paths-list (map first fat-nodes)

        fat-histogram (if (empty? fat-paths-list)
                        []
                        (map
                         #(-> % distinct count)
                         (apply map vector fat-paths-list)))

        fat-positions (filter
                       (fn [x]
                         (not=
                          (nth fat-histogram x)
                          1))
                       (range (count fat-histogram)))]
    (do
      {:only (map
              (fn [p]
                [p (try
                    (nth 
                     (map
                      #(map
                        last
                        (distinct %))
                      (if (empty? muscle-paths-list)
                        []
                        (apply map vector muscle-paths-list)))
                     p)
                    (catch Exception e nil))])
              (filter
               (fn [p]
                 (not
                  (some (fn [mp]
                          (= mp p))
                       muscle-positions)))
               positions))

       :avoid (filter
               second
               (map
                (fn [p]
                  [p (try
                       (nth
                        (map
                         #(map
                           last
                           (distinct %))
                         (if (empty? fat-paths-list)
                           []
                           (apply map vector fat-paths-list)))
                        p)
                       (catch Exception e nil))])
                (filter
                 (fn [p]
                   (not
                    (some (fn [mp]
                            (= mp p))
                          fat-positions)))
                 positions)))})))
