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

(defn path-root-seq-nodes
  "Operates on node objects"
  ([a-w3c-node]
     (path-root-seq-nodes a-w3c-node []))

  ([a-w3c-node cur-path]
     (let [parent (.getParentNode a-w3c-node)]
       (if parent
         (recur parent (cons a-w3c-node cur-path))
         (cons a-w3c-node cur-path)))))

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
   (map w3c-node->xpath nodes-seq)))

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
  [xpath page-src]
  (map #(dates/dates-in-text
         (str/join
          " "
          (str/split (.getTextContent %) #"\s+")))
       (xpath->records xpath
                       (html->xml-doc page-src))))

(defn xpath-recods-dates
  "An xpath returns nodes. We check how many of these
result in nodes that are indexed by a clear date"
  [xpath page-src]
  (let [num-records      (count 
                          (xpath->records 
                           xpath 
                           (html->xml-doc page-src)))

        dategroups-found (count
                          (filter 
                           (fn [x] (> (count x) 0))
                           (neighborhood-text
                            xpath
                            page-src)))]
    (when-not (= num-records 0)
      {:ratio (/ dategroups-found num-records)
       :num-records num-records})))

(defn xpaths-ranked
  [page-src]
  (let [xpaths (:xpaths (minimum-maximal-xpath-set page-src))]
    (reverse
     (sort-by
      (fn [x] (:num-records (second x)))
      (filter
       #(> (:ratio (second %)) 0.70)
         (filter
          #(identity (second %))
          (map (fn [xpath]
                 (list
                  xpath
                  (xpath-recods-dates xpath page-src))) 
               xpaths)))))))

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

(defn minimum-maximal-xpath-records
  "Given a set of xpaths, we compute the xpaths that
are sufficient to generate all the date-indexed records
on the page that we are interested in.
The returned item is the record nodes"
  [xpaths page-src]
  (first ;;; big assumption here.!!!! FIX
   (map
    #(xpath->records % (html->xml-doc page-src))
    (:xpaths
     (let [xml-doc (html->xml-doc page-src)]
       (reduce
        (fn [acc v]
          (let [v-records (xpath->records v xml-doc)]
            (if (some #(same-node-set? v-records %)
                      (:record-sets acc))
              acc
              (merge-with concat acc {:xpaths [v] :record-sets [v-records]}))))
        {:xpaths      []
         :record-sets []}
        xpaths))))))

(defn node-text
  "Text = text nodes + tooltips"
  [a-node]
  (apply
   str
   (.getTextContent a-node)
   (str/join " " (tooltips a-node))))

(defn inner-xml
  "Returns a string representation
of a node. This is an imperfect representation of
the result of innerHTML() in javascript."
  [a-node]
  (let [ls-implementation (-> a-node
                              (.getOwnerDocument)
                              (.getImplementation)
                              (.getFeature "LS" "3.0"))

        ls-serializer     (.createLSSerializer ls-implementation)

        child-nodes       (.getChildNodes a-node)]

    (map
     (fn [i]
       (.writeToString
        ls-serializer
        (.item child-nodes i)))
     (range (.getLength child-nodes)))))

(defn dates-neighboring-texts
  "The dategroups list contains dategroups
that natty has found in our time-range. This
is then grepped for in the XML and we try
to find some patterns.
i and j are used to tweak how far we consider
substrings in the supplied html"
  [xml dategroups i j]
  (filter
   identity
   (map
    (fn [text]
      (let [start (.indexOf xml text)]
        (try
          (list (subs
                 xml
                 (- start i)
                 start)
                (subs
                 xml
                 (+ start (- (count text) 1))
                 (+ start (- (count text) 1) j)))
          (catch Exception e nil))))
    (filter
     (fn [text] (> (count text) 3)) ; more than 3 chars per date
     (map #(.getText %) dategroups)))))

(defn node-set-dates
  [node-set]
  (map
   identity
   (map list
        (map #(apply str (inner-xml %)) node-set)
        (map #(dates/dategroups-in-text
               (node-text %)) node-set))))

(defn date-pattern
  [node-set left-bdry right-bdry]
  (let [xml-dategroups (node-set-dates node-set)]
    
    
    (sort-by
     second
     (reduce
      (fn [acc v]
        (merge-with +' acc (reduce merge (map (fn [x] {x 1}) v))))
      {}
      (map
       (fn [[xml dategroups]]
         (dates-neighboring-texts xml dategroups left-bdry right-bdry))
       xml-dategroups)))))

(defn find-date-pattern
  "We are given a set of date-indexed records.
We return a string path that tells us what the date
indexed records are"
  [node-set]
  (-> (filter
       #(= (count node-set)
           (second (last %)))
       (for [left-bdry  (range 1 7)
             right-bdry (range 1 7)]
         (date-pattern node-set left-bdry right-bdry)))
      last
      last))

(defn extract-dates
  [node-set date-pattern]
  (let [[date-start date-end] date-pattern
        xml-dategroups        (node-set-dates node-set)]
    (map
     vector
     node-set
     (map
      (fn [[xml date-groups]]
        (-> (re-pattern (format "%s(.*)%s"
                                (utils/str->pattern date-start)
                                (utils/str->pattern date-end)))
            (re-find xml)
            second
            dates/dates-in-text
            first))
      xml-dategroups))))

(defn site-model
  [xpaths records page-src]
  {:date-pattern (first (find-date-pattern records))})

(defn date-indexed-records
  [page-src]
  (let [xpaths  (map first (xpaths-ranked page-src))
        records (minimum-maximal-xpath-records xpaths page-src)
        model   (site-model xpaths records page-src)]
    (extract-dates records (:date-pattern model))))

(defn sort-order
  [dates-list]
  (let [timeline
        (reduce
         (fn [acc v]
           (if (and (empty? (:asc acc))
                    (empty? (:dsc acc)))
             (merge-with concat acc {:asc [v] :dsc [v]})
             (when v
              (if (core-time/after? v (last (:asc [v])))
                (merge-with concat acc {:asc [v]})
                (merge-with concat acc {:dsc [v]})))))
         {:asc []
          :dsc []}
         dates-list)]
    (cond (> (count (:asc timeline))
               (* 2 (count (:dsc timeline))))
          'asc

          (> (count (:dsc timeline))
             (* 2 (count (:asc timeline))))
          'dsc

          :else 'unsorted)))

(defn page-model
  [page-src]
  (let [records-dates (date-indexed-records page-src)
        dates         (map second records-dates)
        records       (map first records-dates)
        record-xpath  (-> (first records)
                          path-root-seq-nodes
                          nodes->xpath
                          first)]
    {:num-records   (count records-dates)
     :sort-order    (try (sort-order dates)
                         (catch Exception e 'failed))
     :records-xpath (clojure.string/replace
                     record-xpath #"/\#document" "")
     :date-pattern  (find-date-pattern records)}))
