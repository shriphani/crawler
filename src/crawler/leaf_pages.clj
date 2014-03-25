(ns crawler.leaf-pages
  "Code to download and data mine discussion forum leaf pages"
  (:require [clojure.java.io :as io]
            [crawler.dom :as dom]
            [crawler.utils :as utils]
            [structural-similarity.xpath-text :as xpath-text])
  (:use [clojure.pprint :only [pprint]]
        [clj-xpath.core :only [$x $x:node $x:node+ $x:text+ $x:node*]]
        [clj-ml.utils]
        [clj-ml.classifiers]
        [clj-ml.data]
        [clj-ml.io]))

(def *train-corpus* [["http://www.phpbb.com/community/viewtopic.php?f=65&t=1494875" true]
                     ["http://www.phpbb.com/community/viewtopic.php?f=65&t=2231006" true]
                     ["http://www.vbulletin.org/forum/showthread.php?t=309486" true]
                     ["http://www.vbulletin.org/forum/showthread.php?t=308426" true]
                     ["http://forums.finalgear.com/the-forums/important-reminder-about-forum-rules-57529/" true]
                     ["http://forums.finalgear.com/the-forums/top-gear-lost-episodes-53537/" true]
                     ["http://community.invisionpower.com/" false]
                     ["http://community.invisionpower.com/forum/305-pre-sales-questions/" false]
                     ["http://www.vbulletin.org/forum/index.php" false]
                     ["http://www.vbulletin.org/forum/forumdisplay.php?f=2" false]
                     ["http://forums.finalgear.com/" false]
                     ["http://forums.finalgear.com/the-site-itself/" false]
                     ["http://r.789695.n4.nabble.com/" false]
                     ["http://r.789695.n4.nabble.com/Trouble-with-difining-random-effects-in-glmm-and-lme-td4687033.html" true]
                     ["http://r.789695.n4.nabble.com/Thresold-td4687032.html" true]
                     ["http://r.789695.n4.nabble.com/Correlograms-using-boxes-and-different-variables-on-rows-and-columns-td4631309.html" true]
                     ["http://r.789695.n4.nabble.com/is-it-possible-to-get-the-coordinate-of-mtext-td4687001.html" true]
                     ["http://r.789695.n4.nabble.com/Defining-and-Cutoff-Point-in-R-td4687027.html" true]
                     ["http://r.789695.n4.nabble.com/Beginner-How-do-I-copy-the-results-from-a-for-loop-in-a-csv-file-td4687018.html" true]
                     ["http://r.789695.n4.nabble.com/Howdens-Kitchens-Reviews-td4687022.html" true]
                     ["http://r.789695.n4.nabble.com/filter-seq-wise-td4686796.html" true]
                     ["http://r.789695.n4.nabble.com/survfit-question-Q1-and-Q3-survival-time-td4686863.html" true]
                     ["http://r.789695.n4.nabble.com/Beginner-How-do-I-copy-the-results-from-a-for-loop-in-a-csv-file-td4687013.html" true]
                     ["http://r.789695.n4.nabble.com/locpoly-is-returning-NaN-if-bandwidth-is-small-td4687015.html" true]
                     ["http://r.789695.n4.nabble.com/Reshaping-Data-in-R-Transforming-Two-Columns-Into-One-td4687019.html" true]
                     ["http://r.789695.n4.nabble.com/Error-Message-Help-td4687017.html" true]
                     ["http://r.789695.n4.nabble.com/strangely-long-floating-point-with-write-table-td4686858.html" true]
                     ["http://grails.1312388.n4.nabble.com/" false]
                     ["http://grails.1312388.n4.nabble.com/Grails-Dynamic-Html-Forms-td4655185.html" true]
                     ["http://grails.1312388.n4.nabble.com/Database-agnostic-database-migration-syntax-td4640467.html" true]
                     ["http://grails.1312388.n4.nabble.com/Startup-with-2-3-7-and-cache-ehcache-1-0-1-td4654775.html" true]
                     ["http://grails.1312388.n4.nabble.com/Howdens-Kitchens-Reviews-td4655182.html" true]
                     ["http://grails.1312388.n4.nabble.com/Access-denied-for-user-localhost-using-grails-2-3-5-and-mysql-workbench-td4655181.html" true]
                     ["http://grails.1312388.n4.nabble.com/Static-content-blocked-by-Spring-Security-td4655174.html" true]
                     ["http://grails.1312388.n4.nabble.com/database-migration-SOS-td4544152.html" true]
                     ["http://grails.1312388.n4.nabble.com/How-to-Externalize-i18N-folder-td4655172.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/" false]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/Forum-Guidelines-td7586431.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/Howdens-Kitchens-Reviews-td7596891.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/Two-recent-Relic-hunts-finds-td7596878.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/Good-Day-in-Tot-Lot-Land-td7596883.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/Today-on-the-River-17th-march-td7596855.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/COMPASS-XP-PRO-REPAIR-WHO-td7596870.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/Yesterdays-takes-lotsa-wheats-and-a-couple-silver-td7596797.html" true]
                     ["http://compass-metal-detector-forum.548136.n2.nabble.com/New-old-Compass-toy-td7596785.html" true]
                     ["http://photography-on-the.net/forum/" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=15" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=17" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=9" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=33" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=35" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=34" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=108" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=110" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=122" false]
                     ["http://photography-on-the.net/forum/forumdisplay.php?f=23" false]
                     ["http://hardforum.com/showthread.php?t=1811251" true]
                     ["http://hardforum.com/showthread.php?t=1811422" true]
                     ["http://hardforum.com/showthread.php?t=1811417" true]
                     ["http://www.dgrin.com/showthread.php?t=244757" true]
                     ["http://www.dgrin.com/showthread.php?t=245710" true]
                     ["http://www.dgrin.com/showthread.php?t=245450" true]
                     ["http://www.wilderssecurity.com/showthread.php?t=361318" true]
                     ["http://www.wilderssecurity.com/showthread.php?t=361082" true]
                     ["http://www.wilderssecurity.com/showthread.php?t=360570" true]])

(def *test-corpus* [["http://hardforum.com/showthread.php?t=1811251" true]
                    ["http://hardforum.com/showthread.php?t=1811422" true]
                    ["http://hardforum.com/showthread.php?t=1811417" true]
                    ["http://hardforum.com/showthread.php?t=1811263" true]
                    ["http://hardforum.com/showthread.php?t=1811337" true]
                    ["http://hardforum.com/showthread.php?t=1811272" true]
                    ["http://hardforum.com/showthread.php?t=1811321" true]
                    ["http://hardforum.com/forumdisplay.php?f=116" false]
                    ["http://www.wilderssecurity.com/" false]
                    ["http://www.wilderssecurity.com/forumdisplay.php?f=7" false]
                    ["http://www.wilderssecurity.com/forumdisplay.php?f=87" false]
                    ["http://www.wilderssecurity.com/forumdisplay.php?f=108" false]
                    ["http://www.wilderssecurity.com/forumdisplay.php?f=114" false]
                    ["http://www.wilderssecurity.com/showthread.php?t=334644" true]
                    ["http://www.wilderssecurity.com/showthread.php?t=251015" true]
                    ["http://www.wilderssecurity.com/showthread.php?t=358819" true]
                    ["http://www.wilderssecurity.com/showthread.php?t=361433" true]
                    ["http://www.wilderssecurity.com/showthread.php?t=361318" true]
                    ["http://www.wilderssecurity.com/showthread.php?t=361082" true]
                    ["http://www.wilderssecurity.com/showthread.php?t=360570" true]
                    ["http://www.dgrin.com/" false]
                    ["http://www.dgrin.com/forumdisplay.php?f=3" false]
                    ["http://www.dgrin.com/forumdisplay.php?f=4" false]
                    ["http://www.dgrin.com/forumdisplay.php?f=7" false]
                    ["http://www.dgrin.com/forumdisplay.php?f=23" false]
                    ["http://www.dgrin.com/showthread.php?t=128992" true]
                    ["http://www.dgrin.com/showthread.php?t=244168" true]
                    ["http://www.dgrin.com/showthread.php?t=244757" true]
                    ["http://www.dgrin.com/showthread.php?t=244757" true]
                    ["http://www.dgrin.com/showthread.php?t=245710" true]
                    ["http://www.dgrin.com/showthread.php?t=245450" true]
                    ["http://forum.doom9.org/showthread.php?t=170120" true]
                    ["http://forum.doom9.org/showthread.php?t=22912" true]
                    ["http://forum.doom9.org/showthread.php?t=170360" true]
                    ["http://forum.doom9.org/" false]
                    ["http://www.thehighroad.org/" false]
                    ["http://www.thehighroad.org/forumdisplay.php?f=2" false]
                    ["http://www.thehighroad.org/showthread.php?t=745648" true]
                    ["http://www.thehighroad.org/showthread.php?t=472825" true]
                    ["http://www.thehighroad.org/showthread.php?t=747181" true]
                    ["http://www.thehighroad.org/showthread.php?t=746802" true]
                    ["http://www.thehighroad.org/showthread.php?t=747140" true]
                    ["http://www.thehighroad.org/showthread.php?t=747101" true]
                    ["http://www.thehighroad.org/showthread.php?t=742288" true]
                    ["http://advrider.com/forums/" false]
                    ["http://advrider.com/forums/forumdisplay.php?f=2" false]
                    ["http://advrider.com/forums/forumdisplay.php?f=78" false]
                    ["http://advrider.com/forums/forumdisplay.php?f=4" false]
                    ["http://advrider.com/forums/showthread.php?t=935276" true]
                    ["http://advrider.com/forums/showthread.php?t=961360" true]
                    ["http://advrider.com/forums/showthread.php?t=957797" true]
                    ["http://advrider.com/forums/showthread.php?t=961777" true]
                    ["http://advrider.com/forums/showthread.php?t=956224" true]
                    ["http://groovy.329449.n5.nabble.com/" false]
                    ["http://groovy.329449.n5.nabble.com/How-to-detect-acess-to-private-fields-and-methods-td5718863.html" true]
                    ["http://groovy.329449.n5.nabble.com/Multiple-assignment-underflow-fails-for-arrays-td5718800.html" true]
                    ["http://groovy.329449.n5.nabble.com/implementing-the-each-method-in-a-java-class-to-accept-groovy-closure-td5718831.html" true]
                    ["http://groovy.329449.n5.nabble.com/why-is-a-global-AST-transform-applied-in-one-case-but-not-another-td5718794.html" true]])

(def text-formatting-tags [#"/b/" #"/em/" #"/i/" #"/small/" #"/strong/" #"/sub/" #"/sup/" #"/ins/" #"/del/" #"/mark/" #"/p/"])

(defn generate-features
  [body]
  (let [xml-doc (dom/html->xml-doc body)

        text-nodes ($x:text+ ".//text()" xml-doc)

        punct (apply
               +
               (map
                (fn [t]
                  (double
                   (/ (count (re-seq #"(?![<,>,{,},\[,\]])\p{Punct}" t))
                      (count t))))
                text-nodes))
        
        text-xpaths (xpath-text/page-text-xpaths body)
        anchor-text-xpaths (filter
                            (fn [[x cs]]
                              (re-find #"/a/" x))
                            text-xpaths)
        
        text-xs-cs (reduce
                    (fn [acc [x cs]]
                      (merge-with concat acc {x [(clojure.string/trim cs)]}))
                    {}
                    text-xpaths)
        anchor-xs-cs (reduce
                      (fn [acc [x cs]]
                        (merge-with concat acc {x [(clojure.string/trim cs)]}))
                      {}
                      anchor-text-xpaths)

        cs-per-xp-t (map (fn [[x strings]]
                           (let [n-strings (map count strings)]
                             (double
                              (/ (apply + n-strings)
                                 (count n-strings))))) text-xs-cs)

        cs-per-xp-a      (map (fn [[x strings]]
                                (let [n-strings (map count strings)]
                                  (double
                                   (/ (apply + n-strings)
                                      (count n-strings))))) anchor-xs-cs)

        text-xs (set (map first text-xpaths))
        anchor-xs (set (map first anchor-text-xpaths))

        max-text-xp (last
                     (sort-by #(/ (apply + (map count (second %)))
                                  (count (second %)))
                              text-xs-cs))

        num-forms-t (count
                     (filter
                      identity
                      (map
                       (fn [x]
                         (some (fn [t] (re-find t x)) text-formatting-tags))
                       text-xs)))

        num-forms-a (count
                     (filter
                      identity
                      (map
                       (fn [x]
                         (some (fn [t] (re-find t x)) text-formatting-tags))
                       anchor-xs)))

        num-paras  (count ($x:node* ".//p" xml-doc))
        num-breaks (count ($x:node* ".//br" xml-doc))]
    
    [(double
      (/ (apply + cs-per-xp-t)
         (count cs-per-xp-t)))
     (double
      (/ (apply + cs-per-xp-a)
         (count cs-per-xp-a)))
     (- (count text-xs)
        (count anchor-xs))
     (count anchor-xs)
     (double
      (/ (apply + (map count (second max-text-xp)))
         (count (second max-text-xp))))
     (- num-forms-t num-forms-a)
     num-forms-a
     num-paras
     num-breaks
     punct]))

(defn generate-features-map
  [body]
  (let [[A B C D E F G H I J] (generate-features body)]
    {:A A
     :B B
     :C C
     :D D
     :E E
     :F F
     :G G
     :H H
     :I I
     :J J
     :label 1}))

(defn download-corpus
  []
  (let [wrtr (io/writer "leaf-train.clj")]
    (pprint
     (reduce
      (fn [acc [l label]]
        (do
          (let [bd (first
                    (filter identity (repeatedly 5 #(do
                                                      (Thread/sleep 1000)
                                                      (utils/download-with-cookie l)))))]
            (println l)
            (cons
            [bd label]
            acc))))
      {}
      *train-corpus*)
     wrtr)))

(defn download-test-corpus
  []
  (let [wrtr (io/writer "leaf-test.clj")]
    (pprint
     (reduce
      (fn [acc [l label]]
        (do
          (Thread/sleep 1000)
          (cons
           [l (utils/download-with-cookie l) label]
           acc)))
      []
      *test-corpus*)
     wrtr)))

(defn read-leaf-train-corpus
  []
  (let [rdr (io/reader "leaf-train.clj")]
    (read
     (java.io.PushbackReader. rdr))))

(defn read-leaf-test-corpus
  []
  (let [rdr (io/reader "leaf-test.clj")]
    (read
     (java.io.PushbackReader. rdr))))

(defn test-corpus
  []
  (let [corpus (read-leaf-test-corpus)
        classifier (deserialize-from-file "trained.bin")
        dataset (load-instances :arff "file:///usr0/home/spalakod/Documents/crawler/train.arff")]
    (do
      (dataset-set-class dataset 10)
      (let [res (map
                 (fn [[link b l]]
                   (let [fs (generate-features-map b)
                         instance (make-instance dataset fs)]
                     [link l (do (classifier-classify classifier instance)
                                 (pos?
                                  (Integer/parseInt
                                   (last
                                    (clojure.string/split
                                     (.toString (classifier-label classifier instance))
                                     #",")))))]))
                 corpus)]
        {:accuracy (/ (count (filter #(= (second %) (nth % 2)) res))
                      (count res))
         :results res}))))

(defn generate-arff-file
  []
  (let [corpus (read-leaf-train-corpus)
        wrtr (io/writer "train.arff")]
    (binding [*out* wrtr]
      (println "@relation ABCDEFGHIJ")
      (println)
      (println "@attribute A numeric")
      (println "@attribute B numeric")
      (println "@attribute C numeric")
      (println "@attribute D numeric")
      (println "@attribute E numeric")
      (println "@attribute F numeric")
      (println "@attribute G numeric")
      (println "@attribute H numeric")
      (println "@attribute I numeric")
      (println "@attribute J numeric")
      (println "@attribute label {1,-1}")
      (println)
      (println "@data")
      (doseq [[b l] corpus]
        (let [fs (try (generate-features b)
                      (catch Exception e nil))]
          (when fs
            (print (clojure.string/join ", " (map str fs)))
            (print ", ")
            (println (if l "1" "-1"))))))))
