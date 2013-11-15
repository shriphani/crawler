;;;; Soli Deo Gloria
;;;; spalakod

(ns crawler.test-system
  (:gen-class :main true)
  (:require [clojure.tools.cli :as cli]
            [crawler.dom :as dom]
            [crawler.extractor :as extractor]
            [crawler.page :as page]
            [crawler.rank :as rank]
            [crawler.records :as records])
  (:use     [clj-xpath.core :only [$x $x:node $x:node+ $x:text+]]))

;; positive examples : page contains enumeration
(def *enumerate-positives*
  [{:url   "http://www.fmylife.com"
    :xpath "//html/body[contains(@id,'top')]/div[contains(@id,'background')]/div[contains(@id,'content')]/div[contains(@class,'wrapper')]/div[contains(@class,'pagination')]/ul[contains(@class,'left')]/li/a"}

   {:url   "https://boards.4chan.org/vg/"
    :xpath "//html/body[contains(@class,'board')]/div[contains(@class,'pagelist')]/div[contains(@class,'pages')]/a"}

   {:url   "https://www.phpbb.com/community/viewforum.php?f=69"
    :xpath "//html/body[contains(@id,'phpbb') and contains(@class,'section')]/div[contains(@id,'wrap')]/div[contains(@id,'page')]/div[contains(@class,'topic')]/div[contains(@class,'pagination')]/span/a"}

   {:url   "https://www.phpbb.com/community/viewtopic.php?f=69&t=1870815"
    :xpath "//html/body[contains(@id,'phpbb') and contains(@class,'section')]/div[contains(@id,'wrap')]/div[contains(@id,'page')]/div[contains(@class,'topic')]/div[contains(@class,'pagination')]/span/a"}

   {:url   "http://stackoverflow.com/questions?pagesize=50&sort=newest"
    :xpath "//html/body[contains(@class,'questions')]/div[contains(@class,'container')]/div[contains(@id,'content')]/div[contains(@id,'mainbar')]/div[contains(@class,'pager')]/a"}

   {:url   "http://gis.stackexchange.com/questions?pagesize=50&sort=newest"
    :xpath "//html/body[contains(@class,'questions')]/div[contains(@class,'container')]/div[contains(@id,'content')]/div[contains(@id,'mainbar')]/div[contains(@class,'pager')]/a"}

   {:url   "http://carsandetc.tumblr.com/"
    :xpath "//html/body/div[contains(@id,'wrapper')]/div[contains(@id,'content')]/div[contains(@id,'navigation')]/a"}

   {:url   "http://www.topix.com/forum/city/carrizo-springs-tx/"
    :xpath "//html/body[contains(@id,'stream')]/div[contains(@id,'content')]/div[contains(@id,'content') and contains(@class,'xtra')]/div[contains(@id,'onetwocombo')]/div[contains(@class,'onetwosub')]/div/div/a"}

   {:url   "http://grails.1312388.n4.nabble.com/"
    :xpath "//HTML/body/div[contains(@id,'nabble') and contains(@class,'nabble')]/span[contains(@class,'pages')]/span[contains(@class,'page')]/a"}])

(def nabble-test-case
  ["http://grails.1312388.n4.nabble.com/"
   "http://postgresql.1045698.n5.nabble.com/"
   "http://jenkins-ci.361315.n4.nabble.com/"
   "http://lucene.472066.n3.nabble.com/"
   "http://elasticsearch-users.115913.n3.nabble.com/"
   "http://windows-installer-xml-wix-toolset.687559.n2.nabble.com/"
   "http://dojo-toolkit.33424.n3.nabble.com/"
   "http://jmeter.512774.n5.nabble.com/"
   "http://abaqus-users.1086179.n5.nabble.com/"])

(def blogs-test-case
  ["http://www.charitywater.org/blog/"
   "http://www.hermanmiller.com/discover/"
   "http://www.thesartorialist.com/"])

(def tumblr-test-case
  ["http://oldpeoplefacebook.tumblr.com/"
   "http://anthonybourdain.tumblr.com/"
   "http://1796foods.com/"
   "http://efccooking.tumblr.com/"
   "http://scandybars.tumblr.com/"
   "http://gastrogirl.tumblr.com/"
   "http://foodculturalist.tumblr.com/"
   "http://sexiestfoods.tumblr.com/"
   "http://artinmycoffee.com/"])

(defn test-nabble-enumeration
  [thresh]
  (map
   (fn [a-url]
     (let [enumerators (extractor/enum-candidates
                        (extractor/process-link a-url {})
                        {:sim-thresh thresh})]
       enumerators))
   nabble-test-case))

(defn test-nabble-enumeration-rank
  [thresh]
  (map
   (fn [a-url]
     (let [enumerators (extractor/enum-candidates
                        (extractor/process-link a-url {})
                        {:sim-thresh thresh})]
       (rank/rank-enum-candidates enumerators)))
   nabble-test-case))

(defn test-enumeration
  ([]
     (test-enumeration {:verbose false}))
  ([{verbose :verbose}]
     (if verbose
       (pmap
        (fn [{url :url xpath :xpath}]
          (extractor/process-link url {}))
        *enumerate-positives*)
       (pmap
        (fn [{url :url xpath :xpath}]
          (let [ranked-enums (extractor/process-link url {})
                xpaths       (map #(-> % :xpath) ranked-enums)]
            (.indexOf xpaths xpath)))
        *enumerate-positives*))))

(defn test-similarity
  []
  (pmap
   (fn [{url :url xpath :xpath}]
     {:url url
      :similarities (let [{src-link     :src-link
                           explorations :explorations
                           xpaths-hrefs :xpaths-hrefs
                           signature    :signature
                           dfs          :dfs
                           hrefs        :hrefs
                           novelties    :novelties
                           _            :in-host-xpaths-hrefs}
                          (extractor/process-link url {})]
                      (pmap
                       (fn [{xpath :xpath xpath-explorations :explorations}]
                         {:xpath
                          xpath

                          :similarities
                          (map
                           #(let [xpaths      (extractor/exploration-xpaths %)
                                  xpath-hrefs (-> % :hrefs-table)
                                  expl-sign   (page/page-signature xpaths xpath-hrefs)]
                              {:url                 (-> % :url)
                               :similarity          (* (page/signature-similarity-cosine
                                                        signature expl-sign)
                                                       (page/signature-similarity-cardinality
                                                        signature expl-sign))})
                           xpath-explorations)})
                       explorations))})
   *enumerate-positives*))

(defn similarity-histogram
  "Produces a histogram of similarity scores for the
enumerate-positives dataset"
  []
  (pmap
   (fn [{url :url xpath :xpath}]
     {:url url
      :similarities (let [{src-link     :src-link
                           explorations :explorations
                           xpaths-hrefs :xpaths-hrefs
                           signature    :signature
                           dfs          :dfs
                           hrefs        :hrefs
                           novelties    :novelties
                           _            :in-host-xpaths-hrefs}
                          (extractor/process-link url {})]
                      
                      (reverse
                       (sort-by
                        #(-> % :similarity)
                        (flatten
                         (pmap
                          (fn [{xpath :xpath xpath-explorations :explorations}]
                            (map
                             #(let [xpaths      (extractor/exploration-xpaths %)
                                    xpath-hrefs (-> % :hrefs-table)
                                    expl-sign   (page/page-signature xpaths xpath-hrefs)]
                                {:url                 (-> % :url)
                                 :similarity          (* (page/signature-similarity-cosine
                                                          signature expl-sign)
                                                         (page/signature-similarity-cardinality
                                                          signature expl-sign))})
                             xpath-explorations))
                          explorations)))))})
   *enumerate-positives*))

(defn test-enumeration-candidates
  [thresh]
  (pmap
   (fn [{url :url xpath :xpath}]
     {:url
      url

      :enum-candidates
      (let [info (extractor/process-link url {})]
        (extractor/enum-candidates info {:sim-thresh thresh}))})
   *enumerate-positives*))

(defn dump-explorations
  []
  (pmap
   (fn [{url :url xpath :xpath}]
     (let [[explorations dfs hrefs novelties]
           (extractor/process-link url {})]
       explorations))
   *enumerate-positives*))

(defn similarity-edit-dist-histogram
  [ins-cost del-cost]
  (pmap
   (fn [{url :url xpath :xpath}]
     {:url url
      :similarities (let [{src-link     :src-link
                           explorations :explorations
                           xpaths-hrefs :xpaths-hrefs
                           signature    :signature
                           dfs          :dfs
                           hrefs        :hrefs
                           novelties    :novelties
                           _            :in-host-xpaths-hrefs}
                          (extractor/process-link url {})]
                      
                      (reverse
                       (sort-by
                        #(-> % :similarity)
                        (flatten
                         (pmap
                          (fn [{xpath :xpath xpath-explorations :explorations}]
                            (map
                             #(let [xpaths      (extractor/exploration-xpaths %)
                                    xpath-hrefs (-> % :hrefs-table)
                                    expl-sign   (page/page-signature xpaths xpath-hrefs)]
                                {:url                 (-> % :url)
                                 :similarity          (page/signature-edit-distance-similarity
                                                       signature expl-sign ins-cost del-cost)})
                             xpath-explorations))
                          explorations)))))})
   *enumerate-positives*))

(defn -main
  [& args]
  (let [[optional _ _] (cli/cli args
                                ["--similarity-test"
                                 "Test similarity routines"
                                 :flag true
                                 :default false]

                                ["--enumeration-test"
                                 "Test enumeration routine"
                                 :flag true
                                 :default false]

                                ["--enumeration-candidates-test"
                                 "Test enumeration candidates"
                                 :flag true
                                 :default false]

                                ["--sim-thresh"
                                 "Similarity threshold"]

                                ["--sim-histogram"
                                 "Similarity Histogram for each test case"
                                 :flag true
                                 :default false]

                                ["--edit-distance-sim-histogram"
                                 "Similarity histogram using the XPath edit dist"
                                 :flag true
                                 :default false]

                                ["--ins-cost"
                                 "Insertion cost"]

                                ["--del-cost"
                                 "Insertion cost"]
                                
                                ["-v" "--verbose"
                                 "Verbose version"
                                 :flag true
                                 :default false]

                                ["--nabble-enumeration"
                                 "Nabble index pages consistent"
                                 :flag true
                                 :default false]

                                ["--nabble-enumeration-rank"
                                 "Nabble index pages rank enumeration"
                                 :flag true
                                 :default false])]

    (when (:similarity-test optional)
      (clojure.pprint/pprint (test-similarity)))

    (when (:enumeration-test optional)
      (clojure.pprint/pprint
       (test-enumeration
        {:verbose (optional :verbose)})))

    (when (:enumeration-candidates-test optional)
      (clojure.pprint/pprint
       (-> optional
           :sim-thresh
           Double/parseDouble
           test-enumeration-candidates)))

    (when (:sim-histogram optional)
      (clojure.pprint/pprint
       (similarity-histogram)))

    (when (:nabble-enumeration optional)
      (clojure.pprint/pprint
       (test-nabble-enumeration 0.85)))

    (when (:nabble-enumeration-rank optional)
      (clojure.pprint/pprint
       (test-nabble-enumeration-rank 0.85)))

    (when (:edit-distance-sim-histogram optional)
      (clojure.pprint/pprint
       (similarity-edit-dist-histogram
        (-> optional
            :ins-cost
            Double/parseDouble)
        (-> optional
            :del-cost
            Double/parseDouble))))))
