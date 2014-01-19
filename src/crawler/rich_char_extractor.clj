(ns crawler.rich-char-extractor
  "Character based extraction as opposed to token based extraction"
  (require [crawler.dom :as dom]
           [crawler.similarity :as similarity]
           [net.cgrand.enlive-html :as html]
           [crawler.utils :as utils]))

(defn leaf?
  "Examples are used to to RTDM style stuff."
  [src-num target-num]
  (and
   (-> src-num nil? not)
   (>= 0.2 (/ target-num src-num))))

(defn state-action
  "Args:
    page-src : the body
    url      : the source url
    template-removed: Xpath, Href pairs that we remove from the decision space"
  ([page-src url template-removed]
     (state-action page-src url template-removed []))

  ([page-src url template-removed blacklist]
     (let [processed-pg         (dom/html->xml-doc page-src)

           ; xpaths nodes and associated anchor-text
           xpaths-hrefs-text    (dom/xpaths-hrefs-tokens
                                 processed-pg
                                 url
                                 blacklist)

           template-removed     (map
                                 (fn [[xpath infos]]
                                   (let [template-links (-> xpath
                                                            template-removed
                                                            set)]
                                     [xpath (filter
                                             (fn [{_ :node href :href _1 :text}]
                                               (not (some #{href} template-links)))
                                             infos)]))
                                 xpaths-hrefs-text)

           xpaths-anchors-chars (map
                                 (fn [[xpath nodes]]
                                   [xpath (distinct
                                           (map
                                            (fn [a-node]
                                              {:href (:href a-node)
                                               :num-chars (-> a-node
                                                              :text
                                                              count)
                                               :text (:text a-node)})
                                            nodes))])
                                 template-removed)

           page-wide-nav-chars  (reduce
                                 (fn [acc [xpath nodes]]
                                   (+ acc
                                      (reduce
                                       (fn [acc a-node]
                                         (+ acc (-> a-node
                                                    :text
                                                    count)))
                                       0
                                       nodes)))
                                 0
                                 template-removed)

           xpath-nav-info       (filter
                                 #(not
                                   (or
                                    (-> % :score zero?)
                                    (-> % :hrefs count zero?)))
                                 (map
                                  (fn [[xpath info]]
                                    (let [potential-xpath xpath
                                          potential-score (reduce
                                                           (fn [acc an-info]
                                                             (+ acc (:num-chars an-info)))
                                                           0
                                                           info)
                                          potential-hrefs (reduce
                                                           (fn [acc an-info]
                                                             (cons (:href an-info) acc))
                                                           '()
                                                           info)
                                          potential-texts (reduce
                                                           (fn [acc an-info]
                                                             (cons (:text an-info) acc))
                                                           '()
                                                           info)]
                                      {:xpath potential-xpath
                                       :score potential-score
                                       :hrefs potential-hrefs
                                       :texts potential-texts}))
                                  xpaths-anchors-chars))]

       {:total-nav-info page-wide-nav-chars
        :xpath-nav-info (sort-by :score xpath-nav-info)})))

(defn detect-recommender-engine-links
  [src-text-mean src-text-stdev decision-space body]
  (let [means (map
               (fn [{xpaths :xpaths
                    hrefs  :hrefs
                    score  :score
                    texts  :texts}]
                 (let [text-sizes (map count texts)
                       text-mean  (/
                                   (apply + text-sizes)
                                   (count text-sizes))]
                   [{:xpaths xpaths
                     :hrefs  hrefs
                     :score  score
                     :texts  texts}
                    text-mean]))
               decision-space)]
    (map
     (fn [[stuff a-mean]]
       (let [res        (- a-mean src-text-mean)
             candidate? (or (<= res src-text-stdev)
                            (<= res (* (- 1.0)
                                       src-text-stdev)))]
         (when candidate?
           (let [links (:hrefs stuff)]
             (some
              #(<= 0.8
                   (similarity/tree-edit-distance-html
                    body (utils/download-with-cookie %)))
              (take 3 links))))))
     means)))

(defn filter-content
  "Helper routine you can use to filter
   content that the extractor mined.
   Threshold determined by training"
  [content]
  (filter
   (fn [x]
     (->> x :score (<= 0.1)))
   (map
    (fn [{xpath :xpath score :score hrefs :hrefs texts :texts}]
      {:xpath xpath
       :score (/ score (:total-nav-info content))
       :hrefs hrefs
       :texts texts})
    (:xpath-nav-info content))))

(defn sample-quarter
  [a-seq]
  (let [size (count a-seq)]
    (take (int (/ size 4)) a-seq)))

(defn pagination?
  "Even 1 pagination is good."
  [{xpath :xpath hrefs :hrefs texts :texts} body]
  (let [to-sample (sample-quarter hrefs)
        page-sims (map
                   #(similarity/tree-edit-distance-html
                     body ())
                   to-sample)]

    (filter #(<= 0.8 %) page-sims)))

(defn detect-pagination
  "Pagination detection module.
   First, we kick off from the least useful guys
   and go to the most useful guys. Along the way,
   bundle some shit up."
  [normalized-decision-space body]
  (let [tree1  (html/html-resource
                (java.io.StringReader. body))
        sorted (sort-by :score normalized-decision-space)]
    (map #(pagination? % tree1) sorted)))
