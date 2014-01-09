(ns crawler.rich-char-extractor
  "Character based extraction as opposed to token based extraction"
  (require [crawler.dom :as dom]))

(defn leaf?
  [src-num target-num]
  (and
   (-> src-num nil? not)
   (<= 0.5 (/ target-num src-num))))

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
                                   [xpath (map
                                           (fn [a-node]
                                             {:href (:href a-node)
                                              :num-chars (-> a-node
                                                             :text
                                                             count)})
                                           nodes)])
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
                                    {:xpath xpath
                                     :score (reduce
                                             (fn [acc an-info]
                                               (+ acc (:num-chars an-info)))
                                             0
                                             info)

                                     :hrefs (reduce
                                             (fn [acc an-info]
                                               (cons (:href an-info) acc))
                                             '()
                                             info)})
                                  xpaths-anchors-chars))]

       {:total-nav-info page-wide-nav-chars
        :xpath-nav-info (sort-by :score xpath-nav-info)})))

(defn filter-content
  "Helper routine you can use to filter
   content that the extractor mined.
   Threshold determined by training"
  [content]
  (filter
   (fn [x]
     (->> x :score (<= 0.1)))
   (map
    (fn [{xpath :xpath score :score hrefs :hrefs}]
      {:xpath xpath
       :score (/ score (:total-nav-info content))
       :hrefs hrefs})
    (:xpath-nav-info content))))
