(ns crawler.rich-char-extractor
  "Character based extraction as opposed to token based extraction"
  (require [crawler.dom :as dom]
           [crawler.similarity :as similarity]
           [net.cgrand.enlive-html :as html]
           [crawler.utils :as utils]))

(defn state-action
  "Args:
    page-src : the body
    url-ds   : the source url data-structure
    template-removed: Xpath, Href pairs that we remove from the decision space"
  ([page-src url-ds template-removed]
     (state-action page-src url-ds template-removed []))

  ([page-src url-ds template-removed blacklist]
     (let [url                  (-> url-ds :url)
           
           proc-pg              (dom/html->xml-doc page-src)

           ; xpaths nodes and associated anchor-text
           xpaths-hrefs-text    (dom/xpaths-hrefs-tokens-no-position proc-pg
                                                                     url
                                                                     blacklist)

           template-removed     (map
                                 (fn [[xpath infos]]
                                   (let [template-links (-> xpath
                                                            template-removed
                                                            set)]
                                     [xpath (filter
                                             (fn [{_ :node href :href _1 :text}]
                                               (not
                                                (some #{href} template-links)))
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
                                                             (+
                                                              acc
                                                              (:num-chars an-info)))
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
        :xpath-nav-info (reverse
                         (sort-by :score xpath-nav-info))})))

(defn state-action-model
  [actions page-src url-ds template-removed blacklist]
  (let [taken-actions     (:path url-ds)
        action-to-take    (first
                           (drop
                            (count taken-actions)
                            (reverse
                             (first actions))))]
    (when action-to-take
      (let [processed-pg      (dom/html->xml-doc page-src)
            
            xpaths-hrefs-text (dom/eval-anchor-xpath action-to-take
                                                     processed-pg
                                                     (:url url-ds)
                                                     blacklist)
            
            template-removed     (map
                                  (fn [[xpath infos]]
                                    (let [template-links (-> xpath
                                                             template-removed
                                                             set)]
                                      [xpath (filter
                                              (fn [{_ :node href :href _1 :text}]
                                                (not
                                                 (some #{href} template-links)))
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
                                                              (+
                                                               acc
                                                               (:num-chars an-info)))
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
        {:page-wide-nav-chars page-wide-nav-chars
         :xpath-nav-info (reverse
                          (sort-by :score xpath-nav-info))}))))

(defn state-action-sampled
  ([page-src url-ds template-removed]
     (state-action-sampled page-src url-ds template-removed []))

  ([page-src url-ds template-removed blacklist]
     (let [complete (state-action page-src
                                  url-ds
                                  template-removed
                                  blacklist)]
       (merge
        complete
        {:xpath-nav-info
         (map
          (fn [x]
            (merge x {:hrefs (take 10 (:hrefs x))
                      :texts (take 10 (:texts x))}))
          (:xpath-nav-info complete))}))))
