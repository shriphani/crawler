(ns crawler.rich-char-extractor
  "Character based extraction as opposed to token based extraction"
  (require [crawler.dom :as dom]))

(defn state-action
  ([page-src url]
     (state-action page-src url []))

  ([page-src url blacklist]
     (let [processed-pg         (dom/html->xml-doc page-src)

           ; xpaths nodes and associated anchor-text
           xpaths-hrefs-text    (dom/xpaths-hrefs-tokens
                                 processed-pg
                                 url
                                 blacklist)
           xpaths-anchors-chars (map
                                 (fn [[xpath nodes]]
                                   [xpath (map
                                           (fn [a-node]
                                             {:href (:href a-node)
                                              :num-chars (-> a-node
                                                             :text
                                                             count)})
                                           nodes)])
                                 xpaths-hrefs-text)

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
                                 xpaths-hrefs-text)

           xpath-nav-info       (map
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
                                 xpaths-anchors-chars)]
       
       {:total-nav-info page-wide-nav-chars
        :xpath-nav-info (sort-by :score xpath-nav-info)})))
