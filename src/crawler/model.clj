(ns crawler.model
  "Contains code to read and process a
   sampled model"
  (:require [clojure.java.io :as io]
            [crawler.dom :as dom])
  (:import [java.io PushbackReader]))

(defn read-model
  [a-model-file]
  (with-open [rdr (io/reader a-model-file)]
    (-> rdr PushbackReader. read)))

(defn xpath-bad-separation-quality
  [{model :model corpus :corpus}]
  (let [same-xpath-groups (group-by :src-xpath
                                    (vals corpus))]
    (filter
     #(< 1 (second %))
     (map
      (fn [a-group]
        (let [members (second a-group)]
          [(first a-group)
           (count
            (distinct
             (map
              #(->> %
                    :state-action
                    :xpath-nav-info
                    (map :xpath)
                    set)
              members)))]))
      same-xpath-groups))))

(defn handle-xpaths
  [{model :model corpus :corpus} src-xpath grps]
  (let [urls (set
              (map
               #(-> % second :src-url)
               (filter
                #(= (-> % second :src-xpath)
                    src-xpath)
                corpus)))

        bodies (map
                (fn [a-url] [a-url (-> a-url corpus :body)])
                urls)]
    (map
     (fn [[url body]]
       (dom/refine-xpath body url (first src-xpath) grps))
     bodies)))
