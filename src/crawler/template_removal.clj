(ns crawler.template-removal
  "Generate a preliminary blacklist - comprising template code"
  (:require [crawler.rich-char-extractor :as extractor]
            [clj-http.client :as client]
            [clj-http.cookies :as cookies]))

(def my-cs (cookies/cookie-store))

(defn look-at
  ([a-link cookie]
     (look-at a-link cookie (set [])))

  ([a-link cookie blacklist]
     (try (let [body  (-> a-link (client/get {:cookie-store cookie}) :body)]
            (extractor/state-action body a-link blacklist))
          (catch Exception e nil))))

(defn all-xpaths
  ([page-src url cookie]
     (all-xpaths page-src url (set []) cookie))

  ([page-src url blacklist cookie]
     (let [infos   (extractor/state-action page-src url blacklist)

           samples          (reduce
                             (fn [acc {xpath :xpath _ :score hrefs :hrefs}]
                               (let [picked (take
                                             2
                                             (filter
                                              (fn [a-link]
                                                (and (not (some #{a-link} (:links acc)))
                                                     (not= a-link url)))
                                              hrefs))]
                                 (println :xpath xpath :picked picked)
                                 (if picked
                                   {:links (clojure.set/union (:links acc) (set picked))
                                    :content (cons [xpath picked] (:content acc))}
                                   acc)))
                             {:links (set []) :content (set [])}
                             (:xpath-nav-info infos))

           sampled          (reduce
                             (fn [acc [x l]]
                               (let [action-space (do
                                                    (println :sampling l)
                                                    (map (fn [x]
                                                           (do
                                                             (Thread/sleep 2000)
                                                             (look-at x cookie (set [])))) l))
                                     
                                     action-space-map (into
                                                       {}
                                                       (map
                                                        (fn [{xpath :xpath _ :score hrefs :hrefs}]
                                                          [xpath {:incidence 1 :hrefs [hrefs]}])
                                                        (reduce
                                                         concat
                                                         (map :xpath-nav-info action-space))))]
                                 (merge-with
                                  (fn [x y]
                                    {:incidence (+ (:incidence x)
                                                   (:incidence y))
                                     :hrefs (concat (:hrefs x) (:hrefs y))})
                                  acc action-space-map)))
                             {}
                             (:content samples))
           
           candidates       (into
                             {}
                             (filter
                              (fn [[x info]]
                                (<= 5 (:incidence info)))
                              sampled))

           xpath-link-table (map
                             (fn [{xpath :xpath _ :score hrefs :hrefs}]
                               [xpath (let [obs (:hrefs (candidates xpath))]
                                        (map
                                         (fn [a-link]
                                           [a-link (count
                                                    (filter
                                                     identity
                                                     (map
                                                      #(some #{a-link} %)
                                                      obs)))])
                                         hrefs))])
                             (:xpath-nav-info infos))
           
           xpath-links-candidates (map
                                   (fn [[x ls]]
                                     [x (filter
                                         (fn [[l c]]
                                           (<= 5 c))
                                         ls)])
                                   xpath-link-table)]
       (into
        {} (filter
            #(-> % second not-empty)
            (map
             (fn [[x ls]]
               [x (map first ls)])
             xpath-links-candidates))))))
