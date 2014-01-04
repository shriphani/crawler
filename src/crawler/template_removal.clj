(ns crawler.template-removal
  "Generate a preliminary blacklist - comprising template code"
  (:require [crawler.rich-char-extractor :as extractor]
            [clj-http.client :as client]
            [clj-http.cookies :as cookies]))

(def my-cs (cookies/cookie-store))

(defn look-at
  ([a-link]
     (look-at a-link (set [])))

  ([a-link blacklist]
     (try (let [body  (-> a-link (client/get {:cookie-store my-cs}) :body)]
            (extractor/state-action body a-link blacklist))
          (catch Exception e nil))))

(defn all-xpaths
  ([page-src url]
     (all-xpaths page-src url (set [])))

  ([page-src url blacklist]
     (let [infos   (extractor/state-action page-src url blacklist)
           _ (println :infos infos)
           samples (reduce
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
                           :content (conj (:content acc) [xpath picked])}
                          acc)))
                    {:links (set []) :content (set [])}
                    (:xpath-nav-info infos))

           sampled (into
                    {}
                    (reduce
                     (fn [acc [x l]]
                       (let [action-space (do
                                            (println :sampling l)
                                            (map (fn [x]
                                                   (do
                                                     (Thread/sleep 2000)
                                                     (look-at x))) l))

                             action-space-map (into
                                               {}
                                               (map
                                                (fn [{xpath :xpath _ :score hrefs :hrefs}]
                                                  [xpath {:incidence 1 :hrefs (set hrefs)}])
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
                     (:content samples)))
           res      (map
                     (fn [{xpath :xpath
                          _     :score
                          hrefs :hrefs}]
                       (let [diff (count (clojure.set/difference
                                          (set (:hrefs (sampled xpath)))
                                          (set hrefs)))]
                         {:xpath    xpath
                          :changed  diff
                          :diff-links (clojure.set/difference
                                       (set (:hrefs (sampled xpath)))
                                       (set hrefs))
                          :orig     (set hrefs)
                          :observed (:incidence (sampled xpath))
                          :samples  (reduce
                                     (fn [acc [x ls]]
                                       (+ acc (count (filter identity ls))))
                                     0
                                     (:content samples))}))
                     (:xpath-nav-info infos))

           cands    (filter
                     (fn [x]
                       (and (-> x :changed zero?)
                            (-> x :observed)
                            (-> x :observed (>= 5))))
                     res)]
       sampled)))
