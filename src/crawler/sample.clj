(ns crawler.sample
  "Module to sample decision responses.
   A decision is represented by an XPath
   A sample picks out some links. Visiting is left
   to the user"
  (:require [clj-http.client :as client]))

(defn sample-a-decision
  [links blacklist]
  (take 20 (filter (fn [a-link] (some blacklist)) links)))

(defn sample-some-links
  [links blacklist]
  (map
   (fn [a-link]
     (do
       (println :sampling a-link)
       (flush)
       (try (-> a-link
                client/get
                :body)
            (catch Exception e (do (println a-link)
                                   (flush))))))
   (take (max 4 (int (/ (count links)
                        10)))
         (filter
          (fn [a-link]
            (not (some #{a-link} blacklist)))
          (distinct links)))))
