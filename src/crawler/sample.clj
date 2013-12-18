(ns crawler.sample
  "Module to sample decision responses.
   A decision is represented by an XPath
   A sample picks out some links. Visiting is left
   to the user")

(defn sample-a-decision
  [links blacklist]
  (take 20 (filter (fn [a-link] (some blacklist)) links)))

(defn sample
  "Given a set of decisions, we produce an instance of
   each decision. In URLspeak:
   a decision is an XPath
   a sample is some links from an XPath

   We are hard-coding samples to 20

   Args:
    decisions: {Xpath -> URLs}
    blacklist: [a set of URLs you don't want to follow]"
  [decisions blacklist]
  (map
   (fn [[a-decision info]]
     (sample-a-decision (map #(:href %) info) blacklist))
   decisions))
