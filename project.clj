(defproject crawler "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "0.7.6"]
                 [clj-robots "0.6.0"]
                 [enlive "1.1.4"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure./tools.cli "0.2.4"]
                 [org.bovinegenius/exploding-fish "0.3.3"]]
  :main crawler.core)
