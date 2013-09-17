(defproject crawler "0.1.0-SNAPSHOT"
  :description "CMU Discussions Crawler"
  :url "http://blog.shriphani.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "0.7.6"]
                 [clj-robots "0.6.0"]
                 [clj-time "0.6.0"]
                 [com.github.kyleburton/clj-xpath "1.4.2"]
                 [enlive "1.1.4"]
                 [misc "0.1.0-SNAPSHOT"]
                 [net.sourceforge.htmlcleaner/htmlcleaner "2.6"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.4"]
                 [org.bovinegenius/exploding-fish "0.3.3"]])
