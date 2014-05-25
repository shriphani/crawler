(ns crawler.test-system
  "Test module."
  (:use [crawler.main :as main]))

(def test-entry-points
  [;; vbulletin
   "http://photography-on-the.net/forum/"
   "http://dgrin.com/"
   "http://forum.vizrt.com/"
   "http://advrider.com/forums/"

   ;; phpbb
   "http://www.ilovephilosophy.com/index.php"
   "http://phpbbarcade.jatek-vilag.com/index.php"
   "http://seattlebubble.com/forum/"
   "https://forum.librivox.org/index.php"
   
   ;; ipboard
   "http://forums.aceteam.cl/"
   "http://www.dfwpilots.com/board/"
   "http://forum.newburytoday.co.uk/"
   "http://forums.dumpshock.com/index.php"

   ;; mybb
   "http://mybbmodding.net/forums/"
   "http://historicshooting.com/mybb/index.php"
   "https://amblesideonline.org/forum/"
   "http://www.doctornerdlove.com/forums/"])

(defn download-corpora
  []
  (pmap
   (fn [site]
     (main/discussion-forum-crawler-3 site 100))
   test-entry-points))

