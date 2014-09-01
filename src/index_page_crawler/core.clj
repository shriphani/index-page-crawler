(ns index-page-crawler.core
  "Single threaded crawler module"
  (:require [clj-http.client :as client]
            [clj-http.cookies :as cookies]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :as io]
            [index-page-crawler.utils :as utils]
            (org.bovinegenius [exploding-fish :as uri]))
  (:use [clojure.pprint :only [pprint]]))

(def cookie (cookies/cookie-store))

(def dest-dir (str "/bos/tmp19/spalakod/index-pages/"))

(defn download-with-cookie
  [uri]
  (:body
   (try
     (client/get uri {:cookie-store cookie
                      :headers {"User Agent"
                                (str
                                 "Mozilla/5.0 (Windows NT 6.1;) Gecko/20100101 Firefox/13.0.1; "
                                 "Mandalay/version 1 "
                                 "http://boston.lti.cs.cmu.edu/crawler_12/"
                                 )}})
    (catch Exception e nil))))

(defn write-to-disk
  [url body writer]
  (pprint {:uri url
           :time (c/to-long
                  (t/now))
           :body body}
          writer))

(defn iterate-index-pages
  ([page-src page-uri xpath writer]
     (iterate-index-pages page-src
                          page-uri
                          xpath
                          (set [])
                          writer))

  ([page-src page-uri xpath pages writer]
     (let [anchors (utils/anchor-nodes page-src
                                       page-uri)

           anchors-at-xpath (filter
                             (fn [[n u]]
                               (not
                                (some #{(.getTextContent n)}
                                      pages)))
                             
                             ;; only anchors with digits as anchor-text
                             (filter
                              (fn [[n u]]
                                (re-find #"^\d+$" (.getTextContent n)))
                              
                              ;; only anchors at xpath
                              (filter
                               (fn [[n u]]
                                 (= (utils/node-xpath n)
                                    xpath))
                               anchors)))

           [node link] (first anchors-at-xpath)
           _ (println :downloading link)
           new-pg-src (if link (download-with-cookie link) nil)
           new-pg (if node (.getTextContent node) nil)]
       (do (write-to-disk link
                          new-pg-src
                          writer) ; write
           (Thread/sleep (+ 1500 (rand-int 8500))) ; sleep

           ; iterate
           (when link
            (recur new-pg-src
                   link
                   xpath
                   (clojure.set/union pages
                                      (set [new-pg]))
                   writer))))))

(defn crawl-index-pages
  [landing-uri index-xpath]
  (let [_ (download-with-cookie landing-uri)
        body (download-with-cookie landing-uri)
        writer (io/writer (str dest-dir
                               (uri/host landing-uri)
                               ".pages")
                          :append
                          true)]
    (iterate-index-pages body
                         landing-uri
                         index-xpath
                         writer)))


