(ns index-page-crawler.utils
  (:require [clojure.string :as string]
            (org.bovinegenius [exploding-fish :as uri])
            [subotai.representation :as representation])
  (:use [clj-xpath.core :only [$x:node*]]))

(defn node-attr
  [a-node attr]
  (try (-> a-node
           (.getAttributes)
           (.getNamedItem attr)
           (.getTextContent))
       (catch Exception e nil)))

(defn node-class
  [a-node]
  (let [class (node-attr a-node "class")]
    (try (first
          (string/split class #"\d+|\s+|-|_"))
         (catch Exception _ nil))))

(defn node-position
  [a-node]
  (let [parent (.getParentNode a-node)
        parent-children (.getChildNodes parent)]
    (first
     (filter
      (fn [i]
        (.isSameNode (.item parent-children i)
                     a-node))
      (range
       (.getLength parent-children))))))

(defn build-node-href-pair
  [node src-uri]
  (let [href (node-attr node "href")
        resolved (try (uri/fragment
                       (uri/resolve-uri src-uri href)
                       nil)
                      (catch Exception e nil))]
    [node resolved]))

(defn anchor-nodes
  "Returns a list of anchor nodes
   that might be considered by heritrix as
   outlink candidates"
  [page-src page-uri]
  (let [xml-doc (representation/html->xml-doc page-src)

        all-anchor-nodes ($x:node* ".//a" xml-doc)]
    (filter
     (fn [[n u]]
       (and u
            (= (uri/host u)
               (uri/host page-uri))))
     (map
      (fn [n]
        (build-node-href-pair n page-uri))
      all-anchor-nodes))))

(defn node-path
  ([a-node]
     (node-path a-node []))
  
  ([a-node current-path]
     (if (= (.getNodeName a-node) "#document")
       current-path
       (recur (.getParentNode a-node)
              (cons [(.getNodeName a-node)
                     (node-class a-node)
                     ;(node-position a-node)
                     ]
                    current-path)))))

(defn random-take
  ([n coll]
     (random-take n coll []))

  ([n coll xs]
   (cond (empty? coll)
         xs

         (zero? n)
         xs

         (> n (count coll))
         coll
        
         :else
         (let [taken (rand-nth coll)
               remaining (filter
                          #(not= % taken)
                          coll)]
           (recur (dec n)
                  remaining
                  (cons taken xs))))))

(defn node-xpath
  [a-node]
  (let [path (node-path a-node)]
    (string/join
     "/"
     (map
      (fn [[name class]]
        (if-not class
          name
          (str name "[contains(@class, '" class "')]")))
      path))))
