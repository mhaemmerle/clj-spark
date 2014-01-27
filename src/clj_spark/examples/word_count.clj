(ns clj-spark.examples.word-count
  (:gen-class)
  (:refer-clojure :exclude [fn])
  (:require [clojure.string :refer [split]]
            [clj-spark.api :as k]
            [serializable.fn :refer [fn]]))

(defn word-count [context file]
  (-> (.textFile context file)
      (k/flat-map (fn [x] (split x #"\s+")))
      (k/map (fn [word] [word 1]))
      (k/reduce-by-key +)
      (.collect)))

(defn -main [& args]
  (let [[master file] args]
    (if-not (and master file)
      (println "Usage: word-count <master> <file>")
      (System/exit 1))
    (k/with-context [context master "word-count"]
      (prn (word-count context file)))))
