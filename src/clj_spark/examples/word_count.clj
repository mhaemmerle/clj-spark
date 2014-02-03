(ns clj-spark.examples.word-count
  (:gen-class)
  (:refer-clojure :exclude [fn])
  (:require [clojure.string :refer [split]]
            [clj-spark.api :as k]
            [serializable.fn :refer [fn]]))

(defn word-count [context file]
  (-> (k/text-file context file)
      (k/flat-map (fn [^String x] (seq (.split x " "))))
      (k/map (fn [word] [word 1]))
      (k/reduce-by-key +)
      (k/collect)))

(defn -main [& args]
  (let [[master file & jars] args]
    (when (some nil? [master file])
      (println "Usage: word-count <master> <file>")
      (System/exit 1))
    (k/with-context [context master "word-count" {:jars jars}]
      (prn (word-count context file)))))
