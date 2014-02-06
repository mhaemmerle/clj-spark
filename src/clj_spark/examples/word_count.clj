(ns clj-spark.examples.word-count
  (:gen-class)
  (:refer-clojure :exclude [fn])
  (:require [clojure.string :refer [split]]
            [clj-spark.api :as k]
            [serializable.fn :refer [fn]]))

(defn word-count [lines]
  (-> (k/flat-map lines (fn [^String line] (seq (.split line " "))))
      (k/map (fn [word] [word 1]))
      (k/reduce-by-key +)))

(defn -main [& args]
  (let [jars (k/jars-of-class clj_spark.examples.word_count)
        [master file] args]
    (when (some nil? [master file])
      (println "Usage: word-count <master> <file>")
      (System/exit 1))
    (k/with-context [context master "word-count" {:jars jars}]
      (->> (word-count context file)
           (k/collect)
           (prn)))))
