(ns clj-spark.examples.word-count
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
