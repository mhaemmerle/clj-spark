(ns clj-spark.examples.word-count
  (:refer-clojure :exclude [fn])
  (:require [clojure.string :refer [split]]
            [clojure.test :refer :all]
            [clj-spark.api :as k]
            [serializable.fn :refer [fn]]))

(defn word-count [context file]
  (-> (.textFile context file)
      (k/flat-map (fn [x] (split x #"\s+")))
      (k/map (fn [word] [word 1]))
      (k/reduce-by-key +)
      (.collect)))

(deftest test-word-count
  (k/with-context [context "local" "test-word-count"]
    (is (= [["" 27] ["AND" 3] ["making" 1] ["or," 1] ["places:" 1]]
           (take 5 (word-count context "LICENSE"))))))
