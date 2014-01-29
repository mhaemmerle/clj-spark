(ns clj-spark.functions-test
  (:refer-clojure :exclude [fn])
  (:require [clj-spark.api :as k]
            [clj-spark.examples.word-count :refer :all]
            [clojure.test :refer :all]
            [serializable.fn :refer [fn]]))

(deftest test-something
  (k/with-context [context "local" "word-count"]
    (-> (.textFile context "LICENSE")
        (k/map (fn [word] [word 1]))
        (.collect)
        (prn))))
