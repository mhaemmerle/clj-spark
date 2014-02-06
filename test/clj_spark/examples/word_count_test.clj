(ns clj-spark.examples.word-count-test
  (:refer-clojure :exclude [fn])
  (:require [clj-spark.api :as k]
            [clj-spark.examples.word-count :refer :all]
            [clojure.test :refer :all]))

(deftest test-word-count
  (k/with-context [context "local" "test-word-count"]
    (is (= [["" 27] ["AND" 3] ["making" 1] ["or," 1] ["places:" 1]]
           (->> (k/text-file context "LICENSE")
                (word-count)
                (k/collect)
                (take 5))))))
