(ns clj-spark.examples.text-search
  (:refer-clojure :exclude [fn])
  (:require [clojure.test :refer :all]
            [clj-spark.api :as k]
            [serializable.fn :refer [fn]]))

(defn text-search [context file word]
  (-> (.textFile context file)
      (k/filter (fn [x] (.contains x word)))
      (k/count)))

(deftest test-text-search
  (k/with-context [context "local" "test-text-search"]
    (is (= 15 (text-search context "LICENSE" "License")))))
