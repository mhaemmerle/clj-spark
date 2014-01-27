(ns clj-spark.examples.text-search-test
  (:require [clj-spark.api :as k]
            [clj-spark.examples.text-search :refer :all]
            [clojure.test :refer :all]))

(deftest test-text-search
  (k/with-context [context "local" "test-text-search"]
    (is (= 15 (text-search context "LICENSE" "License")))))
