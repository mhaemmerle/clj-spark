(ns clj-spark.api-test
  (:refer-clojure :exclude [fn])
  (:require [clojure.test :refer :all]
            [clj-spark.api :as k])
  (:import [org.apache.spark.api.java JavaSparkContext]))

(deftest test-context
  (let [context (k/context)]
    (is (instance? JavaSparkContext context))
    (.stop context))
  (let [context (k/context "local" "test-context")]
    (is (instance? JavaSparkContext context))
    (.stop context)))

(deftest test-with-context
  (k/with-context [context "local" "test-with-context"]
    (is (instance? JavaSparkContext context))))
