(ns clj-spark.api-test
  (:refer-clojure :exclude [fn])
  (:require [clojure.test :refer :all]
            [clj-spark.api :as k]
            [clj-spark.examples.word-count])
  (:import [org.apache.spark.api.java JavaSparkContext]
           [org.apache.spark.streaming.api.java JavaStreamingContext]))

(deftest test-context
  (let [context (k/context)]
    (is (instance? JavaSparkContext context))
    (.stop context))
  (let [context (k/context "local" "test-context")]
    (is (instance? JavaSparkContext context))
    (.stop context)))

(deftest test-streaming-context
  (let [context (k/streaming-context)]
    (is (instance? JavaStreamingContext context))
    (.stop context))
  (let [context (k/streaming-context "local" "test-context")]
    (is (instance? JavaStreamingContext context))
    (.stop context)))

(deftest test-with-context
  (k/with-context [context "local" "test-with-context"]
    (is (instance? JavaSparkContext context))))

(deftest test-with-streaming-context
  (k/with-streaming-context [context "local" "test-with-context"]
    (is (instance? JavaStreamingContext context))))

(deftest test-jar-of-class
  (is (every? string? (k/jars-of-class java.lang.String)))
  (is (empty? (k/jars-of-class clj_spark.examples.word_count))))
