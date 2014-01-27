(ns clj-spark.examples.pi-test
  (:refer-clojure :exclude [fn])
  (:require [clj-spark.api :as k]
            [clj-spark.examples.pi :refer :all]
            [clojure.test :refer :all]))

(deftest test-pi
  (k/with-context [context "local" "test-pi"]
    (is (= "3.14" (format "%.2f" (pi context 100000))))))
