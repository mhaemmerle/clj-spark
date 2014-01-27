(ns clj-spark.examples.text-search
  (:gen-class)
  (:refer-clojure :exclude [fn])
  (:require [clj-spark.api :as k]
            [serializable.fn :refer [fn]]))

(defn text-search [context file word]
  (-> (.textFile context file)
      (k/filter (fn [x] (.contains x word)))
      (k/count)))
