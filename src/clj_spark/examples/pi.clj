(ns clj-spark.examples.pi
  (:gen-class)
  (:refer-clojure :exclude [fn])
  (:require [clojure.string :refer [split]]
            [clj-spark.api :as k]
            [serializable.fn :refer [fn]]))

(defn pi [context n]
  (-> (k/parallelize context (range 1 n))
      (k/map (fn [i]
               (let [x (rand) y (rand)]
                 (if (< (+ (* x x) (* y y)) 1)
                   1 0))))
      (k/reduce +)
      (* 4)
      (/ (double n))))

(defn -main [& args]
  (let [[master n] args]
    (if-not (and master n)
      (println "Usage: pi <master> N")
      (System/exit 1))
    (k/with-context [context master "pi"]
      (prn (pi context n)))))
