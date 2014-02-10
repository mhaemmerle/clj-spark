(ns clj-spark.examples.word-count-kafka
  (:gen-class)
  (:refer-clojure :exclude [fn])
  (:require [clojure.string :refer [split]]
            [clj-spark.api :as k]
            [serializable.fn :refer [fn]])
  (:import scala.Tuple2))

(defn word-count [lines]
  (-> (k/flat-map lines (fn [^:Tuple2 line]
                          (seq (.split (._2 line) " "))))
      (k/map (fn [word] [word 1]))
      (k/reduce-by-key +)))

(defn -main [& args]
  (let [jars (k/jars-of-class clj_spark.examples.word_count_kafka)
        [master zk-quorum group-id & topics] args]
    (when (some nil? [master zk-quorum group-id topics])
      (println "Usage: word-count-kafka <master> <zk-quorum> <group-id> <topics>")
      (System/exit 1))
    (let [topics-map (into {} (for [[k v] (apply hash-map topics)]
                                [k (Integer/parseInt v)]))]
      (k/with-streaming-context [context master "kafka-word-count" {:jars jars}]
        (-> (k/kafka-stream context zk-quorum group-id topics-map)
            (word-count)
            (.print))
        (.start context)
        (.awaitTermination context)))))
