(ns clj-spark.examples.word-count-network
  (:gen-class)
  (:require [clj-spark.api :as k]
            [clj-spark.examples.word-count :refer [word-count]]))

(defn -main [& args]
  (let [jars (k/jars-of-class clj_spark.examples.word_count_network)
        [master hostname port] args]
    (when (some nil? [master hostname port])
      (println "Usage: word-count-network <master> <hostname> <port>")
      (System/exit 1))
    (k/with-streaming-context [context master "network-word-count" {:jars jars}]
      (-> (k/socket-text-stream context hostname (Long/parseLong port))
          (word-count)
          (.print))
      (.start context)
      (.awaitTermination context))))
