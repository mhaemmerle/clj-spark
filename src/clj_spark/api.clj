(ns clj-spark.api
  (:use clj-spark.spark.functions)
  (:refer-clojure :exclude [map reduce count filter first take distinct])
  (:require
   [clojure.tools.logging :as log]
   [serializable.fn :as sfn]
   [clojure.string :as s]
   [clj-spark.util :as util])
  (:import java.util.Comparator
           scala.Tuple2
           [clj_spark.fn Function Function2 FlatMapFunction PairFunction]
           [org.apache.spark.api.java JavaSparkContext JavaRDD]))

                                        ; Helpers

(defn spark-context
  [& {:keys [master job-name spark-home jars environment]}]
  (log/warn "JavaSparkContext" master job-name spark-home jars environment)
  (JavaSparkContext. master job-name spark-home (into-array String jars) environment))

(defn context
  "Create a new Spark context."
  [& [master app-name & [opts]]]
  (JavaSparkContext.
   (or master "local")
   (or app-name "REPL")
   (or (:spark-home opts) (System/getenv "SPARK_HOME"))
   (into-array String (:jars opts))
   (or (:env opts) {})))

(defmacro with-context [[symbol master app-name & [opts]] & body]
  `(let [~symbol (context ~master ~app-name ~opts)]
     (try ~@body
          (finally (.stop ~symbol)))))

(defn- untuple
  [^Tuple2 t]
  [(._1 t) (._2 t)])

(defn- double-untuple
  "Convert (k, (v, w)) to [k [v w]]."
  [t]
  (let [[x t2] (untuple t)]
    (vector x (untuple t2))))

(def csv-split util/csv-split)

(defn ftopn
  "Return a fn that takes (key, values), sorts the values in DESC order,
  and takes the top N values.  Returns (k, top-values)."
  [n]
  (fn [[k values]]
    (vector k (->> values (sort util/rcompare) (clojure.core/take n)))))

(defn fchoose
  [& indices]
  (fn [coll]
    (util/choose coll indices)))

(defn ftruthy?
  [f]
  (sfn/fn [x] (util/truthy? (f x))))

(defn feach
  "Mostly useful for parsing a seq of Strings to their respective types.  Example
  (k/map (k/feach as-integer as-long identity identity as-integer as-double))
  Implies that each entry in the RDD is a sequence of 6 things.  The first element should be
  parsed as an Integer, the second as a Long, etc.  The actual functions supplied here can be
  any arbitray transformation (e.g. identity)."
  [& fs]
  (fn [coll]
    (clojure.core/map (fn [f x] (f x)) fs coll)))

                                        ; RDD construction

(defn text-file
  [^JavaSparkContext context filename]
  (.textFile context filename))

(defn parallelize
  [^JavaSparkContext context lst]
  (.parallelize context lst))

                                        ; Transformations

(defn echo-types
                                        ; TODO make this recursive
  [c]
  (if (coll? c)
    (println "TYPES" (clojure.core/map type c))
    (println "TYPES" (type c)))
  c)

(defn trace
  [msg]
  (fn [x]
    (prn "TRACE" msg x)
    x))

(defn map
  [^JavaRDD rdd f]
  (.map rdd (Function. f)))

(defn reduce
  [^JavaRDD rdd f]
  (.reduce rdd (Function2. f)))

(defn flat-map
  [^JavaRDD rdd f]
  (.flatMap rdd (FlatMapFunction. f)))

(defn filter
  [^JavaRDD rdd f]
  (.filter rdd (Function. (ftruthy? f))))

(defn foreach
  [^JavaRDD rdd f]
  (.foreach rdd (void-function f)))

(defn aggregate
  [^JavaRDD rdd zero-value seq-op comb-op]
  (.aggregate rdd zero-value (function2 seq-op) (function2 comb-op)))

(defn fold
  [^JavaRDD rdd zero-value f]
  (.fold rdd zero-value (function2 f)))

(defn reduce-by-key
  [^JavaRDD rdd f]
  (-> rdd
      (.map (PairFunction. identity))
      (.reduceByKey (Function2. f))
      (.map (Function. untuple))))

(defn group-by-key
  [^JavaRDD rdd]
  (-> rdd
      (.map (PairFunction. identity))
      .groupByKey
      (.map (Function. untuple))))

(defn sort-by-key
  ([^JavaRDD rdd]
     (sort-by-key rdd compare true))
  ([^JavaRDD rdd x]
                                        ; Note: RDD has a .sortByKey signature with just a Boolean arg, but it doesn't
                                        ; seem to work when I try it, bool is ignored.
     (if (instance? Boolean x)
       (sort-by-key rdd compare x)
       (sort-by-key rdd x true)))
  ([^JavaRDD rdd compare-fn asc?]
     (-> rdd
         (.map (PairFunction. identity))
         (.sortByKey
          (if (instance? Comparator compare-fn)
            compare-fn
            (comparator compare-fn))
          (util/truthy? asc?))
         (.map (Function. untuple)))))

(defn join
  [^JavaRDD rdd ^JavaRDD other]
  (-> rdd
      (.map (PairFunction. identity))
      (.join (.map other (PairFunction. identity)))
      (.map (Function. double-untuple))))

                                        ; Actions

(def first (memfn first))

(def count (memfn count))

(def glom (memfn glom))

(def cache (memfn cache))

(def collect (memfn collect))

                                        ; take defined with memfn fails with an ArityException, so doing this instead:
(defn take
  [^JavaRDD rdd cnt]
  (.take rdd cnt))

(def distinct (memfn distinct))
