(ns clj-spark.api
  (:refer-clojure :exclude [map reduce count filter first take distinct])
  (:require [clj-spark.util :as util]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [serializable.fn :as sfn])
  (:import java.util.Comparator
           scala.Tuple2
           [clj_spark.fn Function Function2 FlatMapFunction PairFunction VoidFunction]
           [org.apache.spark.api.java JavaSparkContext JavaRDD JavaRDDLike JavaPairRDD]
           [org.apache.spark.streaming Duration]
           [org.apache.spark.streaming.api.java JavaStreamingContext]))

(defn spark-context
  [& {:keys [master job-name spark-home jars environment]}]
  (log/warn "Deprecated: Use clj-spark.api/context instead!")
  (log/warn "JavaSparkContext" master job-name spark-home jars environment)
  (JavaSparkContext. master job-name spark-home (into-array String jars) environment))

(defn ^JavaSparkContext context
  "Create a new Spark context."
  [& [master app-name & [opts]]]
  (JavaSparkContext.
   (or master "local")
   (or app-name "REPL")
   (or (:spark-home opts) (System/getenv "SPARK_HOME"))
   (into-array String (:jars opts))
   (or (:env opts) {})))

(defmacro with-context
  "Bind `symbol` to a new Spark context, evaluate `body` and stop the
  context afterwards."
  [[symbol master app-name & [opts]] & body]
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

(defn jars-of-class [class]
  (try (seq (JavaSparkContext/jarOfClass class))
       (catch Exception e
         (log/warnf "Can't find JAR for class: %s" class))))

(defn text-file
  [^JavaSparkContext context filename]
  (.textFile context filename))

(defn parallelize
  [^JavaSparkContext context lst]
  (.parallelize context lst))

(defn map
  "Return a new RDD by applying a function to all elements of this RDD."
  [rdd f]
  (.map rdd (Function. f)))

(defn reduce
  "Reduces the elements of this RDD using the specified commutative
  and associative binary operator."
  [rdd f]
  (.reduce rdd (Function2. f)))

(defn flat-map
  "Return a new RDD by first applying a function to all elements of
  this RDD, and then flattening the results."
  [rdd f]
  (.flatMap rdd (FlatMapFunction. f)))

(defn filter
  "Return a new RDD containing only the elements that satisfy a predicate."
  [rdd f]
  (.filter rdd (Function. (ftruthy? f))))

(defn foreach
  "Applies a function f to all elements of this RDD."
  [rdd f]
  (.foreach rdd (VoidFunction. f)))

(defn aggregate
  "Aggregate the elements of each partition, and then the results for
  all the partitions, using given combine functions and a neutral zero
  value."
  [rdd zero-value seq-op comb-op]
  (.aggregate rdd zero-value (Function2. seq-op) (Function2. comb-op)))

(defn fold
  "Aggregate the elements of each partition, and then the results for
  all the partitions, using a given associative function and a neutral
  zero value."
  [rdd zero-value f]
  (.fold rdd zero-value (Function2. f)))

(defn reduce-by-key
  "Merge the values for each key using an associative reduce function."
  [rdd f]
  (-> rdd
      (.map (PairFunction. identity))
      (.reduceByKey (Function2. f))
      (.map (Function. untuple))))

(defn group-by-key
  "Return an RDD of grouped elements."
  [rdd]
  (-> rdd
      (.map (PairFunction. identity))
      .groupByKey
      (.map (Function. untuple))))

(defn sort-by-key
  ([rdd]
     (sort-by-key rdd compare true))
  ([rdd x]
     ;; Note: RDD has a .sortByKey signature with just a Boolean arg,
     ;; but it doesn't seem to work when I try it, bool is ignored.
     (if (instance? Boolean x)
       (sort-by-key rdd compare x)
       (sort-by-key rdd x true)))
  ([rdd compare-fn asc?]
     (-> rdd
         (.map (PairFunction. identity))
         (.sortByKey
          (if (instance? Comparator compare-fn)
            compare-fn
            (comparator compare-fn))
          (util/truthy? asc?))
         (.map (Function. untuple)))))

(defn join
  "Return an RDD containing all pairs of elements with matching keys
  in this and other."
  [rdd ^JavaRDD other]
  (-> rdd
      (.map (PairFunction. identity))
      (.join (.map other (PairFunction. identity)))
      (.map (Function. double-untuple))))

(defn cache
  "Persist this RDD with the default storage level (MEMORY_ONLY)."
  [rdd]
  (.cache rdd))

(defn collect
  "Return a seq that contains all of the elements in this RDD."
  [rdd]
  (.collect rdd))

(defn count
  "Return the number of elements in the RDD."
  [rdd]
  (.count rdd))

(defn distinct
  "Return a new RDD containing the distinct elements in this RDD."
  ([rdd]
     (.distinct rdd))
  ([rdd partitions]
     (.distinct rdd partitions)))

(defn first
  "Return the first element in this RDD."
  [rdd]
  (.first rdd))

(defn glom
  "Return an RDD created by coalescing all elements within each
partition into an array."
  [rdd]
  (.glom rdd))

(defn take
  "Take the first num elements of the RDD."
  [rdd cnt]
  (.take rdd cnt))

;; STREAMING

(defn ^JavaStreamingContext streaming-context
  "Create a new Spark streaming context."
  [& [master app-name & [opts]]]
  (JavaStreamingContext.
   (or master "local")
   (or app-name "REPL")
   (or (:duration opts) (Duration. 1000))
   (or (:spark-home opts) (System/getenv "SPARK_HOME"))
   (into-array String (:jars opts))
   (or (:env opts) {})))

(defmacro with-streaming-context
  "Bind `symbol` to a new Spark streaming context, evaluate `body` and
  stop the execution of the streams afterwards."
  [[symbol master app-name & [opts]] & body]
  `(let [~symbol (streaming-context ~master ~app-name ~opts)]
     (try ~@body
          (finally
            (System/clearProperty "spark.master.port") ;; TODO: Still needed?
            (.stop ~symbol)))))

(defn socket-text-stream
  "Create an input stream from the network source at `hostname` at `port`."
  [dstream hostname port & [storage-level]]
  (.socketTextStream dstream hostname port))
