(ns clj-spark.spark.functions
  (:require
   [clj-spark.util]
   [serializable.fn :as sfn])
  (:import
   scala.Tuple2))

;;; Helpers

(defn- serfn?
  [f]
  (= (type f) :serializable.fn/serializable-fn))

(def serialize-fn sfn/serialize)

(def deserialize-fn (memoize sfn/deserialize))

(def array-of-bytes-type (Class/forName "[B"))

;;; Generic

(defn -init
  "Save the function f in state"
  [f]
  [[] f])

(defn -call
  [this & xs]
                                        ; A little ugly that I have to do the deser here, but I tried in the -init fn and it failed.  Maybe it would work in a :post-init?
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (deserialize-fn fn-or-serfn)
            fn-or-serfn)]
    (apply f xs)))

;;; Functions

(defn mk-sym
  [fmt sym-name]
  (symbol (format fmt sym-name)))

(defmacro gen-function
  [clazz wrapper-name]
  (let [new-class-sym (mk-sym "clj_spark.spark.functions.%s" clazz)
        prefix-sym (mk-sym "%s-" clazz)
        ]
    `(do
       (def ~(mk-sym "%s-init" clazz) -init)
       (def ~(mk-sym "%s-call" clazz) -call)
       (gen-class
        :name ~new-class-sym
        :extends ~(mk-sym "org.apache.spark.api.java.function.%s" clazz)
        :implements [java.io.Serializable]
        :prefix ~prefix-sym
        :init ~'init
        :state ~'state
        :constructors {[Object] []})
       (defn ~(symbol (str prefix-sym "writeObject")) [this# out#]
         (prn "WRITE OBJECT"))
       (defn ~(symbol (str prefix-sym "readObject")) [this# in#]
         (prn "READ OBJECT"))
       (defn ~(symbol (str prefix-sym "readObjectNoData")) [this# in#]
         (prn "READ OBJECT NO DATA"))
       (defn ~(symbol (str prefix-sym "toString")) [this#]
         "MOO")
       (defn ~wrapper-name [f#]
         (new ~new-class-sym
              (if (serfn? f#) (serialize-fn f#) f#))))))

(gen-function Function function)

(gen-function VoidFunction void-function)

(gen-function Function2 function2)

(gen-function FlatMapFunction flat-map-function)

(gen-function PairFunction pair-function)

                                        ; Replaces the PairFunction-call defined by the gen-function macro.
(defn PairFunction-call
  [this x]
  (let [[a b] (-call this x)] (Tuple2. a b)))

(defn function
  [fn] (clj_spark.fn.Function. fn))

;; (.call (function (fn [x y] x)) [1 2])

(import java.io.ByteArrayOutputStream)
(import java.io.ObjectOutputStream)

(let [bos (ByteArrayOutputStream.)
      out (ObjectOutputStream. bos)]
  (.writeObject out (clj_spark.fn.Base."f"))
  bos)

;; (import 'clj_spark.fn.Base)
(str (clj_spark.fn.Base."f"))
(class (fn []))
