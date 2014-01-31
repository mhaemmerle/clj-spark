package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import clojure.lang.IFn;

public class FlatMapFunction<T, R> extends org.apache.spark.api.java.function.FlatMapFunction<T, R> {
  
  private static final long serialVersionUID = 7526471155622776147L;

  private Object fn;

  public FlatMapFunction(Object fn) {
    System.out.println("FlatMapFunction constructor");
    System.out.println(fn);
    this.fn = fn;
  }

  @Override
  public Iterable<R> call(T arg) throws Exception {
    return (Iterable<R>) ((IFn) fn).invoke(arg);
  }
    
  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
    System.out.println("FlatMapFunction: deserialize");
    this.fn = Base.deserializeFn(aInputStream);
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    System.out.println("FlatMapFunction: serialize");
    Base.serializeFn(aOutputStream, this.fn);
  }  

}
