package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import scala.Tuple2;
import clojure.lang.IFn;
import clojure.lang.ISeq;

public class PairFunction<T, K, V> extends org.apache.spark.api.java.function.PairFunction<T, K, V> {
  
  private static final long serialVersionUID = 7526471155622776147L;

  private IFn fn;

  public PairFunction(IFn fn) {
    System.out.println("PairFunction constructor");
    System.out.println(fn);
    this.fn = fn;
  }

  @Override
  public Tuple2<K, V> call(T arg) throws Exception {
    List result = (List) fn.invoke(arg);
    return new Tuple2(result.get(0), result.get(1));
  }  
    
  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
    System.out.println("PairFunction: deserialize");
    this.fn = Base.deserializeFn(aInputStream);
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    System.out.println("PairFunction: serialize");
    Base.serializeFn(aOutputStream, this.fn);
  }

}
