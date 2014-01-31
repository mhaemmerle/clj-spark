package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import clojure.lang.IFn;

public class FlatMapFunction extends org.apache.spark.api.java.function.FlatMapFunction<Object, Object> {
  
  private static final long serialVersionUID = 7526471155622776147L;

  private IFn fn;

  public FlatMapFunction(IFn fn) {
    System.out.println("FlatMapFunction constructor");
    System.out.println(fn);
    this.fn = fn;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Iterable<Object> call(Object arg) throws Exception {
    return (Iterable<Object>) fn.invoke(arg);
  }
    
  private void readObject(ObjectInputStream input) throws ClassNotFoundException, IOException {
    this.fn = Serialization.deserializeFn(input);
  }

  private void writeObject(ObjectOutputStream output) throws IOException {
    Serialization.serializeFn(output, this.fn);
  }  

}
