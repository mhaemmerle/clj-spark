package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import clojure.lang.IFn;

public class Function2<T1, T2, R> extends org.apache.spark.api.java.function.Function2<T1, T2, R> {
  
  private static final long serialVersionUID = 7526471155622776147L;
  
  private Object fn;

  public Function2(Object fn) {
    System.out.println("Function constructor");
    System.out.println(fn);
    this.fn = fn;
  }

  @Override
  public R call(T1 arg1, T2 arg2) throws Exception {
    return (R) ((IFn) fn).invoke(arg1, arg2);
  }

  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
    System.out.println("Function2: deserialize");
    this.fn = Base.deserializeFn(aInputStream);
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    System.out.println("Function2: serialize");
    Base.serializeFn(aOutputStream, this.fn);
  }

}
