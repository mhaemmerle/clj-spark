package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import clojure.lang.IFn;

public class Function extends org.apache.spark.api.java.function.Function<Object, Object> implements Serializable {

  private static final long serialVersionUID = 7526471155622776147L;
  
  private IFn fn;

  public Function(IFn fn) {
    this.fn = fn;
  }

  public Object call(Object arg) {
    return fn.invoke(arg);
  }

  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
    this.fn = Serialization.deserializeFn(aInputStream);
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    Serialization.serializeFn(aOutputStream, this.fn);
  }
  
}
