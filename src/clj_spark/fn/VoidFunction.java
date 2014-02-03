package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import clojure.lang.IFn;

public class VoidFunction extends org.apache.spark.api.java.function.VoidFunction<Object> implements Serializable {

  private static final long serialVersionUID = -6168279019143076294L;

  private IFn fn;

  public VoidFunction(IFn fn) {
    this.fn = fn;
  }

  public void call(Object arg) {
    fn.invoke(arg);
  }

  private void readObject(ObjectInputStream input) throws ClassNotFoundException, IOException {
    this.fn = Serialization.deserializeFn(input);
  }

  private void writeObject(ObjectOutputStream output) throws IOException {
    Serialization.serializeFn(output, this.fn);
  }

}
