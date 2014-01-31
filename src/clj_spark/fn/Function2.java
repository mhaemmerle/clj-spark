package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import clojure.lang.IFn;

public class Function2 extends org.apache.spark.api.java.function.Function2<Object, Object, Object> {
  
  private static final long serialVersionUID = 7526471155622776147L;
  
  private IFn fn;

  public Function2(IFn fn) {
    this.fn = fn;
  }

  @Override
  public Object call(Object arg1, Object arg2) throws Exception {
    return fn.invoke(arg1, arg2);
  }

  private void readObject(ObjectInputStream input) throws ClassNotFoundException, IOException {
    this.fn = Serialization.deserializeFn(input);
  }

  private void writeObject(ObjectOutputStream output) throws IOException {
    Serialization.serializeFn(output, this.fn);
  }

}
