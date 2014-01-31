package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import scala.Tuple2;
import clojure.lang.IFn;

public class PairFunction extends org.apache.spark.api.java.function.PairFunction<Object, Object, Tuple2> {
  
  private static final long serialVersionUID = 7526471155622776147L;

  private IFn fn;

  public PairFunction(IFn fn) {
    this.fn = fn;
  }

  @Override
  public Tuple2 call(Object arg) throws Exception {
    List<Object> result = (List<Object>) fn.invoke(arg);
    return new Tuple2<Object, Object>(result.get(0), result.get(1));
  }  
    
  private void readObject(ObjectInputStream input) throws ClassNotFoundException, IOException {
    this.fn = Serialization.deserializeFn(input);
  }

  private void writeObject(ObjectOutputStream output) throws IOException {
    Serialization.serializeFn(output, this.fn);
  }

}
