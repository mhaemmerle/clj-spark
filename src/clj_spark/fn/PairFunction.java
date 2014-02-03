package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Iterator;

import scala.Tuple2;
import clojure.lang.IFn;

public class PairFunction extends org.apache.spark.api.java.function.PairFunction<Object, Object, Tuple2> {

  private static final long serialVersionUID = 6365733658860940360L;

  private IFn fn;

  public PairFunction(IFn fn) {
    this.fn = fn;
  }

  @Override
  public Tuple2 call(Object arg) throws Exception {
    @SuppressWarnings("unchecked")
    Iterator<Object> iterator = ((Collection<Object>) fn.invoke(arg)).iterator();
    return new Tuple2<Object, Object>(iterator.next(), iterator.next());
  }  
    
  private void readObject(ObjectInputStream input) throws ClassNotFoundException, IOException {
    this.fn = Serialization.deserializeFn(input);
  }

  private void writeObject(ObjectOutputStream output) throws IOException {
    Serialization.serializeFn(output, this.fn);
  }

}
