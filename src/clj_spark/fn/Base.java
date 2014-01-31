package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class Base {
  
  private static Var deserializer = null; 
  private static Var serializer = null;
  
  static {
    Var require = RT.var("clojure.core", "require");
    require.invoke(Symbol.create("serializable.fn"));
    deserializer = RT.var("serializable.fn", "deserialize");
    serializer = RT.var("serializable.fn", "serialize");
  }

  public static IFn deserializeFn(ObjectInputStream input) throws IOException {
    int length = input.readInt();
    byte[] serialized = new byte[length];
    input.readFully(serialized);    
    return (IFn) deserializer.invoke(serialized);
  }

  public static void serializeFn(ObjectOutputStream output, IFn fn) throws IOException {    
    byte[] serialized = (byte[]) serializer.invoke(fn);   
    output.writeInt(serialized.length);
    output.write(serialized, 0, serialized.length);
  }

}
