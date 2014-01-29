package clj_spark.fn;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Var;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Function<T, R> extends org.apache.spark.api.java.function.Function<T, R> implements Serializable {

    private Object fn;

    public Function(Object fn) {
	System.out.println("Function constructor");
	System.out.println(fn);
	this.fn = fn;
    }

    public Object call(Object arg) {
	System.out.println("Function call");
	return ((IFn) fn).invoke(arg);
    }

    private static final long serialVersionUID = 7526471155622776147L;

    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
	System.out.println("readObject");
	Var deserialize = RT.var("serializable.fn", "deserialize");
	System.out.println(deserialize);
	aInputStream.defaultReadObject();
	this.fn = deserialize.invoke(this.fn);;
    }

    private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
	System.out.println("writeObject");
	Var serialize = RT.var("serializable.fn", "serialize");
	System.out.println(serialize);
	Object fn = this.fn;
	this.fn = serialize.invoke(fn);;
	aOutputStream.defaultWriteObject();
	this.fn = fn;
    }


}
