package clj_spark.fn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Base implements java.io.Serializable {

    public String fn;

    public Base(String fn) {
    	System.out.println("constructor");
	this.fn = fn;
    }

    private static final long serialVersionUID = 7526471155622776147L;

    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
	System.out.println("readObject");
	aInputStream.defaultReadObject();
    }

    private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
	System.out.println("writeObject");
	aOutputStream.defaultWriteObject();
    }

    public String toString() {
	return fn;
    }

}
