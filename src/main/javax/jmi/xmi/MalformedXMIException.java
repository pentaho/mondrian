package javax.jmi.xmi;

public class MalformedXMIException extends Exception {
    
    /**
     * Creates a new instance of <code>MalformedXMIException</code> without detail message.
     */
    public MalformedXMIException() {
    }
    
    
    /**
     * Constructs an instance of <code>MalformedXMIException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public MalformedXMIException(String msg) {
        super(msg);
    }
}
