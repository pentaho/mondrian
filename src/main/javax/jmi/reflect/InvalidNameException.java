package javax.jmi.reflect;

/** Thrown when a user pases an invalid feature name to a reflective method.
 */
public class InvalidNameException extends JmiException {
    
    private final String invalidName;
    
    /**
     * Constructs an <code>InvalidNameException</code> without detail message.
     * @param invalidName Invalid name.
     */
    public InvalidNameException(String invalidName) {
        this.invalidName = invalidName;
    }

    /**
     * Constructs an <code>InvalidNameException</code> with the specified detail message.
     * @param invalidName Invalid name.
     * @param msg the detail message.
     */
    public InvalidNameException(String invalidName, String msg) {
        super(msg);
        this.invalidName = invalidName;
    }
    
    /** Returns invalid name.
     * @return invalid name.
     */
    public String getInvalidName() {
        return invalidName;
    }
}
