
package javax.jmi.reflect;

/** Ancestor for all JMI exceptions thrown by reflective and generated
 * methods.
 */
public abstract class JmiException extends RuntimeException {
    private final RefObject elementInError;
    private final Object objectInError;
    
    /**
     * Constructs new <code>JmiException</code> without detail message.
     */
    public JmiException() {
        this(null, (RefObject) null);
    }
    
    /**
     * Constructs an <code>JmiException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public JmiException(String msg) {
        this(null, null, msg);
    }
    
    /**
     * Constructs new <code>JmiException</code> without detail message.
     * @param elementInError JMI object in error.
     */    
    public JmiException(RefObject elementInError) {
        this(null, elementInError);
    }

    /**
     * Constructs an <code>JmiException</code> with the specified detail message.
     * @param elementInError JMI object in error.
     * @param msg the detail message.
     */
    public JmiException(RefObject elementInError, String msg) {
        this(null, elementInError, msg);
    }
    
    /**
     * Constructs new <code>JmiException</code> without detail message.
     * @param objectInError Value that caused the error.
     * @param elementInError JMI object in error.
     */    
    public JmiException(Object objectInError, RefObject elementInError) {
        super();
        this.objectInError = objectInError;
        this.elementInError = elementInError;
    }
    
    /**
     * Constructs an <code>JmiException</code> with the specified detail message.
     * @param objectInError Value that caused the error.
     * @param elementInError JMI object in error.
     * @param msg the detail message.
     */
    public JmiException(Object objectInError, RefObject elementInError, String msg) {
        super(msg);
        this.objectInError = objectInError;
        this.elementInError = elementInError;
    }
    
    /**
     * Returns element in error.
     * @return element in error.
     */
    public RefObject getElementInError() {
        return elementInError;
    }
    
    /**
     * Returns object in error.
     * @return object in error
     */
    public Object getObjectInError() {
        return objectInError;
    }
}