package javax.jmi.reflect;

/** Exception thrown when an instance object is a component of itself.
 */
public class CompositionCycleException extends JmiException {
    
    /**
     * Constructs an <code>CompositionCycleException</code> without detail message.
     * @param objectInError An instance that caused the composition cycle.
     * @param elementInError Attribute, Reference or Association that is being updated to form a cycle.
     */
    public CompositionCycleException(Object objectInError, RefObject elementInError) {
        super(objectInError, elementInError);
    }
    
    /**
     * Constructs an <code>CompositionCycleException</code> with the specified detail message.
     * @param objectInError An instance that caused the composition cycle.
     * @param elementInError Attribute, Reference or Association that is being updated to form a cycle.
     * @param msg the detail message.
     */
    public CompositionCycleException(Object objectInError, RefObject elementInError, String msg) {
        super(objectInError, elementInError, msg);
    }
}