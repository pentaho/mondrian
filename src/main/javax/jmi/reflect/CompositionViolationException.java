package javax.jmi.reflect;

/** Exception thrown when an instance object is going to become owned by more than one element (in sense of aggregation semantics).
 */
public class CompositionViolationException extends JmiException {
    
    /**
     * Constructs a <code>CompositionViolationException</code> without detail message.
     * @param objectInError An instance (component) that caused the composition violation.
     * @param elementInError Attribute, Reference or Association that is being updated to violate the composition.
     */
    public CompositionViolationException(Object objectInError, RefObject elementInError) {
        super(objectInError, elementInError);
    }
    
    /**
     * Constructs a <code>CompositionViolationException</code> with the specified detail message.
     * @param objectInError An instance (component) that caused the composition voilation.
     * @param elementInError Attribute, Reference or Association that is being updated to violate the composition.
     * @param msg the detail message.
     */
    public CompositionViolationException(Object objectInError, RefObject elementInError, String msg) {
        super(objectInError, elementInError, msg);
    }
}