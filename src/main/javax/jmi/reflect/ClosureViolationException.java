package javax.jmi.reflect;

/** Exception thrown when Composition Closure or Reference Closure rules are
 * violated. (Supertype Closure rule can never be violated in JMI).
 */
public class ClosureViolationException extends JmiException {
    
    /**
     * Constructs an <code>ClosureViolationException</code> without detail message.
     * @param objectInError An instance that violated the closure rule.
     * @param elementInError Reference, Attribute or Association for which the closure rule is violated.
     */
    public ClosureViolationException(Object objectInError, RefObject elementInError) {
        super(objectInError, elementInError);
    }

    /**
     * Constructs an <code>ClosureViolationException</code> with the specified detail message.
     * @param objectInError An instance that violated the closure rule.
     * @param elementInError Reference, Attribute or Association for which the closure rule is violated.
     * @param msg the detail message.
     */
    public ClosureViolationException(Object objectInError, RefObject elementInError, String msg) {
        super(objectInError, elementInError, msg);
    }
}