package javax.jmi.reflect;

/** Exception thrown by <code>refCreateInstance</code> and <i>create</i> methods
 * when a client attempts to create the second instance of a singleton.
 * <BR><B>I think we should rather return the existing instance of singleton
 * from the create method without throwing this exception. This behaviour is
 * more common for singletons.</B>
 */
public class AlreadyExistsException extends JmiException {
    
    private final RefObject existing;
    
    /**
     * Constructs new <code>AlreadyExistsException</code> without detail message.
     * @param existing existing singleton instance
     */
    public AlreadyExistsException(RefObject existing) {
        super(existing.refMetaObject());
        this.existing = existing;
    }


    /**
     * Constructs an <code>AlreadyExistsException</code> with the specified detail message.
     * @param existing existing singleton instance
     * @param msg the detail message.
     */
    public AlreadyExistsException(RefObject existing, String msg) {
        super(existing.refMetaObject(), msg);
        this.existing = existing;
    }
    
    /**
     * Returns existing instance of singleton.
     * @return existing singleton instance
     */
    public RefObject getExistingInstance() {
        return existing;
    }
}