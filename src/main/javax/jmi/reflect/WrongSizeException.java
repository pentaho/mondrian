package javax.jmi.reflect;

/** Exception which arises when a collection contains
 * fewer or more values than is required by the corresponding
 * <CODE>Multiplicity.lower</CODE> and <code>Multiplicity.upper</code>.
 */
public class WrongSizeException extends JmiException {
    
    /** Creates <CODE>WrongSizeException</CODE> with a detail message.
     * @param elementInError element in error.
     */    
    public WrongSizeException(RefObject elementInError) {
        super(elementInError);
    }
    
    /** Creates <CODE>WrongSizeException</CODE> with a detail message.
     * @param elementInError element in error.
     * @param msg detail message.
     */    
    public WrongSizeException(RefObject elementInError, String msg) {
        super(elementInError, msg);
    }
}
