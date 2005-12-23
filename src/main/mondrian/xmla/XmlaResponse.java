package mondrian.xmla;


/**
 * XML/A response interface.
 *
 * @author Gang Chen
 */
public interface XmlaResponse {

    /**
     * Report XML/A error (not SOAP fault).
     */
    public void error(Throwable t);

    /**
     * Get helper for writing XML document.
     */
    public SaxWriter getWriter();
}

// End XmlaResponse.java
