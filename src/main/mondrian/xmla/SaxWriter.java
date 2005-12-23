package mondrian.xmla;

/**
 * <code>SaxWriter</code> is a SAX {@link org.xml.sax.ContentHandler}
 * which, perversely, converts its events into an output document.
 *
 * @author jhyde
 * @author Gang Chen
 * @since 27 April, 2003
 */
public interface SaxWriter {

    public void startDocument();

    public void endDocument();

    public void startElement(String name);

    public void startElement(String name, String[] attrs);

    public void endElement();

    public void element(String name, String[] attrs);

    public void characters(String data);

    public void completeBeforeElement(String tagName);

}
