/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2010 Pentaho
// All Rights Reserved.
*/

package mondrian.xmla;

/**
 * <code>SaxWriter</code> is similar to a SAX {@link org.xml.sax.ContentHandler}
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

    public void startElement(String name, Object... attrs);

    public void endElement();

    public void element(String name, Object... attrs);

    public void characters(String data);

    /**
     * Informs the writer that a sequence of elements of the same name is
     * starting.
     *
     * <p>For XML, is equivalent to {@code startElement(name)}.
     *
     * <p>For JSON, initiates the array construct:
     *
     * <blockquote><code>"name" : [<br/>
     * &nbsp;&nbsp;{ ... },<br/>
     * &nbsp;&nbsp;{ ... }<br/>
     * ]</br></code></blockquote>
     *
     * @param name Element name
     * @param subName Child element name
     */
    public void startSequence(String name, String subName);

    /**
     * Informs the writer that a sequence of elements of the same name has
     * ended.
     */
    public void endSequence();

    /**
     * Generates a text-only element, {@code &lt;name&gt;data&lt;/name&gt;}.
     *
     * <p>For XML, this is equivalent to
     *
     * <blockquote><code>startElement(name);<br/>
     * characters(data);<br/>
     * endElement();</code></blockquote>
     *
     * but for JSON, generates {@code "name": "data"}.
     *
     * @param name Name of element
     * @param data Text content of element
     */
    public void textElement(String name, Object data);

    public void completeBeforeElement(String tagName);

    /**
     * Sends a piece of text verbatim through the writer. It must be a piece
     * of well-formed XML.
     */
    public void verbatim(String text);

    /**
     * Flushes any unwritten output.
     */
    public void flush();
}

// End SaxWriter.java
