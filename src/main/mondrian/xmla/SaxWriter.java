/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

/**
 * <code>SaxWriter</code> is similar to a SAX {@link org.xml.sax.ContentHandler}
 * which, perversely, converts its events into an output document.
 *
 * @author jhyde
 * @author Gang Chen
 * @since 27 April, 2003
 * @version $Id$
 */
public interface SaxWriter {

    public void startDocument();

    public void endDocument();

    public void startElement(String name);

    public void startElement(String name, String... attrs);

    public void endElement();

    public void element(String name, String... attrs);

    public void characters(String data);

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
