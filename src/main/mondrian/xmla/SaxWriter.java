/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import org.xml.sax.ContentHandler;

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

    public void startElement(String name, String[] attrs);

    public void endElement();

    public void element(String name, String[] attrs);

    public void characters(String data);

    public void completeBeforeElement(String tagName);
}

// End SaxWriter.java
