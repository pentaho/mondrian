/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.xom.XMLUtil;
import mondrian.xom.XOMUtil;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * <code>SAXWriter</code> is a SAX {@link org.xml.sax.ContentHandler}
 * which, perversely, converts its events into an output document.
 *
 * @author jhyde
 * @since 27 April, 2003
 * @version $Id$
 */
public class SAXWriter implements ContentHandler {
    private final PrintWriter pw;
    private int indent;
    private String indentString = "  ";
    private int state = STATE_END_ELEMENT;
    /** Inside the tag of an element. */
    private static final int STATE_IN_TAG = 0;
    /** After the tag at the end of an element. */
    private static final int STATE_END_ELEMENT = 1;
    /** After the tag at the start of an element. */
    private static final int STATE_AFTER_TAG = 2;
    /** After a burst of character data. */
    private static final int STATE_CHARACTERS = 3;

    /**
     * Creates a <code>SAXWriter</code> writing to an {@link OutputStream}.
     */
    public SAXWriter(OutputStream stream) {
        this.pw = new PrintWriter(new OutputStreamWriter(stream));
    }

    /**
     * Creates a <code>SAXWriter</code> writing to a {@link Writer}.
     *
     * <p>If <code>writer</code> is a {@link PrintWriter},
     * {@link #SAXWriter(PrintWriter)} is preferred.
     */
    public SAXWriter(Writer writer) {
        this.pw = new PrintWriter(writer);
    }

    /**
     * Creates a <code>SAXWriter</code> writing to a {@link PrintWriter}.
     * @param writer
     */
    public SAXWriter(PrintWriter writer) {
        this.pw = writer;
    }

    public void setDocumentLocator(Locator locator) {
    }

    public void startDocument()
            throws SAXException {
        pw.println("<?xml version=\"1.0\"?>");
    }

    public void endDocument()
            throws SAXException {
        this.pw.flush();
    }

    public void startPrefixMapping(String prefix, String uri)
            throws SAXException {
    }

    public void endPrefixMapping(String prefix)
            throws SAXException {
    }

    public void startElement(String namespaceURI, String localName,
                             String qName, Attributes atts)
            throws SAXException {
        checkTag();
        if (indent > 0) {
            pw.println();
        }
        for (int i = 0; i < indent; i++) {
            pw.write(indentString);
        }
        indent++;
        pw.write('<');
        pw.write(qName);
        for (int i = 0; i < atts.getLength(); i++) {
            XMLUtil.printAtt(pw, atts.getQName(i), atts.getValue(i));
        }
        state = STATE_IN_TAG;
    }

    private void checkTag() {
        if (state == STATE_IN_TAG) {
            state = STATE_AFTER_TAG;
            pw.print(">");
        }
    }

    public void endElement(String namespaceURI, String localName,
                           String qName)
            throws SAXException {
        indent--;
        if (state == STATE_IN_TAG) {
            pw.write("/>");
        } else {
            if (state != STATE_CHARACTERS) {
                pw.println();
                for (int i = 0; i < indent; i++) {
                    pw.write(indentString);
                }
            }
            pw.write("</");
            pw.write(qName);
            pw.write('>');
        }
        state = STATE_END_ELEMENT;
    }

    public void characters(char ch[], int start, int length)
            throws SAXException {
        checkTag();

        // Display the string, quoting in <![CDATA[ ... ]]> if necessary,
        // or using XML escapes as a last result.
        String s = new String(ch, start, length);
        if (XOMUtil.stringHasXMLSpecials(s)) {
            if (s.indexOf("]]>") < 0) {
                pw.print("<![CDATA[");
                pw.print(s);
                pw.print("]]>");
            } else {
                XMLUtil.stringEncodeXML(s, pw);
            }
        } else {
            pw.print(s);
        }

        state = STATE_CHARACTERS;
    }

    public void ignorableWhitespace(char ch[], int start, int length)
            throws SAXException {
    }

    public void processingInstruction(String target, String data)
            throws SAXException {
    }

    public void skippedEntity(String name)
            throws SAXException {
    }
}

// End SAXWriter.java
