/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde <jhyde@users.sf.net>
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.util;

import mondrian.olap.Util;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.util.Stack;

/**
 * Wrapper around a {@link org.xml.sax.ContentHandler SAX content handler} which
 * makes it easier to generate consistent SAX events.
 *
 * <p>An instance of this class on top of a {@link mondrian.util.SAXWriter} is
 * a convenient way of generating an XML document.
 *
 * @author jhyde
 * @since May 2, 2003
 * @version $Id$
 **/
public class SAXHandler implements ContentHandler {
    private final ContentHandler contentHandler;
    private final Stack stack = new Stack();
    static final Attributes EmptyAttributes = new Attributes() {
        public int getLength() {
            return 0;
        }

        public String getURI(int index) {
            return null;
        }

        public String getLocalName(int index) {
            return null;
        }

        public String getQName(int index) {
            return null;
        }

        public String getType(int index) {
            return null;
        }

        public String getValue(int index) {
            return null;
        }

        public int getIndex(String uri, String localName) {
            return 0;
        }

        public int getIndex(String qName) {
            return 0;
        }

        public String getType(String uri, String localName) {
            return null;
        }

        public String getType(String qName) {
            return null;
        }

        public String getValue(String uri, String localName) {
            return null;
        }

        public String getValue(String qName) {
            return null;
        }
    };

    public SAXHandler(ContentHandler saxHandler) {
        this.contentHandler = saxHandler;
    }

    //
    // ContentHandler methods

    public void startDocument() throws SAXException {
        contentHandler.startDocument();
    }

    public void endDocument() throws SAXException {
        Util.assertTrue(stack.isEmpty());
        contentHandler.endDocument();
    }

    public void setDocumentLocator(Locator locator) {
        contentHandler.setDocumentLocator(locator);
    }

    public void startPrefixMapping(String prefix, String uri)
            throws SAXException {
        contentHandler.startPrefixMapping(prefix, uri);
    }

    public void endPrefixMapping(String prefix)
            throws SAXException {
        contentHandler.endPrefixMapping(prefix);
    }

    public void startElement(String namespaceURI, String localName,
            String qName, Attributes atts)
            throws SAXException {
        contentHandler.startElement(namespaceURI, localName, qName, atts);
    }

    public void endElement(String namespaceURI, String localName,
            String qName)
            throws SAXException {
        contentHandler.endElement(namespaceURI, localName, qName);
    }

    public void characters(char ch[], int start, int length)
            throws SAXException {
        contentHandler.characters(ch, start, length);
    }

    public void ignorableWhitespace(char ch[], int start, int length)
            throws SAXException {
        contentHandler.ignorableWhitespace(ch, start, length);
    }

    public void processingInstruction(String target, String data)
            throws SAXException {
        contentHandler.processingInstruction(target, data);
    }

    public void skippedEntity(String name)
            throws SAXException {
    }

    //
    // Simplifying methods

    public void characters(String s) throws SAXException {
        contentHandler.characters(s.toCharArray(), 0, s.length());
    }

    public void element(String tagName, String[] attributes) throws SAXException {
        startElement(tagName, attributes);
        endElement();
    }

    public void startElement(String tagName) throws SAXException {
        contentHandler.startElement(null, null, tagName, EmptyAttributes);
        stack.push(tagName);
    }

    public void startElement(String tagName, String[] attributes) throws SAXException {
        contentHandler.startElement(null, null, tagName, new StringAttributes(attributes));
        stack.push(tagName);
    }

    public void endElement() throws SAXException {
        String tagName = (String) stack.pop();
        contentHandler.endElement(null, null, tagName);
    }

    /**
     * List of SAX attributes based upon a string array.
     */
    public static class StringAttributes implements Attributes {
        private final String[] strings;

        public StringAttributes(String[] strings) {
            this.strings = strings;
        }

        public int getLength() {
            return strings.length / 2;
        }

        public String getURI(int index) {
            return null;
        }

        public String getLocalName(int index) {
            return null;
        }

        public String getQName(int index) {
            return strings[index * 2];
        }

        public String getType(int index) {
            return null;
        }

        public String getValue(int index) {
            return strings[index * 2 + 1];
        }

        public int getIndex(String uri, String localName) {
            return -1;
        }

        public int getIndex(String qName) {
            final int count = strings.length / 2;
            for (int i = 0; i < count; i++) {
                String string = strings[i * 2];
                if (string.equals(qName)) {
                    return i;
                }
            }
            return -1;
        }

        public String getType(String uri, String localName) {
            return null;
        }

        public String getType(String qName) {
            return null;
        }

        public String getValue(String uri, String localName) {
            return null;
        }

        public String getValue(String qName) {
            final int index = getIndex(qName);
            if (index < 0) {
                return null;
            } else {
                return strings[index * 2 + 1];
            }
        }
    }
}

// End SAXHandler.java