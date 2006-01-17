/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import mondrian.olap.MondrianException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

/**
 * Utility methods for XML/A implementation.
 *
 * @author Gang Chen
 * @version $Id$
 */
public class XmlaUtil {

    /**
     * Invalid characters for XML element name.
     *
     * XML element name:
     *
     * Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
     * S ::= (#x20 | #x9 | #xD | #xA)+
     * NameChar ::= Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender
     * Name ::= (Letter | '_' | ':') (NameChar)*
     * Names ::= Name (#x20 Name)*
     * Nmtoken ::= (NameChar)+
     * Nmtokens ::= Nmtoken (#x20 Nmtoken)*
     *
     */
    private static final String[] CHAR_TABLE = new String[256];

    static {
        initCharTable(" \t\r\n(){}[]+/*%!,?");
    }

    private static void initCharTable(String charStr) {
        char[] chars = charStr.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            CHAR_TABLE[c] = encodeChar(c);
        }
    }

    private static String encodeChar(char c) {
        StringBuffer buf = new StringBuffer();
        buf.append("_x");
        String str = Integer.toHexString(c);
        for (int i = 4 - str.length(); i > 0; i--) {
            buf.append("0");
        }
        return buf.append(str).append("_").toString();
    }

    /**
     * This function is mainly for encode element names in result of Drill Through
     * execute, because its element names come from database, we cannot make sure
     * they are valid XML contents.
     */
    public static String encodeElementName(String name) {
        StringBuffer buf = new StringBuffer();
        char[] nameChars = name.toCharArray();
        for (int i = 0; i < nameChars.length; i++) {
            char ch = nameChars[i];
            String encodedStr = (ch >= CHAR_TABLE.length ? null : CHAR_TABLE[ch]);
            if (encodedStr == null)
                buf.append(ch);
            else
                buf.append(encodedStr);
        }
        return buf.toString();
    }


    public static String element2Text(Element elem) {
        StringWriter writer = new StringWriter();
        try {
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer();
            transformer.transform(new DOMSource(elem), new StreamResult(writer));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return writer.getBuffer().toString();
    }

    public static Element text2Element(String text) {
        return _2Element(new InputSource(new StringReader(text)));
    }

    public static Element stream2Element(InputStream stream) {
        return _2Element(new InputSource(stream));
    }

    private static Element _2Element(InputSource source) {
        Element elem = null;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setIgnoringElementContentWhitespace(true);
            factory.setIgnoringComments(true);
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(source);
            elem = doc.getDocumentElement();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return elem;
    }

    /**
     * @return null if there is no child element.
     */
    public static Element firstChildElement(Element parent,
                                            String ns,
                                            String lname) {
        NodeList nlst = parent.getChildNodes();
        for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
            Node n = nlst.item(i);
            if (n instanceof Element) {
                Element e = (Element) n;
                if ((ns == null || ns.equals(e.getNamespaceURI())) &&
                    (lname == null || lname.equals(e.getLocalName()))) {
                    return e;
                }
            }
        }
        return null;
    }

    public static Element[] filterChildElements(Element parent,
                                                String ns,
                                                String lname) {
        List elems = new ArrayList();
        NodeList nlst = parent.getChildNodes();
        for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
            Node n = nlst.item(i);
            if (n instanceof Element) {
                Element e = (Element) n;
                if ((ns == null || ns.equals(e.getNamespaceURI())) &&
                    (lname == null || lname.equals(e.getLocalName()))) {
                    elems.add(e);
                }
            }
        }
        return (Element[]) elems.toArray(new Element[0]);
    }

    public static String textInElement(Element elem) {
        StringBuffer buf = new StringBuffer();
        elem.normalize();
        NodeList nlst = elem.getChildNodes();
        for (int i = 0, nlen = nlst.getLength(); i < nlen ; i++) {
            Node n = nlst.item(i);
            if (n instanceof Text) {
                buf.append(((Text) n).getData());
            }
        }
        return buf.toString();
    }

    /**
     * Finds root MondrianException in exception chain if exists,
     * otherwise the input throwable.
     */
    public static Throwable rootThrowable(Throwable throwable) {
        Throwable rootThrowable = throwable.getCause();
        if (rootThrowable != null && rootThrowable instanceof MondrianException) {
            return rootThrowable(rootThrowable);
        }
        return throwable;
    }

    public static String normalizeNumricString(String numericStr) {
        // This is here because different JDBC drivers
        // use different Number classes to return
        // numeric values (Double vs BigDecimal) and
        // these have different toString() behavior.
        // If it contains a decimal point, then
        // strip off trailing '0's. After stripping off
        // the '0's, if there is nothing right of the
        // decimal point, then strip off decimal point.
        int index = numericStr.indexOf('.');
        if (index > 0) {
            boolean found = false;
            int p = numericStr.length();
            char c = numericStr.charAt(p - 1);
            while (c == '0') {
                found = true;
                p--;
                c = numericStr.charAt(p - 1);
            }
            if (c == '.') {
                p--;
            }
            if (found)
                return numericStr.substring(0, p);
        }
        return numericStr;
    }

}

// End XmlaUtil.java
