/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, May 2, 2003
*/
package mondrian.xmla;

import mondrian.olap.MondrianException;
import org.apache.log4j.Logger;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for XML/A implementation.
 *
 * @author Gang Chen
 * @version $Id$
 */
public class XmlaUtil implements XmlaConstants {

    private static final Logger LOGGER = Logger.getLogger(XmlaUtil.class);
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
        StringBuilder buf = new StringBuilder();
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
     *
     * <p>Quoth the <a href="http://xmla.org">XML/A specification</a>, version
     * 1.1:
     * <blockquote>
     * XML does not allow certain characters as element and attribute names.
     * XML for Analysis supports encoding as defined by SQL Server 2000 to
     * address this XML constraint. For column names that contain invalid XML
     * name characters (according to the XML 1.0 specification), the nonvalid
     * Unicode characters are encoded using the corresponding hexadecimal
     * values. These are escaped as _x<i>HHHH_</i> where <i>HHHH</i> stands for
     * the four-digit hexadecimal UCS-2 code for the character in
     * most-significant bit first order. For example, the name "Order Details"
     * is encoded as Order_<i>x0020</i>_Details, where the space character is
     * replaced by the corresponding hexadecimal code.
     * </blockquote>
     */
    public static String encodeElementName(String name) {
        StringBuilder buf = new StringBuilder();
        char[] nameChars = name.toCharArray();
        for (int i = 0; i < nameChars.length; i++) {
            char ch = nameChars[i];
            String encodedStr = (ch >= CHAR_TABLE.length ? null : CHAR_TABLE[ch]);
            if (encodedStr == null) {
                buf.append(ch);
            } else {
                buf.append(encodedStr);
            }
        }
        return buf.toString();
    }


    public static String element2Text(Element elem)
            throws XmlaException {
        StringWriter writer = new StringWriter();
        try {
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer();
            transformer.transform(new DOMSource(elem), new StreamResult(writer));
        } catch (Exception e) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                USM_DOM_PARSE_CODE,
                USM_DOM_PARSE_FAULT_FS,
                e);
        }
        return writer.getBuffer().toString();
    }

    public static Element text2Element(String text)
            throws XmlaException {
        return _2Element(new InputSource(new StringReader(text)));
    }

    public static Element stream2Element(InputStream stream)
            throws XmlaException {
        return _2Element(new InputSource(stream));
    }

    private static Element _2Element(InputSource source)
            throws XmlaException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setIgnoringElementContentWhitespace(true);
            factory.setIgnoringComments(true);
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(source);
            Element elem = doc.getDocumentElement();
            return elem;

        } catch (Exception e) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                USM_DOM_PARSE_CODE,
                USM_DOM_PARSE_FAULT_FS,
                e);
        }
    }

    /**
     * @return null if there is no child element.
     */
    public static Element firstChildElement(Element parent,
                                            String ns,
                                            String lname) {
        if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(100);
            buf.append("XmlaUtil.firstChildElement: ");
            buf.append(" ns=\"");
            buf.append(ns);
            buf.append("\", lname=\"");
            buf.append(lname);
            buf.append("\"");
            LOGGER.debug(buf.toString());
        }
        NodeList nlst = parent.getChildNodes();
        for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
            Node n = nlst.item(i);
            if (n instanceof Element) {
                Element e = (Element) n;

                if (LOGGER.isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(100);
                    buf.append("XmlaUtil.firstChildElement: ");
                    buf.append(" e.getNamespaceURI()=\"");
                    buf.append(e.getNamespaceURI());
                    buf.append("\", e.getLocalName()=\"");
                    buf.append(e.getLocalName());
                    buf.append("\"");
                    LOGGER.debug(buf.toString());
                }

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

/*
way too noisy
        if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(100);
            buf.append("XmlaUtil.filterChildElements: ");
            buf.append(" ns=\"");
            buf.append(ns);
            buf.append("\", lname=\"");
            buf.append(lname);
            buf.append("\"");
            LOGGER.debug(buf.toString());
        }
*/

        List elems = new ArrayList();
        NodeList nlst = parent.getChildNodes();
        for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
            Node n = nlst.item(i);
            if (n instanceof Element) {
                Element e = (Element) n;

/*
                if (LOGGER.isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(100);
                    buf.append("XmlaUtil.filterChildElements: ");
                    buf.append(" e.getNamespaceURI()=\"");
                    buf.append(e.getNamespaceURI());
                    buf.append("\", e.getLocalName()=\"");
                    buf.append(e.getLocalName());
                    buf.append("\"");
                    LOGGER.debug(buf.toString());
                }
*/

                if ((ns == null || ns.equals(e.getNamespaceURI())) &&
                    (lname == null || lname.equals(e.getLocalName()))) {
                    elems.add(e);
                }
            }
        }
        return (Element[]) elems.toArray(new Element[0]);
    }

    public static String textInElement(Element elem) {
        StringBuilder buf = new StringBuilder(100);
        elem.normalize();
        NodeList nlst = elem.getChildNodes();
        for (int i = 0, nlen = nlst.getLength(); i < nlen ; i++) {
            Node n = nlst.item(i);
            if (n instanceof Text) {
                final String data = ((Text) n).getData();
                buf.append(data);
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

    /**
     * Corrects for the differences between numeric strings arising because
     * JDBC drivers use different representations for numbers
     * ({@link Double} vs. {@link java.math.BigDecimal}) and
     * these have different toString() behavior.
     *
     * <p>If it contains a decimal point, then
     * strip off trailing '0's. After stripping off
     * the '0's, if there is nothing right of the
     * decimal point, then strip off decimal point.
     */
    public static String normalizeNumericString(String numericStr) {
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
            if (found) {
                return numericStr.substring(0, p);
            }
        }
        return numericStr;
    }

    public static String encodeBase64(byte[] bytes) {
        // This uses Sun's private encodes, we need to find public
        // implementation that can be added to Mondrian.
        sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
        String encodedStr = encoder.encodeBuffer(bytes);
        return encodedStr;
    }

    public static byte[] decodeBase64(String arg) throws IOException {
        // This uses Sun's private encodes, we need to find public
        // implementation that can be added to Mondrian.
        sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
        byte[] bytes = decoder.decodeBuffer(arg);
        return bytes;
    }
}

// End XmlaUtil.java
