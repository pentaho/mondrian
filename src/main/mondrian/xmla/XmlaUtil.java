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

import mondrian.olap.*;
import mondrian.xmla.impl.DefaultXmlaResponse;
import org.apache.log4j.Logger;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.nio.charset.Charset;

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
     * <p>XML element name:
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
    private static final Pattern LOWERCASE_PATTERN = Pattern.compile(".*[a-z].*");

    static {
        initCharTable(" \t\r\n(){}[]+/*%!,?");
    }

    private static void initCharTable(String charStr) {
        char[] chars = charStr.toCharArray();
        for (char c : chars) {
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
     * Encodes an XML element name.
     *
     * <p>This function is mainly for encode element names in result of Drill
     * Through execute, because its element names come from database, we cannot
     * make sure they are valid XML contents.
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
     *
     * @param name Name of XML element
     * @return encoded name
     */
    public static String encodeElementName(String name) {
        StringBuilder buf = new StringBuilder();
        char[] nameChars = name.toCharArray();
        for (char ch : nameChars) {
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
            return doc.getDocumentElement();

        } catch (Exception e) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                USM_DOM_PARSE_CODE,
                USM_DOM_PARSE_FAULT_FS,
                e);
        }
    }

    /**
     * Returns the first child element of an XML element, or null if there is
     * no first child.
     *
     * @param parent XML element
     * @param ns     Namespace
     * @param lname  Local name of child
     * @return First child, or null if there is no child element
     */
    public static Element firstChildElement(
        Element parent,
        String ns,
        String lname)
    {
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

        List<Element> elems = new ArrayList<Element>();
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
        return elems.toArray(new Element[elems.size()]);
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
     *
     * @param throwable Exception
     * @return Root exception
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
     *
     * @param numericStr Numeric string
     * @return Normalized string
     */
    public static String normalizeNumericString(String numericStr) {
        int index = numericStr.indexOf('.');
        if (index > 0) {
            // If it uses exponential notation, 1.0E4, then it could
            // have a trailing '0' that should not be stripped of,
            // e.g., 1.0E10. This would be rather bad.
            if (numericStr.indexOf('e') != -1) {
                return numericStr;
            } else if (numericStr.indexOf('E') != -1) {
                return numericStr;
            }

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

    /**
     * Returns a set of column headings and rows for a given metadata request.
     *
     * <p/>Leverages mondrian's implementation of the XML/A specification, and
     * is exposed here for use by mondrian's olap4j driver.
     *
     * @param connection Connection
     * @param catalogName Catalog name
     * @param methodName Metadata method name per XMLA (e.g. "MDSCHEMA_CUBES")
     * @param restrictionMap Restrictions
     * @return Set of rows and column headings
     */
    public static MetadataRowset getMetadataRowset(
        final Connection connection,
        String catalogName,
        String methodName,
        final Map<String, Object> restrictionMap)
    {
        RowsetDefinition rowsetDefinition = RowsetDefinition.valueOf(methodName);

        final Map<String, String> propertyMap = new HashMap<String, String>();
        final String dataSourceName = "xxx";
        propertyMap.put(
            PropertyDefinition.DataSourceInfo.name(),
            dataSourceName);

        DataSourcesConfig.DataSource dataSource =
            new DataSourcesConfig.DataSource();
        dataSource.name = dataSourceName;
        DataSourcesConfig.DataSources dataSources =
            new DataSourcesConfig.DataSources();
        dataSources.dataSources =
            new DataSourcesConfig.DataSource[] {dataSource};
        DataSourcesConfig.Catalog catalog = new DataSourcesConfig.Catalog();
        catalog.name = catalogName;
        catalog.definition = "dummy"; // any not-null value will do
        dataSource.catalogs =
            new DataSourcesConfig.Catalogs();
        dataSource.catalogs.catalogs =
            new DataSourcesConfig.Catalog[] {catalog};

        Rowset rowset =
            rowsetDefinition.getRowset(
                new XmlaRequest() {
                    public int getMethod() {
                        return METHOD_DISCOVER;
                    }

                    public Map<String, String> getProperties() {
                        return propertyMap;
                    }

                    public Map<String, Object> getRestrictions() {
                        return restrictionMap;
                    }

                    public String getStatement() {
                        return null;
                    }

                    public String getRoleName() {
                        return null;
                    }

                    public Role getRole() {
                        return connection.getRole();
                    }

                    public String getRequestType() {
                        throw new UnsupportedOperationException();
                    }

                    public boolean isDrillThrough() {
                        throw new UnsupportedOperationException();
                    }

                    public int drillThroughMaxRows() {
                        throw new UnsupportedOperationException();
                    }

                    public int drillThroughFirstRowset() {
                        throw new UnsupportedOperationException();
                    }
                },
                new XmlaHandler(
                    dataSources,
                    null,
                    "xmla") {
                    protected Connection getConnection(
                        final DataSourcesConfig.Catalog catalog,
                        final Role role,
                        final String roleName)
                        throws XmlaException
                    {
                        return connection;
                    }
                }
            );
        List<Rowset.Row> rowList = new ArrayList<Rowset.Row>();
        rowset.populate(
            new DefaultXmlaResponse(
                new ByteArrayOutputStream(),
                Charset.defaultCharset().name()),
            rowList);
        MetadataRowset result = new MetadataRowset();
        for (Rowset.Row row : rowList) {
            Object[] values =
                new Object[rowsetDefinition.columnDefinitions.length];
            int k = -1;
            for (RowsetDefinition.Column columnDefinition
                : rowsetDefinition.columnDefinitions)
            {
                Object o = row.get(columnDefinition.name);
                if (o instanceof List) {
                    o = toString((List<String>) o);
                } else if (o instanceof String[]) {
                    o = toString(Arrays.asList((String []) o));
                }
                values[++k] = o;
            }
            result.rowList.add(Arrays.asList(values));
        }
        for (RowsetDefinition.Column columnDefinition
            : rowsetDefinition.columnDefinitions)
        {
            String columnName = columnDefinition.name;
            if (LOWERCASE_PATTERN.matcher(columnName).matches()) {
                columnName = Util.camelToUpper(columnName);
            }
            // VALUE is a SQL reserved word
            if (columnName.equals("VALUE")) {
                columnName = "PROPERTY_VALUE";
            }
            result.headerList.add(columnName);
        }
        return result;
    }

    private static <T> String toString(List<T> list) {
        StringBuilder buf = new StringBuilder();
        int k = -1;
        for (T t : list) {
            if (++k > 0) {
                buf.append(", ");
            }
            buf.append(t);
        }
        return buf.toString();
    }

    /**
     * Result of a metadata query.
     */
    public static class MetadataRowset {
        public final List<String> headerList = new ArrayList<String>();
        public final List<List<Object>> rowList = new ArrayList<List<Object>>();
    }

    /**
     * Wrapper which indicates that a restriction is to be treated as a
     * SQL-style wildcard match.
     */
    public static class Wildcard {
        public final String pattern;

        public Wildcard(String pattern) {
            this.pattern = pattern;
        }
    }

    /**
     * Generates descriptions of the columns returned by each metadata query,
     * in javadoc format, suitable for pasting into
     * <code>OlapDatabaseMetaData</code>.
     */
    public static void generateMetamodelJavadoc() throws IOException {
        PrintWriter pw =
            new PrintWriter(
                new FileWriter("C:/open/mondrian/olap4j_javadoc.java"));
        pw.println("    /**");
        String prefix = "     * ";
        for (RowsetDefinition o : RowsetDefinition.values()) {
            pw.println(prefix);
            pw.println(prefix + "<p>" + o.name() + "</p>");
            pw.println(prefix + "<ol>");
            for (RowsetDefinition.Column columnDefinition : o.columnDefinitions) {
                String columnName = columnDefinition.name;
                if (LOWERCASE_PATTERN.matcher(columnName).matches()) {
                    columnName = Util.camelToUpper(columnName);
                }
                if (columnName.equals("VALUE")) {
                    columnName = "PROPERTY_VALUE";
                }
                String type;
                switch (columnDefinition.type) {
                case StringSometimesArray:
                case EnumString:
                case UUID:
                    type = "String";
                    break;
                case DateTime:
                    type = "Timestamp";
                    break;
                case Boolean:
                    type = "boolean";
                    break;
                case UnsignedLong:
                case Long:
                    type = "long";
                    break;
                case UnsignedInteger:
                case Integer:
                    type = "int";
                    break;
                default:
                    type = columnDefinition.type.name();
                }
                String s = prefix + "<li><b>" + columnName + "</b> "
                    + type
                    + (columnDefinition.nullable
                    ? " (may be <code>null</code>)" 
                    : "")
                    + " => " + columnDefinition.description
                    + "</li>";
                s = s.replaceAll("\n|\r|\r\n", "<br/>");
                final String between =
                    System.getProperty("line.separator") + prefix + "        ";
                pw.println(breakLines(s, 79, between));
            }
            pw.println(prefix + "</ol>");
        }
        pw.close();
    }

    private static String breakLines(
        String s, int lineLen, String between)
    {
        StringBuilder buf = new StringBuilder();
        final int originalLineLen = lineLen;
        while (s.length() > 0) {
            if (s.length() > lineLen) {
                int space = s.lastIndexOf(' ', lineLen);
                if (space >= 0) {
                    // best, break at last space before line limit
                    buf.append(s.substring(0, space));
                    buf.append(between);
                    lineLen = originalLineLen - between.length();
                    s = s.substring(space + 1);
                } else {
                    // next best, break at first space after line limit
                    space = s.indexOf(' ', lineLen);
                    if (space >= 0) {
                        buf.append(s.substring(0, space));
                        buf.append(between);
                        lineLen = originalLineLen - between.length();
                        s = s.substring(space + 1);
                    } else {
                        // worst, keep the whole line
                        buf.append(s);
                        s = "";
                    }
                }
            } else {
                buf.append(s);
                s = "";
            }
        }
        return buf.toString();
    }
}

// End XmlaUtil.java
