/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.util.SAXHandler;
import mondrian.util.SAXWriter;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.servlet.ServletContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

/**
 * An <code>XmlaMediator</code> responds to XML for Analysis requests.
 *
 * @author jhyde
 * @since 27 April, 2003
 * @version $Id$
 */
public class XmlaMediator {
    private static final String XMLA_NS = "urn:schemas-microsoft-com:xml-analysis";
    static ThreadLocal threadServletContext = new ThreadLocal();

    /**
     * Processes a request.
     * @param request  XML request, for example, "<SOAP-ENV:Envelope ...>".
     * @param response Destination for response
     */
    public void process(String request, Writer response) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder documentBuilder = null;
        try {
            documentBuilder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw Util.newError(e, "Error processing '" + request + "'");
        }
        Document document = null;
        try {
            document = documentBuilder.parse(new InputSource(new StringReader(request)));
        } catch (SAXException e) {
            throw Util.newError(e, "Error processing '" + request + "'");
        } catch (IOException e) {
            throw Util.newError(e, "Error processing '" + request + "'");
        }
        Element documentElement = document.getDocumentElement();
        try {
            process(documentElement, new SAXHandler(new SAXWriter(response)));
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    /**
     * Processes a request, specified as a &lt;Discover&gt; or &lt;Execute&gt;
     * {@link Element DOM element}, and writes the response as a set of SAX
     * events.
     * @param element &lt;Discover&gt; or &lt;Execute&gt;
     *   {@link Element DOM element}
     * @param saxHandler Object to write SAX events to
     */
    public void process(Element element, SAXHandler saxHandler) throws SAXException {
        saxHandler.startDocument();
        processEnvelope(element, saxHandler);
        saxHandler.endDocument();
    }

    private void processEnvelope(Element element, SAXHandler saxHandler) throws SAXException {
        String tagName = element.getLocalName();
        Util.assertTrue(tagName.equals("Envelope"));
        //final NodeList childNodes = element.getChildNodes();
        saxHandler.startElement("SOAP-ENV:Envelope", new String[] {
            "xmlns:SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/",
             "SOAP-ENV:encodingStyle", "http://schemas.xmlsoap.org/soap/encoding/",
        });
        saxHandler.startElement("SOAP-ENV:Body");
        processBody(firstElement(element, "Body"), saxHandler);
        saxHandler.endElement();
        saxHandler.endElement();
    }

    private void processBody(Element element, SAXHandler saxHandler) throws SAXException {
        String tagName = element.getLocalName();
        Util.assertTrue(tagName.equals("Body"));
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            final Node node = childNodes.item(i);
            if (node instanceof Element) {
                try {
                    processRequest((Element) node, saxHandler);
                } catch (Exception e) {
                    saxHandler.startElement("Error");
                    saxHandler.characters(e.toString());
                    saxHandler.endElement();
                }
            }
        }
    }

    private void processRequest(Element element, SAXHandler saxHandler) {
        String tagName = element.getTagName();
        //String tagNs = element.getNamespaceURI();
        if (tagName.equals("Discover")) {
            discover(element, saxHandler);
        } else if (tagName.equals("Execute")) {
            execute(element, saxHandler);
        } else {
            throw Util.newError("Element <" + tagName + "> not supported");
        }
    }

    private void execute(Element execute, SAXHandler saxHandler) {
        Element command = firstElement(execute, "Command");
        if (command == null) {
            throw Util.newError("<Command> parameter is required");
        }
        String statement = firstElementCDATA(command, "Statement");
        if (statement == null) {
            throw Util.newError("<Statement> parameter is required");
        }
        final Properties properties = getProperties(execute);

        try { 
            saxHandler.startElement("ExecuteResponse", new String[] { 
                                        "xmlns", XMLA_NS}); 
            saxHandler.startElement("return"); 
            saxHandler.startElement("root", new String[] { 
                                        "xmlns", XMLA_NS + ":mddataset"}); 
            saxHandler.startElement("xsd:schema", new String[] { 
                                        "xmlns:xsd", "http://www.w3.org/2001/XMLSchema"}); 
            // todo: schema definition 
            saxHandler.endElement(); 

        try { 
            MDDataSet cellSet = executeQuery(statement, properties); 
            cellSet.unparse(saxHandler); 
        } finally { 
            saxHandler.endElement(); 
            saxHandler.endElement(); 
            saxHandler.endElement(); 
        } 
        } catch (SAXException e) { 
            throw Util.newError(e, "Error while processing execute request"); 
        }
    }

    private MDDataSet executeQuery(String statement, Properties properties) {
        final String formatName = properties.getProperty(PropertyDefinition.Format.name);
        Enumeration.Format format = Enumeration.Format.getValue(formatName);
        final String axisFormatName = properties.getProperty(PropertyDefinition.AxisFormat.name);
        Enumeration.AxisFormat axisFormat = Enumeration.AxisFormat.getValue(axisFormatName);
        final Connection connection = getConnection(properties);
        final Query query = connection.parseQuery(statement);
        final Result result = connection.execute(query);
        return new MDDataSet(result, format, axisFormat);
    }

    static class MDDataSet {
        private final Result result;
        private final Enumeration.Format format;
        private final Enumeration.AxisFormat axisFormat;
        private static final String[] cellProps = new String[] {
            "Value",
            "FmtValue",
            "FormatString"};
        private static final String[] cellPropLongs = new String[] {
            Property.VALUE.name,
            Property.FORMATTED_VALUE.name,
            Property.FORMAT_STRING.name};
        private static final String[] props = new String[] {
            "UName",
            "Caption",
            "LName",
            "LNum"};
        private static final String[] propLongs = new String[] {
            Property.MEMBER_UNIQUE_NAME.name,
            Property.MEMBER_CAPTION.name,
            Property.LEVEL_UNIQUE_NAME.name,
            Property.LEVEL_NUMBER.name};

        public MDDataSet(Result result, Enumeration.Format format, Enumeration.AxisFormat axisFormat) {
            this.result = result;
            this.format = format;
            this.axisFormat = axisFormat;
        }

        public void unparse(SAXHandler saxHandler) throws SAXException {
            if (format != Enumeration.Format.Multidimensional) {
                throw new UnsupportedOperationException("<Format>: only 'Multidimensional' currently supported");
            }
            olapInfo(saxHandler);
            axes(saxHandler);
            cellData(saxHandler);
        }

        private void olapInfo(SAXHandler saxHandler) throws SAXException {
            saxHandler.startElement("OlapInfo");
            saxHandler.startElement("CubeInfo");
            saxHandler.startElement("Cube");
            saxHandler.startElement("CubeName");
            saxHandler.characters(result.getQuery().getCube().getName());
            saxHandler.endElement();
            saxHandler.endElement();
            saxHandler.endElement(); // CubeInfo
            // -----------
            saxHandler.startElement("AxesInfo");
            final Axis[] axes = result.getAxes();
            for (int i = 0; i < axes.length; i++) {
                Axis axis = axes[i];
                saxHandler.startElement("AxisInfo", new String[] {
                    "name", "Axis" + i});
                Hierarchy[] hierarchies;
                if (axis.positions.length > 0) {
                    final Position position = axis.positions[0];
                    hierarchies = new Hierarchy[position.members.length];
                    for (int j = 0; j < position.members.length; j++) {
                        Member member = position.members[j];
                        hierarchies[j] = member.getHierarchy();
                    }
                } else {
                    hierarchies = new Hierarchy[0];
                    final QueryAxis queryAxis = this.result.getQuery().axes[i];
                    // todo:
                }
                for (int j = 0; j < hierarchies.length; j++) {
                    saxHandler.startElement("HierarchyInfo", new String[] {
                        "name", hierarchies[j].getName()});
                    for (int k = 0; k < props.length; k++) {
                        saxHandler.element(props[k], new String[] {
                            "name", hierarchies[j].getUniqueName() + ".[" + propLongs[k] + "]"});
                    }
                    saxHandler.endElement(); // HierarchyInfo
                }
                saxHandler.endElement(); // AxisInfo
            }
            saxHandler.endElement(); // AxesInfo
            // -----------
            saxHandler.startElement("CellInfo");
            saxHandler.element("Value", new String[] {
                "name", "VALUE"});
            saxHandler.element("FmtValue", new String[] {
                "name", "FORMATTED_VALUE"});
            saxHandler.element("FormatString", new String[] {
                "name", "FORMAT_STRING"});
            saxHandler.endElement(); // CellInfo
            // -----------
            saxHandler.endElement(); // OLAPInfo
        }

        private void axes(SAXHandler saxHandler) throws SAXException {
            if (axisFormat != Enumeration.AxisFormat.TupleFormat) {
                throw new UnsupportedOperationException("<AxisFormat>: only 'TupleFormat' currently supported");
            }
            saxHandler.startElement("Axes");
            final Axis[] axes = result.getAxes();
            for (int i = 0; i < axes.length; i++) {
                Axis axis = axes[i];
                saxHandler.startElement("Axis", new String[] {
                    "name", "Axis" + i});
                saxHandler.startElement("Tuples");
                for (int j = 0; j < axis.positions.length; j++) {
                    Position position = axis.positions[j];
                    saxHandler.startElement("Tuple");
                    for (int k = 0; k < position.members.length; k++) {
                        Member member = position.members[k];
                        saxHandler.startElement("Member", new String[] {
                            "Hierarchy", member.getHierarchy().getName()});
                        for (int m = 0; m < props.length; m++) {
                            final Object value = member.getPropertyValue(propLongs[m]);
                            if (value != null) {
                                saxHandler.startElement(props[m]); // UName
                                saxHandler.characters(value.toString());
                                saxHandler.endElement(); // UName
                            }
                        }
                        saxHandler.endElement(); // Member
                    }
                    saxHandler.endElement(); // Tuple
                }
                saxHandler.endElement(); // Tuples
                saxHandler.endElement(); // Axis
            }
            saxHandler.endElement(); // Axes
        }

        private void cellData(SAXHandler saxHandler) throws SAXException {
            saxHandler.startElement("CellData");
            final int axisCount = result.getAxes().length;
            int[] pos = new int[axisCount];
            int[] cellOrdinal = new int[] {0};
            recurse(saxHandler, pos, axisCount - 1, cellOrdinal);
            saxHandler.endElement(); // CellData
        }


        private void recurse(SAXHandler saxHandler, int[] pos, int axis, int[] cellOrdinal) throws SAXException {
            final int axisLength = result.getAxes()[axis].positions.length;
            for (int i = 0; i < axisLength; i++) {
                pos[axis] = i;
                if (axis == 0) {
                    saxHandler.startElement("Cell", new String[] {
                        "CellOrdinal", Integer.toString(cellOrdinal[0]++)});
                    final Cell cell = result.getCell(pos);
                    for (int j = 0; j < cellProps.length; j++) {
                        String cellPropLong = cellPropLongs[j];
                        final Object value =
                            cell.getPropertyValue(cellPropLong);
                        if (value != null) {
                            saxHandler.startElement(cellProps[j]);
                            String valueString = value.toString();

                            // This is here because different JDBC drivers
                            // use different Number classes to return
                            // numeric values (Double vs BigDecimal) and
                            // these have different toString() behavior.
                            // If it contains a decimal point, then
                            // strip off trailing '0's. After stripping off
                            // the '0's, if there is nothing right of the
                            // decimal point, then strip off decimal point.
                            if (cellPropLong == Property.VALUE.name &&
                                   value instanceof Number) {
                                int index = valueString.indexOf('.');
                                if (index > 0) {
                                    boolean found = false;
                                    int p = valueString.length();
                                    char c = valueString.charAt(p-1);
                                    while (c == '0') {
                                        found = true;
                                        p--;
                                        c = valueString.charAt(p-1);
                                    }
                                    if (c == '.') {
                                        p--;
                                    }
                                    if (found) {
                                        valueString =
                                            valueString.substring(0, p);
                                    }
                                }
                            }

                            saxHandler.characters(valueString);
                            saxHandler.endElement();
                        }
                    }

                    saxHandler.endElement(); // Cell
                } else {
                    recurse(saxHandler, pos, axis - 1, cellOrdinal);
                }
            }
        }
    }

    private void discover(Element discover, SAXHandler saxHandler) {
        String requestType = firstElementCDATA(discover, "RequestType");
        if (requestType == null) {
            throw Util.newError("<RequestType> parameter is required");
        }
        HashMap restrictionsProperties = getRestrictions(discover);
        Properties propertyProperties = getProperties(discover);
        final RowsetDefinition rowsetDefinition = RowsetDefinition.getValue(requestType);
        Rowset rowset = rowsetDefinition.getRowset(restrictionsProperties, propertyProperties);
        try {
            saxHandler.startElement("DiscoverResponse", new String[] {
                "xmlns", XMLA_NS});
            saxHandler.startElement("return");
            saxHandler.startElement("root", new String[] {
                "xmlns", XMLA_NS + ":rowset"});
            saxHandler.startElement("xsd:schema", new String[] {
                "xmlns:xsd", "http://www.w3.org/2001/XMLSchema"});
            // todo: schema definition
            saxHandler.endElement();
            try {
                rowset.unparse(saxHandler);
            } finally {
                // keep the tags balanced, even if there's an error
                saxHandler.endElement();
                saxHandler.endElement();
                saxHandler.endElement();
            }
        } catch (SAXException e) {
            throw Util.newError(e, "Error while processing '" + requestType + "' discovery request");
        }
    }

    private HashMap getRestrictions(Element discover) {
        Element restrictions = firstElement(discover, "Restrictions");
        if (restrictions == null) {
            throw Util.newError("<Restrictions> parameter is required (but may be empty)");
        }
        Element restrictionList = firstElement(restrictions, "RestrictionList");
        HashMap restrictionsMap = new HashMap();
        if (restrictionList != null) {
            NodeList childNodes = restrictionList.getChildNodes();
            for (int i = 0, n = childNodes.getLength(); i < n; i++) {
                Node childNode = childNodes.item(i);
                if (childNode instanceof Element) {
                    Element childElement = (Element) childNode;
                    String childTag = childElement.getLocalName();
                    Object childValue = getCDATA2(childElement);
                    restrictionsMap.put(childTag, childValue);
                }
            }
        }
        return restrictionsMap;
    }

    /**
     * Returns the &lt;Properties&gt; contained within a &lt;Execute&gt; or
     * &lt;Discover&gt; element.
     */
    private Properties getProperties(Element method) {
        Element properties = firstElement(method, "Properties");
        if (properties == null) {
            throw Util.newError("<Properties> parameter is required (but may be epmty)");
        }
        Element propertyList = firstElement(properties, "PropertyList");
        Properties propertyProperties = new Properties();
        if (propertyList != null) {
            NodeList childNodes = propertyList.getChildNodes();
            for (int i = 0, n = childNodes.getLength(); i < n; i++) {
                Node childNode = childNodes.item(i);
                if (childNode instanceof Element) {
                    Element childElement = (Element) childNode;
                    String childTag = childElement.getLocalName();
                    String childValue = getCDATA(childElement);
                    propertyProperties.setProperty(childTag, childValue);
                }
            }
        }
        return propertyProperties;
    }


    /**
     * Returns a Mondrian connection as specified by a set of properties
     * (especially the "Connect string" property).
     * @param properties
     * @return
     */
    static Connection getConnection(Properties properties) {
        final String dataSourceInfo = properties.getProperty(PropertyDefinition.DataSourceInfo.name);
        Util.PropertyList connectProperties = Util.parseConnectString(dataSourceInfo);
        final String catalog = properties.getProperty(PropertyDefinition.Catalog.name);
        if (catalog != null) {
            connectProperties.put("CatalogName", catalog);
        }
        final ServletContext servletContext =
                (ServletContext) threadServletContext.get();
        return DriverManager.getConnection(connectProperties, servletContext,
                false);
    }

    /**
     * Returns the first child element with a given tag, or null if there are
     * none.
     */
    private Element firstElement(Element element, String tagName) {
        NodeList elements = element.getElementsByTagNameNS("*", tagName);
        for (int i = 0; i < elements.getLength(); i++) {
            Node node = elements.item(i);
            if (node instanceof Element) {
                return (Element) node;
            }
        }
        return null;
    }

    /**
     * Returns the text content of the first child element with a
     * given tag, or null if there is no such child.
     */
    private String firstElementCDATA(Element element, String tagName) {
        Element child = firstElement(element, tagName);
        if (child == null) {
            return null;
        }
        return getCDATA(child);
    }

    private String getCDATA(Element child) {
        child.normalize();
        NodeList childNodes = child.getChildNodes();
        switch (childNodes.getLength()) {
        case 0:
            return "";
        case 1:
            final Text grandChild = (Text) childNodes.item(0);
            return grandChild.getData();
        default:
            StringBuffer sb = new StringBuffer();
            for (int i = 0, n = childNodes.getLength(); i < n; i++) {
                sb.append(childNodes.item(i).toString());
            }
            return sb.toString();
        }
    }

    private Object getCDATA2(Element child) {
        child.normalize();
        NodeList childNodes = child.getChildNodes();
        if (valuesExist(childNodes)) {
            ArrayList list = new ArrayList();
            for (int i = 0, n = childNodes.getLength(); i < n; i++) {
                final Node node = childNodes.item(i);
                if (node instanceof Element &&
                        ((Element) node).getLocalName().equals("Value")) {
                    list.add(getCDATA((Element) node));
                }
            }
            return (String[]) list.toArray(new String[0]);
        } else {
            return getCDATA(child);
        }
    }

    private static boolean valuesExist(NodeList childNodes) {
        for (int i = 0, n = childNodes.getLength(); i < n; i++) {
            final Node node = childNodes.item(i);
            if (node instanceof Element &&
                    ((Element) node).getLocalName().equals("Value")) {
                return true;
            }
        }
        return false;
    }
}

// End XmlaMediator.java
