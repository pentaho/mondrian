/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2003 Julian Hyde
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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Properties;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * An <code>XmlaMediator</code> responds to XML for Analysis requests.
 *
 * @author jhyde
 * @since 27 April, 2003
 * @version $Id$
 */
public class XmlaMediator {
    private static final String XMLA_NS = "urn:schemas-microsoft-com:xml-analysis";

    /**
     * Processes a request.
     * @param request
     * @param response
     */
    public void process(String request, OutputStream response) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
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
        String tagName = element.getTagName();
        Util.assertTrue(tagName.equals("SOAP-ENV:Envelope"));
        //final NodeList childNodes = element.getChildNodes();
        saxHandler.startElement("SOAP-ENV:Envelope", new String[] {
            "xmlns:SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/",
             "SOAP-ENV:encodingStyle", "http://schemas.xmlsoap.org/soap/encoding/",
        });
        saxHandler.startElement("SOAP-ENV:Body");
        processBody(firstElement(element, "SOAP-ENV:Body"), saxHandler);
        saxHandler.endElement();
        saxHandler.endElement();
    }

    private void processBody(Element element, SAXHandler saxHandler) throws SAXException {
        String tagName = element.getTagName();
        Util.assertTrue(tagName.equals("SOAP-ENV:Body"));
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
        MDDataSet cellSet = executeQuery(statement, properties);
        try {
            cellSet.unparse(saxHandler);
        } catch (SAXException e) {
            e.printStackTrace();  //To change body of catch statement use Options | File Templates.
        }
    }

    private MDDataSet executeQuery(String statement, Properties properties) {
        final String dataSourceName = properties.getProperty(PropertyDefinition.DataSourceInfo.name_);
        final String catalog = properties.getProperty(PropertyDefinition.Catalog.name_);
        final String formatName = properties.getProperty(PropertyDefinition.Format.name_);
        Enumeration.Format format = Enumeration.Format.getValue(formatName);
        final String axisFormatName = properties.getProperty(PropertyDefinition.AxisFormat.name_);
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
            Property.PROPERTY_VALUE,
            Property.PROPERTY_FORMATTED_VALUE,
            Property.PROPERTY_FORMAT_STRING};
        private static final String[] props = new String[] {
            "UName",
            "Caption",
            "LName",
            "LNum"};
        private static final String[] propLongs = new String[] {
            Property.PROPERTY_MEMBER_UNIQUE_NAME,
            Property.PROPERTY_MEMBER_CAPTION,
            Property.PROPERTY_LEVEL_UNIQUE_NAME,
            Property.PROPERTY_LEVEL_NUMBER};

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
            saxHandler.startElement("OLAPInfo");
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
                if (axis.positions.length > 0) {
                    final Position position = axis.positions[0];
                    for (int j = 0; j < position.members.length; j++) {
                        Member member = position.members[j];
                        saxHandler.startElement("HierarchyInfo", new String[] {
                            "name", member.getHierarchy().getName()});
                        for (int k = 0; k < props.length; k++) {
                            saxHandler.element(props[k], new String[] {
                                "name", member.getHierarchy().getUniqueName() + ".[" + propLongs[k] + "]"});
                        }
                        saxHandler.endElement(); // HierarchyInfo
                    }
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
            int[] pos = new int[] {axisCount};
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
                        final Object value = cell.getPropertyValue(cellPropLongs[j]);
                        if (value != null) {
                            saxHandler.startElement(cellProps[j]);
                            saxHandler.characters(value.toString());
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
                "xmlns", "urn:schemas-microsoft-com:xml-analysis"});
            saxHandler.startElement("return");
            saxHandler.startElement("root", new String[] {
                "xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset"});
            saxHandler.startElement("xsd:schema", new String[] {
                "xmlns:xsd", "http://www.w3.org/2001/XMLSchema"});
            // todo: schema definition
            saxHandler.endElement();
            rowset.unparse(saxHandler);
            saxHandler.endElement();
            saxHandler.endElement();
            saxHandler.endElement();
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
                    String childTag = childElement.getTagName();
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
                    String childTag = childElement.getTagName();
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
        final String dataSourceInfo = properties.getProperty("DataSourceInfo");
        return DriverManager.getConnection(dataSourceInfo, null, false);
    }

    /**
     * Returns the first child element with a given tag, or null if there are
     * none.
     */
    private Element firstElement(Element element, String tagName) {
        NodeList elements = element.getElementsByTagName(tagName);
        for (int i = 0; i < elements.getLength(); i++) {
            Node node = elements.item(i);
            if (node instanceof Element) {
                return (Element) node;
            }
        }
        return null;
    }

    /**
     * Returns the text content of the first child element with a given tag, or
     * null if there is no such child.
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
                        ((Element) node).getTagName().equals("Value")) {
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
                    ((Element) node).getTagName().equals("Value")) {
                return true;
            }
        }
        return false;
    }
}

// End XmlaMediator.java
