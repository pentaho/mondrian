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

import java.io.*;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import mondrian.olap.*;
import mondrian.rolap.RolapConnection;
import mondrian.util.SAXHandler;
import mondrian.util.SAXWriter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.apache.log4j.Logger;

/**
 * An <code>XmlaMediator</code> responds to XML for Analysis requests.
 *
 * @author jhyde
 * @since 27 April, 2003
 * @version $Id$
 */
public class XmlaMediator {
    private static final boolean DRILLTHROUGH_EXTENDS_CONTEXT = true;

    private static final Logger LOGGER = Logger.getLogger(XmlaMediator.class);
    private static final String XSD_NS = "http://www.w3.org/2001/XMLSchema";
    private static final String XSI_NS = "http://www.w3.org/2001/XMLSchema-instance";
    private static final String XMLA_NS = "urn:schemas-microsoft-com:xml-analysis";
    private static final String XMLA_MDDATASET_NS = "urn:schemas-microsoft-com:xml-analysis:mddataset";
    private static final String XMLA_ROWSET_NS = "urn:schemas-microsoft-com:xml-analysis:rowset";
    static ThreadLocal threadServletContext = new ThreadLocal();
    static Map dataSourcesMap = new HashMap();

    /**
     * Please call this method before any usage of XmlaMediator.
     * @param dataSources
     */
    public static void initDataSourcesMap(DataSourcesConfig.DataSources dataSources) {
        Map map = new HashMap();
        for (int i = 0; i < dataSources.dataSources.length; i++) {
                        DataSourcesConfig.DataSource ds = dataSources.dataSources[i];
                        if (map.containsKey(ds.getDataSourceName())) {
                                throw Util.newError("duplicated data source name '" + ds.getDataSourceName() + "'");
                        }
                        map.put(ds.getDataSourceName(), ds);
                }
        dataSourcesMap = Collections.unmodifiableMap(map);
    }

    /**
     * Processes a request.
     * @param request  XML request, for example, "<SOAP-ENV:Envelope ...>".
     * @param response Destination for response
     */
    public void process(String request, Writer response) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder documentBuilder;
        try {
            documentBuilder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw Util.newError(e, "Error processing '" + request + "'");
        }
        Document document;
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Request " + request + " failed", e);
            }
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

    private void processBody(Element element, SAXHandler saxHandler) {
        String tagName = element.getLocalName();
        Util.assertTrue(tagName.equals("Body"));
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            final Node node = childNodes.item(i);
            if (node instanceof Element) {
                processRequest((Element) node, saxHandler);
            }
        }
    }

    private void processRequest(Element element, SAXHandler saxHandler) {
        String tagName = element.getLocalName();
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

        boolean isDrillThrough = false;
        // No usage of Regex in 1.4
        String upperStatement = statement.toUpperCase();
        int dtOffset = upperStatement.indexOf("DRILLTHROUGH");
        int slOffset = upperStatement.indexOf("SELECT");
        if (dtOffset != -1 && dtOffset < slOffset) {
            String format = properties.getProperty(PropertyDefinition.Format.name);
            if ("Tabular".compareToIgnoreCase(format) == 0)
                isDrillThrough = true;
            else
                throw Util.newError("Must set property 'Format' to 'Tabular' for DrillThrough");
        }

        try {
                saxHandler.startElement("ExecuteResponse", new String[] {
                                "xmlns", XMLA_NS});
                saxHandler.startElement("return", new String[] {
                   "xmlns:xsi", XSI_NS,
                   "xmlns:xsd", XSD_NS,});
                saxHandler.startElement("root", new String[] {
                                "xmlns", isDrillThrough ? XMLA_ROWSET_NS : XMLA_MDDATASET_NS});
                saxHandler.startElement("xsd:schema", new String[] {
                                "xmlns:xsd", XSD_NS});
                        // todo: schema definition
                saxHandler.endElement();
                try {
                if (isDrillThrough) {
                    StringBuffer dtStatement = new StringBuffer();
                    dtStatement.append(statement.substring(0, dtOffset)); // formulas
                    dtStatement.append(statement.substring(dtOffset + "DRILLTHROUGH".length())); // select to end
                    executeDrillThroughQuery(dtStatement.toString(), properties).unparse(saxHandler);
                } else {
                    executeQuery(statement, properties).unparse(saxHandler);
                }
                } catch(RuntimeException re) { // MondrianException is subclass of RuntimeException
                    saxHandler.completeBeforeElement("root");
                reportXmlaError(saxHandler, re);
            } finally {
                        saxHandler.endElement();
                        saxHandler.endElement();
                        saxHandler.endElement();
                }
        } catch (SAXException e) {
                throw Util.newError(e, "Error while processing execute request");
        }
    }

    private TabularRowSet executeDrillThroughQuery(String statement, Properties properties) {
        final Connection connection = getConnection(properties);
        final Query query = connection.parseQuery(statement);
        final Result result = connection.execute(query);
        Cell dtCell = result.getCell(new int[] {0,0});

        if (!dtCell.canDrillThrough()) {
            throw Util.newError("Cannot do DillThrough operation on the cell");
        }

        String dtSql = dtCell.getDrillThroughSQL(DRILLTHROUGH_EXTENDS_CONTEXT);
        TabularRowSet rowset = null;
        java.sql.Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = ((RolapConnection)connection).getDataSource().getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(dtSql);
            rowset = new TabularRowSet(rs);
        } catch (SQLException sqle) {
            Util.newError(sqle, "Error while executing DrillThrough sql '" + dtSql + "'");
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (SQLException ignored){}
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException ignored){}
            try {
                if (conn != null && !conn.isClosed()) conn.close();
            } catch (SQLException ignored){}
        }

        return rowset;
    }

    static class TabularRowSet {
        private String[] header;
        private List rows;

        public TabularRowSet(ResultSet rs) throws SQLException {
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();

            // populate header
            header = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                header[i] = md.getColumnName(i+1);
            }

            // populate data
            rows = new ArrayList();
            while(rs.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i+1);
                }
                rows.add(row);
            }
        }

        public void unparse(SAXHandler saxHandler) throws SAXException {
            String[] encodedHeader = new String[header.length];
            for (int i = 0; i < header.length; i++) {
                // replace " " with "_x0020_" in column headers,
                // otherwise will generate a badly-formatted xml doc.
                encodedHeader[i] = header[i].replaceAll(" ", "_x0020_");
            }

            for (Iterator it = rows.iterator(); it.hasNext();) {
                Object[] row = (Object[])it.next();
                saxHandler.startElement("row");
                for (int i = 0; i < row.length; i++) {
                    saxHandler.startElement(encodedHeader[i]);
                    Object value = row[i];
                    if (value == null) {
                        saxHandler.characters("<null>");
                    } else {
                        if (value instanceof Number)
                            saxHandler.characters(normalizeNumricString(row[i].toString()));
                        else
                            saxHandler.characters(row[i].toString());
                    }
                    saxHandler.endElement();
                }
                saxHandler.endElement(); // row
            }
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
            "LNum",
            "DisplayInfo"};
        private static final String[] propLongs = new String[] {
            Property.MEMBER_UNIQUE_NAME.name,
            Property.MEMBER_CAPTION.name,
            Property.LEVEL_UNIQUE_NAME.name,
            Property.LEVEL_NUMBER.name,
            "DISPLAY_INFO"};
        private static final String[] realPropLongs = new String[] {
            Property.MEMBER_UNIQUE_NAME.name,
            Property.MEMBER_CAPTION.name,
            Property.LEVEL_UNIQUE_NAME.name,
            Property.LEVEL_NUMBER.name,
            Property.CHILDREN_CARDINALITY.name};

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
            axisInfo(saxHandler, result.getSlicerAxis(), "SlicerAxis");
            for (int i = 0; i < axes.length; i++) {
                axisInfo(saxHandler, axes[i], "Axis"+i);
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
            saxHandler.endElement(); // OlapInfo
        }

        private void axisInfo(SAXHandler saxHandler, Axis axis, String axisName) throws SAXException {
            saxHandler.startElement("AxisInfo", new String[] {
                    "name", axisName});
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
                    //final QueryAxis queryAxis = this.result.getQuery().axes[i];
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

        private void axes(SAXHandler saxHandler) throws SAXException {
            if (axisFormat != Enumeration.AxisFormat.TupleFormat) {
                throw new UnsupportedOperationException("<AxisFormat>: only 'TupleFormat' currently supported");
            }
            saxHandler.startElement("Axes");
            axis(saxHandler, result.getSlicerAxis(), "SlicerAxis");
            final Axis[] axes = result.getAxes();
            for (int i = 0; i < axes.length; i++) {
                axis(saxHandler, axes[i], "Axis" + i);
            }
            saxHandler.endElement(); // Axes
        }

        private void axis(SAXHandler saxHandler, Axis axis, String axisName) throws SAXException {
            saxHandler.startElement("Axis", new String[] {
                    "name", axisName});
                saxHandler.startElement("Tuples");
                Position[] positions = axis.positions;
                for (int j = 0; j < positions.length; j++) {
                    Position position = positions[j];
                    saxHandler.startElement("Tuple");
                    for (int k = 0; k < position.members.length; k++) {
                        Member member = position.members[k];
                        saxHandler.startElement("Member", new String[] {
                            "Hierarchy", member.getHierarchy().getName()});
                        for (int m = 0; m < props.length; m++) {
                            final Object value = member.getPropertyValue(realPropLongs[m]);
                            if (value != null) {
                                saxHandler.startElement(props[m]); // UName
                                if (realPropLongs[m].equals(Property.CHILDREN_CARDINALITY.name)) { // DisplayInfo
                                    int displayInfo = calculateDisplayInfo((j == 0 ? null : positions[j-1]),
                                            (j+1 == positions.length ? null : positions[j+1]),
                                            member, k, ((Integer)value).intValue());
                                    saxHandler.characters(Integer.toString(displayInfo));
                                } else {
                                    saxHandler.characters(value.toString());
                                }
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

        private int calculateDisplayInfo(Position prevPosition, Position nextPosition,
                Member currentMember, int memberOrdinal, int childrenCount) {
            int displayInfo = 0xffff & childrenCount;

            if (nextPosition != null) {
                String currentUName = currentMember.getUniqueName();
                String nextParentUName = nextPosition.members[memberOrdinal].getParentUniqueName();
                displayInfo |= (currentUName.equals(nextParentUName) ? 0x10000 : 0);
            }
            if (prevPosition != null) {
                String currentParentUName = currentMember.getParentUniqueName();
                String prevParentUName = prevPosition.members[memberOrdinal].getParentUniqueName();
                displayInfo |= (currentParentUName != null && currentParentUName.equals(prevParentUName) ? 0x20000 : 0);
            }
            return displayInfo;
        }

        private void cellData(SAXHandler saxHandler) throws SAXException {
            saxHandler.startElement("CellData");
            final int axisCount = result.getAxes().length;
            int[] pos = new int[axisCount];
            int[] cellOrdinal = new int[] {0};

            if (axisCount == 0) { // For MDX like: SELECT FROM Sales
                emitCell(saxHandler, result.getCell(pos), cellOrdinal[0]);
            } else {
                recurse(saxHandler, pos, axisCount - 1, cellOrdinal);
            }

            saxHandler.endElement(); // CellData
        }


        private void recurse(SAXHandler saxHandler, int[] pos, int axis, int[] cellOrdinal) throws SAXException {
            final int axisLength = result.getAxes()[axis].positions.length;
            for (int i = 0; i < axisLength; i++) {
                pos[axis] = i;
                if (axis == 0) {
                    final Cell cell = result.getCell(pos);
                    emitCell(saxHandler, cell, cellOrdinal[0]++);
                } else {
                    recurse(saxHandler, pos, axis - 1, cellOrdinal);
                }
            }
        }


        private void emitCell(SAXHandler saxHandler, Cell cell, int ordinal) throws SAXException {
            if (cell.isNull()) { // Ignore null cells like MS AS
                return;
            }
            saxHandler.startElement("Cell", new String[] {
                    "CellOrdinal", Integer.toString(ordinal)});
            for (int i = 0; i < cellProps.length; i++) {
                String cellPropLong = cellPropLongs[i];
                final Object value =
                    cell.getPropertyValue(cellPropLong);


                // Deduce the XML datatype from the declared datatype
                // of the measure, if present. (It comes from the
                // "datatype" attribute of the "Measure" element.) If
                // not present, use the value type to guess.
                //
                // The value type depends upon the RDBMS and the JDBC
                // driver, so it tends to produce inconsistent results
                // between platforms.
                String valueType;
                String datatype = (String)
                    cell.getPropertyValue(Property.DATATYPE.getName());
                if (datatype != null) {
                    if (datatype.equals("Integer")) {
                        valueType = "xsd:int";
                    } else if (datatype.equals("Numeric")) {
                        valueType = "xsd:double";
                    } else {
                        valueType = "xsd:string";
                    }
                } else if (value instanceof Integer || value instanceof Long) {
                    valueType = "xsd:int";
                } else if (value instanceof Double || value instanceof BigDecimal) {
                    valueType = "xsd:double";
                } else {
                    valueType = "xsd:string";
                }

                if (value != null) {
                    if (cellPropLong.equals(Property.VALUE.name)) {
                        saxHandler.startElement(cellProps[i], new String[]{"xsi:type", valueType});
                    } else {
                        saxHandler.startElement(cellProps[i]);
                    }

                    String valueString = value.toString();

                    if (cellPropLong.equals(Property.VALUE.name) &&
                           value instanceof Number) {
                        valueString = normalizeNumricString(valueString);
                    }

                    saxHandler.characters(valueString);
                    saxHandler.endElement();
                }
            }
            saxHandler.endElement(); // Cell
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
                "xmlns", XMLA_ROWSET_NS});
            saxHandler.startElement("xsd:schema", new String[] {
                "xmlns:xsd", XSD_NS,
                "targetNamespace", XMLA_ROWSET_NS,
                "xmlns:xsi", XSI_NS,
                "xmlns:sql", "urn:schemas-microsoft-com:xml-sql",
                "elementFormDefault", "qualified"});
            //TODO: add schema
            saxHandler.endElement();
            try {
                rowset.unparse(saxHandler);
            } catch(RuntimeException re) { // MondrianException is subclass of RuntimeException
                saxHandler.completeBeforeElement("root");
                reportXmlaError(saxHandler, re);
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

    private void reportXmlaError(SAXHandler saxHandler, Exception exception) throws SAXException {
        Throwable throwable = gotoRootThrowable(exception);
        saxHandler.startElement("Messages");
        saxHandler.startElement("Error", new String[] {
                "ErrorCode", throwable.getClass().getName(),
                "Description", throwable.getMessage(),
                "Source", "Mondrian",
                "Help", "",
        });
        // Don't dump stack trace to client
//        StringWriter stackWriter = new StringWriter();
//        throwable.printStackTrace(new PrintWriter(stackWriter));
//        saxHandler.characters(stackWriter.getBuffer().toString());
        saxHandler.endElement();
        saxHandler.endElement();
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
        if (!dataSourcesMap.containsKey(dataSourceInfo)) {
                throw Util.newError("no data source is configured with name '" + dataSourceInfo + "'");
        }

        DataSourcesConfig.DataSource ds = (DataSourcesConfig.DataSource)dataSourcesMap.get(dataSourceInfo);
        Util.PropertyList connectProperties = Util.parseConnectString(ds.getDataSourceInfo());
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
         * Retrieving the root MondrianException in an exception chain if exists.
         * @param throwable the last one in exception chain.
         * @return the root MondrianException if exists, otherwise the input exception.
         */
        static Throwable gotoRootThrowable(Throwable throwable) {
                Throwable rootThrowable = throwable.getCause();
                if (rootThrowable != null && rootThrowable instanceof MondrianException) {
                        return gotoRootThrowable(rootThrowable);
                }
                return throwable;
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
                if (node instanceof Element && node.getLocalName().equals("Value")) {
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
            if (node instanceof Element && node.getLocalName().equals("Value")) {
                return true;
            }
        }
        return false;
    }

    private static String normalizeNumricString(String numericStr) {
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
            char c = numericStr.charAt(p-1);
            while (c == '0') {
                found = true;
                p--;
                c = numericStr.charAt(p-1);
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

// End XmlaMediator.java
