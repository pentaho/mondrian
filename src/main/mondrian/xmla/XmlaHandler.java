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
import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.rolap.RolapConnection;
import mondrian.spi.CatalogLocator;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;


/**
 * An <code>XmlaHandler</code> responds to XML for Analysis (XML/A) requests.
 *
 * @author jhyde, Gang Chen
 * @version $Id$
 * @since 27 April, 2003
 */
public class XmlaHandler implements XmlaConstants {
    private static final Logger LOGGER = Logger.getLogger(XmlaHandler.class);

    private final Map dataSourcesMap;
    private CatalogLocator catalogLocator = null;

    private static interface QueryResult {
        public void unparse(SaxWriter res) throws SAXException;
    }


    public XmlaHandler(DataSourcesConfig.DataSources dataSources,
                       CatalogLocator catalogLocator) {
        this.catalogLocator = catalogLocator;
        Map map = new HashMap();
        for (int i = 0; i < dataSources.dataSources.length; i++) {
            DataSourcesConfig.DataSource ds = dataSources.dataSources[i];
            if (map.containsKey(ds.getDataSourceName())) {
                throw Util.newError("duplicated data source name '" +
                                    ds.getDataSourceName() + "'");
            }
            map.put(ds.getDataSourceName(), ds);
        }
        dataSourcesMap = Collections.unmodifiableMap(map);
    }

    public Map getDataSourceEntries() {
        return dataSourcesMap;
    }

    /**
     * Processes a request.
     *
     * @param request  XML request, for example, "<SOAP-ENV:Envelope ...>".
     * @param response Destination for response
     */
    public void process(XmlaRequest request, XmlaResponse response) {
        int method = request.getMethod();
        switch (method) {
        case METHOD_DISCOVER:
            discover(request, response);
            break;
        case METHOD_EXECUTE:
            execute(request, response);
            break;
        default:
            throw new IllegalArgumentException("Unsupported XML/A method");
        }
    }

    private void execute(XmlaRequest request, XmlaResponse response) {
        // Check response's rowset format in request
        String propertyName = null;
        try {
            if (request.isDrillThrough()) {
                propertyName = PropertyDefinition.Format.name;
                final String formatName = (String) request.getProperties().get(propertyName);
                Enumeration.Format format = Enumeration.Format.getValue(formatName);
                if (format != Enumeration.Format.Tabular) {
                    throw new UnsupportedOperationException("<Format>: only 'Tabular' allowed when drilling through");
                }
            } else {
                propertyName = PropertyDefinition.Format.name;
                final String formatName = (String) request.getProperties().get(propertyName);
                Enumeration.Format format = Enumeration.Format.getValue(formatName);
                propertyName = PropertyDefinition.AxisFormat.name;
                final String axisFormatName = (String) request.getProperties().get(propertyName);
                Enumeration.AxisFormat axisFormat = Enumeration.AxisFormat.getValue(axisFormatName);
                if (format != Enumeration.Format.Multidimensional) {
                    throw new UnsupportedOperationException("<Format>: only 'Multidimensional' currently supported");
                }
                if (axisFormat != Enumeration.AxisFormat.TupleFormat) {
                    throw new UnsupportedOperationException("<AxisFormat>: only 'TupleFormat' currently supported");
                }
            }
        } catch (Error e) {
            throw new UnsupportedOperationException("Property <" +
                                                    propertyName +
                                                    "> must be provided");
        }

        // Handle execute
        QueryResult result = null;
        if (request.isDrillThrough()) {
            result = executeDrillThroughQuery(request);
        } else {
            result = executeQuery(request);
        }

        SaxWriter writer = response.getWriter();
        writer.startDocument();

        writer.startElement("ExecuteResponse", new String[] {"xmlns", NS_XMLA});
        writer.startElement("return", new String[] {"xmlns:xsi", NS_XSI,
                                                    "xmlns:xsd", NS_XSD,});
        writer.startElement("root",
                            new String[] {"xmlns",
                                          request.isDrillThrough() ? NS_XMLA_ROWSET :
                                                                     NS_XMLA_MDDATASET});
        writer.startElement("xsd:schema", new String[] {"xmlns:xsd", NS_XSD});
        //TODO: schema definition
        writer.endElement();

        try {
            result.unparse(writer);
        } catch (Throwable t) {
            LOGGER.error("Errors when unparsing XML/A execute result", t);
            response.error(t);
        } finally {
            writer.endElement();
            writer.endElement();
            writer.endElement();
        }

        writer.endDocument();
    }

    private QueryResult executeDrillThroughQuery(XmlaRequest request) {
        final String statement = request.getStatement();
        final Connection connection = getConnection(request);
        final Query query = connection.parseQuery(statement);
        final Result result = connection.execute(query);
        Cell dtCell = result.getCell(new int[] {0, 0});

        if (!dtCell.canDrillThrough()) {
            throw Util.newError("Cannot do DillThrough operation on the cell");
        }

        String dtSql = dtCell.getDrillThroughSQL(true);
        TabularRowSet rowset = null;
        java.sql.Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;



        try {
            conn = ((RolapConnection) connection).getDataSource().getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);

            String temp = dtSql.toUpperCase();
            int selOff = temp.indexOf("SELECT");
            int fromOff = temp.indexOf("FROM");
            StringBuffer buf = new StringBuffer();
            buf.append("select count(*) ");
            buf.append(dtSql.substring(fromOff));

            String countSql = buf.toString();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("drill through counting sql: " + countSql);
            }
            rs = stmt.executeQuery(countSql);
            rs.next();
            int count = rs.getInt(1);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("drill through sql: " + dtSql);
            }
            rs = stmt.executeQuery(dtSql);
            rowset = new TabularRowSet(rs, request.drillThroughMaxRows(),
                                       request.drillThroughFirstRowset(), count);
        } catch (SQLException sqle) {
            throw Util.newError(sqle, "Errors when executing DrillThrough sql '" +
                                      dtSql + "'");
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (SQLException ignored) {
            }
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException ignored) {
            }
            try {
                if (conn != null && !conn.isClosed()) conn.close();
            } catch (SQLException ignored) {
            }
        }

        return rowset;
    }

    static class TabularRowSet implements QueryResult {
        private String[] header;
        private List rows;
        private int totalCount;

        public TabularRowSet(ResultSet rs,
                             int maxRows,
                             int firstRowset,
                             int totalCount) throws SQLException {
            this.totalCount = totalCount;
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();

            // populate header
            header = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                header[i] = md.getColumnName(i + 1);
            }

            // skip to first row
            rs.absolute(firstRowset < 0 ? 1 : firstRowset);

            // populate data
            rows = new ArrayList();
            maxRows = (maxRows < 0 ? Integer.MAX_VALUE : maxRows);
            while (rs.next() && maxRows > 0) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                rows.add(row);
                maxRows--;
            }
        }

        public void unparse(SaxWriter writer) throws SAXException {
            String[] encodedHeader = new String[header.length];
            for (int i = 0; i < header.length; i++) {
                // replace invalid XML element name, like " ", with "_x0020_" in
                // column headers, otherwise will generate a badly-formatted xml doc.
                encodedHeader[i] = XmlaUtil.encodeElementName(header[i]);
            }

            String countStr = Integer.toString(totalCount);
            writer.startElement("row");
            for (int i = 0; i < encodedHeader.length; i++) {
                writer.startElement(encodedHeader[i]);
                writer.characters(countStr);
                writer.endElement();
            }
            writer.endElement(); // row

            for (Iterator it = rows.iterator(); it.hasNext();) {
                Object[] row = (Object[]) it.next();
                writer.startElement("row");
                for (int i = 0; i < row.length; i++) {
                    writer.startElement(encodedHeader[i]);
                    Object value = row[i];
                    if (value == null) {
                        writer.characters("null");
                    } else {
                        if (value instanceof Number) {
                            writer.characters(XmlaUtil.normalizeNumricString(value.toString()));
                        } else {
                            writer.characters(row[i].toString());
                        }
                    }
                    writer.endElement();
                }
                writer.endElement(); // row
            }
        }
    }

    private QueryResult executeQuery(XmlaRequest request) {
        final String statement = request.getStatement();
        final Connection connection = getConnection(request);
        final Query query = connection.parseQuery(statement);
        final Result result = connection.execute(query);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("mdx: " + statement);
        }

        return new MDDataSet(result);
    }

    static class MDDataSet implements QueryResult {
        private final Result result;
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
            "DisplayInfo",
            "Depth"};
        private static final String[] propLongs = new String[] {
            Property.MEMBER_UNIQUE_NAME.name,
            Property.MEMBER_CAPTION.name,
            Property.LEVEL_UNIQUE_NAME.name,
            Property.LEVEL_NUMBER.name,
            Property.DISPLAY_INFO.name,
            Property.DEPTH.name};

        public MDDataSet(Result result) {
            this.result = result;
        }

        public void unparse(SaxWriter writer) throws SAXException {
            olapInfo(writer);
            axes(writer);
            cellData(writer);
        }

        private void olapInfo(SaxWriter writer) {
            writer.startElement("OlapInfo");
            writer.startElement("CubeInfo");
            writer.startElement("Cube");
            writer.startElement("CubeName");
            writer.characters(result.getQuery().getCube().getName());
            writer.endElement();
            writer.endElement();
            writer.endElement(); // CubeInfo
            // -----------
            writer.startElement("AxesInfo");
            final Axis[] axes = result.getAxes();
            axisInfo(writer, result.getSlicerAxis(), "SlicerAxis");
            for (int i = 0; i < axes.length; i++) {
                axisInfo(writer, axes[i], "Axis" + i);
            }
            writer.endElement(); // AxesInfo
            // -----------
            writer.startElement("CellInfo");
            writer.element("Value", new String[] {
                "name", "VALUE"});
            writer.element("FmtValue", new String[] {
                "name", "FORMATTED_VALUE"});
            writer.element("FormatString", new String[] {
                "name", "FORMAT_STRING"});
            writer.endElement(); // CellInfo
            // -----------
            writer.endElement(); // OlapInfo
        }

        private void axisInfo(SaxWriter writer, Axis axis, String axisName) {
            writer.startElement("AxisInfo", new String[] {
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
                writer.startElement("HierarchyInfo", new String[] {
                    "name", hierarchies[j].getName()});
                for (int k = 0; k < props.length; k++) {
                    writer.element(props[k], new String[] {
                        "name", hierarchies[j].getUniqueName() + ".[" + 
                                propLongs[k] + "]"});
                }
                writer.endElement(); // HierarchyInfo
            }
            writer.endElement(); // AxisInfo
        }

        private void axes(SaxWriter writer) {
            writer.startElement("Axes");
            axis(writer, result.getSlicerAxis(), "SlicerAxis");
            final Axis[] axes = result.getAxes();
            for (int i = 0; i < axes.length; i++) {
                axis(writer, axes[i], "Axis" + i);
            }
            writer.endElement(); // Axes
        }

        private void axis(SaxWriter writer, Axis axis, String axisName) {
            writer.startElement("Axis", new String[] {
                "name", axisName});
            writer.startElement("Tuples");
            Position[] positions = axis.positions;
            for (int j = 0; j < positions.length; j++) {
                Position position = positions[j];
                writer.startElement("Tuple");
                for (int k = 0; k < position.members.length; k++) {
                    Member member = position.members[k];
                    writer.startElement("Member", new String[] {
                        "Hierarchy", member.getHierarchy().getName()});
                    for (int m = 0; m < props.length; m++) {
                        Object value = null;
                        if (propLongs[m].equals(Property.DISPLAY_INFO.name)) {
                            Integer childrenCard =
                                (Integer) member.getPropertyValue(Property.CHILDREN_CARDINALITY.name);
                            int displayInfo =
                                calculateDisplayInfo((j == 0 ? null : positions[j - 1]),
                                                     (j + 1 == positions.length ? null : positions[j + 1]),
                                                     member, k, childrenCard.intValue());
                            value = new Integer(displayInfo);
                        } else if (propLongs[m].equals(Property.DEPTH.name)) {
                            value = new Integer(member.getDepth());
                        } else {
                            value = member.getPropertyValue(propLongs[m]);
                        }
                        if (value != null) {
                            writer.startElement(props[m]); // Properties
                            writer.characters(value.toString());
                            writer.endElement(); // Properties
                        }
                    }
                    writer.endElement(); // Member
                }
                writer.endElement(); // Tuple
            }
            writer.endElement(); // Tuples
            writer.endElement(); // Axis
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

        private void cellData(SaxWriter writer) throws SAXException {
            writer.startElement("CellData");
            final int axisCount = result.getAxes().length;
            int[] pos = new int[axisCount];
            int[] cellOrdinal = new int[] {0};

            if (axisCount == 0) { // For MDX like: SELECT FROM Sales
                emitCell(writer, result.getCell(pos), cellOrdinal[0]);
            } else {
                recurse(writer, pos, axisCount - 1, cellOrdinal);
            }

            writer.endElement(); // CellData
        }


        private void recurse(SaxWriter writer, int[] pos, int axis, int[] cellOrdinal) throws SAXException {
            final int axisLength = result.getAxes()[axis].positions.length;
            for (int i = 0; i < axisLength; i++) {
                pos[axis] = i;
                if (axis == 0) {
                    final Cell cell = result.getCell(pos);
                    emitCell(writer, cell, cellOrdinal[0]++);
                } else {
                    recurse(writer, pos, axis - 1, cellOrdinal);
                }
            }
        }


        private void emitCell(SaxWriter writer, Cell cell, int ordinal) {
            if (cell.isNull()) // Ignore null cell like MS AS
                return;

            writer.startElement("Cell", new String[] {
                "CellOrdinal", Integer.toString(ordinal)});
            for (int i = 0; i < cellProps.length; i++) {
                String cellPropLong = cellPropLongs[i];
                final Object value = cell.getPropertyValue(cellPropLong);

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
                        writer.startElement(cellProps[i], new String[] {"xsi:type", valueType});
                    } else {
                        writer.startElement(cellProps[i]);
                    }

                    String valueString = value.toString();

                    if (cellPropLong.equals(Property.VALUE.name) &&
                            value instanceof Number) {
                        valueString = XmlaUtil.normalizeNumricString(valueString);
                    }

                    writer.characters(valueString);
                    writer.endElement();
                }
            }
            writer.endElement(); // Cell
        }
    }

    private void discover(XmlaRequest request, XmlaResponse response) {
        final RowsetDefinition rowsetDefinition =
            RowsetDefinition.getValue(request.getRequestType());
        Rowset rowset = rowsetDefinition.getRowset(request, this);

        try {
            final String formatName =
                (String) request.getProperties().get(PropertyDefinition.Format.name);
            Enumeration.Format format = Enumeration.Format.getValue(formatName);
            if (format != Enumeration.Format.Tabular) {
                throw new UnsupportedOperationException("<Format>: only 'Tabular' allowed in Discover method type");
            }
        } catch (Error e) {
        }

        SaxWriter writer = response.getWriter();
        writer.startDocument();

        writer.startElement("DiscoverResponse", new String[] {
            "xmlns", NS_XMLA});
        writer.startElement("return");
        writer.startElement("root", new String[] {
            "xmlns", NS_XMLA_ROWSET});
        writer.startElement("xsd:schema", new String[] {
            "xmlns:xsd", NS_XSD,
            "targetNamespace", NS_XMLA_ROWSET,
            "xmlns:xsi", NS_XSI,
            "xmlns:sql", "urn:schemas-microsoft-com:xml-sql",
            "elementFormDefault", "qualified"});
        //TODO: add schema
        writer.endElement();
        try {
            rowset.unparse(response);
        } catch (Throwable t) { // MondrianException is subclass of RuntimeException
            LOGGER.error("Errors when unparsing XML/A discover result", t);
            response.error(t);
        } finally {
            // keep the tags balanced, even if there's an error
            writer.endElement();
            writer.endElement();
            writer.endElement();
        }

        writer.endDocument();

    }

    /**
     * Returns a Mondrian connection as specified by a set of properties
     * (especially the "Connect string" property).
     *
     * @param request
     * @return
     */
    Connection getConnection(XmlaRequest request) {
        Map properties = request.getProperties();
        final String dataSourceInfo =
            (String) properties.get(PropertyDefinition.DataSourceInfo.name);
        if (!dataSourcesMap.containsKey(dataSourceInfo)) {
            throw Util.newError("no data source is configured with name '" + dataSourceInfo + "'");
        }

        DataSourcesConfig.DataSource ds =
            (DataSourcesConfig.DataSource) dataSourcesMap.get(dataSourceInfo);
        Util.PropertyList connectProperties =
            Util.parseConnectString(ds.getDataSourceInfo());
        connectProperties.put("catalog",
                              catalogLocator.locate(connectProperties.get("catalog")));

        // Checking access
        if (!DataSourcesConfig.DataSource.AUTH_MODE_UNAUTHENTICATED.equalsIgnoreCase(ds.getAuthenticationMode()) &&
                null == request.getRole()) {
            throw new SecurityException("Access denied for data source needing authentication");
        }

        connectProperties.put("role", request.getRole());
        RolapConnection conn =
            (RolapConnection) DriverManager.getConnection(connectProperties, null, false);

        return conn;
    }

}

// End XmlaHandler.java
