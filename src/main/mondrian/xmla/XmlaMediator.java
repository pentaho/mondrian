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
import java.util.Arrays;
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
    private static final String nl = System.getProperty("line.separator");

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
        final NodeList childNodes = element.getChildNodes();
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
        String tagNs = element.getNamespaceURI();
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
        Element restrictions = firstElement(discover, "Restrictions");
        if (restrictions == null) {
            throw Util.newError("<Restrictions> parameter is required (but may be empty)");
        }
        Element restrictionList = firstElement(restrictions, "RestrictionList");
        Properties restrictionsProperties = new Properties();
        if (restrictionList != null) {
            NodeList childNodes = restrictionList.getChildNodes();
            for (int i = 0, n = childNodes.getLength(); i < n; i++) {
                Node childNode = childNodes.item(i);
                if (childNode instanceof Element) {
                    Element childElement = (Element) childNode;
                    String childTag = childElement.getTagName();
                    String childValue = getCDATA(childElement);
                    restrictionsProperties.setProperty(childTag, childValue);
                }
            }
        }
        Properties propertyProperties = getProperties(discover);
        Rowset rowset = DiscoverRequestType.handle(requestType, restrictionsProperties, propertyProperties);
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
    private static Connection getConnection(Properties properties) {
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

    static class DiscoverRequestType extends EnumeratedValues {
        /** Returns a list of XML for Analysis data sources
         * available on the server or Web Service. (For an
         * example of how these may be published, see
         * "XML for Analysis Implementation Walkthrough"
         * in the XML for Analysis specification.) */
        public static final int DISCOVER_DATASOURCES = 0;
        /** todo: Copy comments from xmla spec for this and others */
        public static final int DISCOVER_PROPERTIES = 1;
        public static final int DISCOVER_SCHEMA_ROWSETS = 2;
        public static final int DISCOVER_ENUMERATORS = 3;
        public static final int DISCOVER_KEYWORDS = 4;
        public static final int DISCOVER_LITERALS = 5;
        public static final int DBSCHEMA_CATALOGS = 6;
        public static final int DBSCHEMA_COLUMNS = 7;
        public static final int DBSCHEMA_PROVIDER_TYPES = 8;
        public static final int DBSCHEMA_TABLES = 9;
        public static final int DBSCHEMA_TABLES_INFO = 10;
        public static final int MDSCHEMA_ACTIONS = 11;
        public static final int MDSCHEMA_CUBES = 12;
        public static final int MDSCHEMA_DIMENSIONS = 13;
        public static final int MDSCHEMA_FUNCTIONS = 14;
        public static final int MDSCHEMA_HIERARCHIES = 15;
        public static final int MDSCHEMA_MEASURES = 16;
        public static final int MDSCHEMA_MEMBERS = 17;
        public static final int MDSCHEMA_PROPERTIES = 18;
        public static final int MDSCHEMA_SETS = 19;
        public static final int OTHER = 20;
        public static final DiscoverRequestType instance = new DiscoverRequestType();

        private DiscoverRequestType() {
            super(new String[] {
                "DISCOVER_DATASOURCES",
                "DISCOVER_PROPERTIES",
                "DISCOVER_SCHEMA_ROWSETS",
                "DISCOVER_ENUMERATORS",
                "DISCOVER_KEYWORDS",
                "DISCOVER_LITERALS",
                "DBSCHEMA_CATALOGS",
                "DBSCHEMA_COLUMNS",
                "DBSCHEMA_PROVIDER_TYPES",
                "DBSCHEMA_TABLES",
                "DBSCHEMA_TABLES_INFO",
                "MDSCHEMA_ACTIONS",
                "MDSCHEMA_CUBES",
                "MDSCHEMA_DIMENSIONS",
                "MDSCHEMA_FUNCTIONS",
                "MDSCHEMA_HIERARCHIES",
                "MDSCHEMA_MEASURES",
                "MDSCHEMA_MEMBERS",
                "MDSCHEMA_PROPERTIES",
                "MDSCHEMA_SETS",
                "OTHER",
            });
        }

        private static Rowset handle(String requestType, Properties restrictions, Properties properties) {
            int requestTypeOrdinal = instance.getOrdinal(requestType);
            switch (requestTypeOrdinal) {
            case DISCOVER_DATASOURCES:
                return new DatasourcesRowset(restrictions, properties);
            case DISCOVER_PROPERTIES:
                return new DiscoverPropertiesRowset(restrictions, properties);
            case DISCOVER_SCHEMA_ROWSETS:
                return new SchemaRowsetsRowset(restrictions, properties);
            case DISCOVER_ENUMERATORS:
                return new DiscoverEnumeratorsRowset(restrictions, properties);
            case DISCOVER_KEYWORDS:
                return new DiscoverKeywordsRowset(restrictions, properties);
            case DISCOVER_LITERALS:
                return new DiscoverLiteralsRowset(restrictions, properties);
            case DBSCHEMA_CATALOGS:
                return new DbschemaCatalogsRowset(restrictions, properties);
            case DBSCHEMA_COLUMNS:
                return new DbschemaColumnsRowset(restrictions, properties);
            case DBSCHEMA_PROVIDER_TYPES:
                return new DbschemaProviderTypesRowset(restrictions, properties);
            case DBSCHEMA_TABLES:
                return new DbschemaTablesRowset(restrictions, properties);
            case DBSCHEMA_TABLES_INFO:
                return new DbschemaTablesInfoRowset(restrictions, properties);
            case MDSCHEMA_ACTIONS:
                return new MdschemaActionsRowset(restrictions, properties);
            case MDSCHEMA_CUBES:
                return new MdschemaCubesRowset(restrictions, properties);
            case MDSCHEMA_DIMENSIONS:
                return new MdschemaDimensionsRowset(restrictions, properties);
            case MDSCHEMA_FUNCTIONS:
                return new MdschemaFunctionsRowset(restrictions, properties);
            case MDSCHEMA_HIERARCHIES:
                return new MdschemaHierarchiesRowset(restrictions, properties);
            case MDSCHEMA_MEASURES:
                return new MdschemaMeasuresRowset(restrictions, properties);
            case MDSCHEMA_MEMBERS:
                return new MdschemaMembersRowset(restrictions, properties);
            case MDSCHEMA_PROPERTIES:
                return new MdschemaPropertiesRowset(restrictions, properties);
            case MDSCHEMA_SETS:
                return new MdschemaSetsRowset(restrictions, properties);
            default:
                throw instance.badValue(requestTypeOrdinal);
            }
        }

        private static RowsetDefinition getDefinition(int ordinal) {
            switch (ordinal) {
            case DISCOVER_DATASOURCES:
                return DatasourcesRowset.definition;
            case DISCOVER_SCHEMA_ROWSETS:
                return SchemaRowsetsRowset.definition;
                // todo: DISCOVER_PROPERTIES rowset
                // todo: DISCOVER_SCHEMA_ROWSETS rowset
                // todo: DISCOVER_ENUMERATORS rowset
                // todo: DISCOVER_KEYWORDS rowset
                // todo: DISCOVER_LITERALS rowset
            default:
                throw instance.badValue(ordinal);
            }
        }
    }

    private static class DatasourcesRowset extends Rowset {
        private static final RowsetDefinition.Column DataSourceName = new RowsetDefinition.Column("DataSourceName", RowsetDefinition.Type.String, null, true, false,
                                    "The name of the data source, such as FoodMart 2000.");
        private static final RowsetDefinition.Column DataSourceDescription = new RowsetDefinition.Column("DataSourceDescription", RowsetDefinition.Type.String, null, false, true,
                                    "A description of the data source, as entered by the publisher.");
        private static final RowsetDefinition.Column URL = new RowsetDefinition.Column("URL", RowsetDefinition.Type.String, null, true, true,
                                    "The unique path that shows where to invoke the XML for Analysis methods for that data source.");
        private static final RowsetDefinition.Column DataSourceInfo = new RowsetDefinition.Column("DataSourceInfo", RowsetDefinition.Type.String, null, false, true,
                                    "A string containing any additional information required to connect to the data source. This can include the Initial Catalog property or other information for the provider." + nl +
                        "Example: \"Provider=MSOLAP;Data Source=Local;\"");
        private static final RowsetDefinition.Column ProviderName = new RowsetDefinition.Column("ProviderName", RowsetDefinition.Type.String, null, true, true,
                                    "The name of the provider behind the data source. " + nl +
                        "Example: \"MSDASQL\"");
        private static final RowsetDefinition.Column ProviderType = new RowsetDefinition.Column("ProviderType", RowsetDefinition.Type.Array, XmlaMediator.ProviderType.enumeration, true, false,
                                    "The types of data supported by the provider. May include one or more of the following types. Example follows this table." + nl +
                        "TDP: tabular data provider." + nl +
                        "MDP: multidimensional data provider." + nl +
                        "DMP: data mining provider. A DMP provider implements the OLE DB for Data Mining specification.");
        private static final RowsetDefinition.Column AuthenticationMode = new RowsetDefinition.Column("AuthenticationMode", RowsetDefinition.Type.EnumString, XmlaMediator.AuthenticationMode.enumeration, true, false,
                                    "Specification of what type of security mode the data source uses. Values can be one of the following:" + nl +
                        "Unauthenticated: no user ID or password needs to be sent." + nl +
                        "Authenticated: User ID and Password must be included in the information required for the connection." + nl +
                        "Integrated: the data source uses the underlying security to determine authorization, such as Integrated Security provided by Microsoft Internet Information Services (IIS).");
        private static final RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_DATASOURCES", new RowsetDefinition.Column[] {
                    DataSourceName,
                    DataSourceDescription,
                    URL,
                    DataSourceInfo,
                    ProviderName,
                    ProviderType,
                    AuthenticationMode,
                });

        public DatasourcesRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public void unparse(SAXHandler saxHandler) throws SAXException {
            Connection connection = getConnection(properties);
            Row row = new Row();
            row.set(DataSourceName.name, null);
            row.set(DataSourceDescription.name, null);
            row.set(URL.name, null);
            row.set(DataSourceInfo.name, null);
            row.set(ProviderType.name, null);
            row.set(AuthenticationMode.name, null);
            emit(definition, row, saxHandler);
        }
    }

    private static class ProviderType extends EnumeratedValues.BasicValue {
        private ProviderType(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
        public static final ProviderType TDP = new ProviderType("TDP", 0, "tabular data provider.");
        public static final ProviderType MDP = new ProviderType("MDP", 1, "multidimensional data provider.");
        public static final ProviderType DMP = new ProviderType("DMP", 2, "data mining provider. A DMP provider implements the OLE DB for Data Mining specification.");
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new ProviderType[] {TDP, MDP, DMP}
        );
    }
    private static class AuthenticationMode extends EnumeratedValues.BasicValue {
        private AuthenticationMode(String name, int ordinal, String description) {
            super(name, ordinal, description);
        }
        public static final AuthenticationMode Unauthenticated = new AuthenticationMode("Unauthenticated", 0, "no user ID or password needs to be sent.");
        public static final AuthenticationMode Authenticated = new AuthenticationMode("Authenticated", 1, "User ID and Password must be included in the information required for the connection.");
        public static final AuthenticationMode Integrated = new AuthenticationMode("Integrated", 2, "the data source uses the underlying security to determine authorization, such as Integrated Security provided by Microsoft Internet Information Services (IIS).");
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new AuthenticationMode[] {Unauthenticated, Authenticated, Integrated}
        );
    }

    private static class SchemaRowsetsRowset extends Rowset {
        private static RowsetDefinition definition = new RowsetDefinition(
                "DISCOVER_DATASOURCES", new RowsetDefinition.Column[] {
                    new RowsetDefinition.Column(
                            "SchemaName",
                            RowsetDefinition.Type.EnumerationArray,
                            null, true, false, "The name of the schema/request. This returns the values in the RequestTypes enumeration, plus any additional types suppoted by the provider. The provider defines rowset structures for the additional types"
                    ),
                    new RowsetDefinition.Column(
                            "Restrictions",
                            RowsetDefinition.Type.Array,
                            null, false, true, "An array of the restrictions suppoted by provider. An example follows this table."
                    ),
                    new RowsetDefinition.Column(
                            "Description",
                            RowsetDefinition.Type.String,
                            null, false, true, "A localizable description of the schema"
                    ),
                }
        );

        public SchemaRowsetsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public void unparse(SAXHandler saxHandler) throws SAXException {
            String[] names = DiscoverRequestType.instance.getNames();
            Arrays.sort(names);
            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                int ordinal = DiscoverRequestType.instance.getOrdinal(name);
                RowsetDefinition rowsetDefinition = DiscoverRequestType.getDefinition(ordinal);
                emit(definition, rowsetDefinition, saxHandler);
            }
        }
    }

    static class DiscoverPropertiesRowset extends Rowset {
        DiscoverPropertiesRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DiscoverPropertiesRowset", new RowsetDefinition.Column[] {
                    new RowsetDefinition.Column("PropertyName", RowsetDefinition.Type.StringSometimesArray, null, true, false,
                            "The name of the property."),
                    new RowsetDefinition.Column("PropertyDescription", RowsetDefinition.Type.String, null, false, true,
                            "A localizable text description of the property."),
                    new RowsetDefinition.Column("PropertyType", RowsetDefinition.Type.String, null, false, true,
                            "The XML data type of the property."),
                    new RowsetDefinition.Column("PropertyAccessType", RowsetDefinition.Type.EnumString, null, false, false,
                            "Access for the property. The value can be Read, Write, or ReadWrite."),
                    new RowsetDefinition.Column("IsRequired", RowsetDefinition.Type.Boolean, null, false, true,
                            "True if a property is required, false if it is not required."),
                    new RowsetDefinition.Column("Value", RowsetDefinition.Type.String, null, false, true,
                            "The current value of the property."),
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            final String[] propertyNames = PropertyDefinition.enumeration.getNames();
            for (int i = 0; i < propertyNames.length; i++) {
                PropertyDefinition propertyDefinition = PropertyDefinition.getValue(propertyNames[i]);
                Row row = new Row();
                row.set("PropertyName", propertyDefinition.name_);
                row.set("PropertyDescription", propertyDefinition.description_);
                row.set("PropertyType", propertyDefinition.type);
                emit(definition, propertyDefinition, saxHandler);
            }
        }
    }

    static class DiscoverSchemaRowsetsRowset extends Rowset {
        DiscoverSchemaRowsetsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DiscoverSchemaRowsetsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DiscoverEnumeratorsRowset extends Rowset {
        DiscoverEnumeratorsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DiscoverEnumeratorsRowset", new RowsetDefinition.Column[] {
                    new RowsetDefinition.Column("EnumName", RowsetDefinition.Type.StringArray, null, true, false,
                            "The name of the enumerator that contains a set of values."),
                    new RowsetDefinition.Column("EnumDescription", RowsetDefinition.Type.String, null, false, true,
                            "A localizable description of the enumerator."),
                    new RowsetDefinition.Column("EnumType", RowsetDefinition.Type.String, null, false, false,
                            "The data type of the Enum values."),
                    new RowsetDefinition.Column("ElementName", RowsetDefinition.Type.String, null, false, false,
                            "The name of one of the value elements in the enumerator set." + nl +
                "Example: TDP"),
                    new RowsetDefinition.Column("ElementDescription", RowsetDefinition.Type.String, null, false, true,
                            "A localizable description of the element (optional)."),
                    new RowsetDefinition.Column(
                            "ElementValue", RowsetDefinition.Type.String, null, false, true, "The value of the element." + nl +
                "Example: 01"),
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DiscoverKeywordsRowset extends Rowset {
        DiscoverKeywordsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DiscoverKeywordsRowset", new RowsetDefinition.Column[] {
                    new RowsetDefinition.Column("Keyword", RowsetDefinition.Type.StringSometimesArray, null, true, false,
                            "A list of all the keywords reserved by a provider." + nl +
                "Example: AND"),
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DiscoverLiteralsRowset extends Rowset {
        DiscoverLiteralsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DiscoverLiteralsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaCatalogsRowset extends Rowset {
        DbschemaCatalogsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DbschemaCatalogsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaColumnsRowset extends Rowset {
        DbschemaColumnsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DbschemaColumnsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaProviderTypesRowset extends Rowset {
        DbschemaProviderTypesRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DbschemaProviderTypesRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaTablesRowset extends Rowset {
        DbschemaTablesRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DbschemaTablesRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class DbschemaTablesInfoRowset extends Rowset {
        DbschemaTablesInfoRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "DbschemaTablesInfoRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaActionsRowset extends Rowset {
        MdschemaActionsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaActionsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaCubesRowset extends Rowset {
        MdschemaCubesRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        private static final String CATALOG_NAME = "CATALOG_NAME";
        private static final String SCHEMA_NAME = "SCHEMA_NAME";
        private static final String CUBE_NAME = "CUBE_NAME";
        private static final String IS_DRILLTHROUGH_ENABLED = "IS_DRILLTHROUGH_ENABLED";
        private static final String IS_WRITE_ENABLED = "IS_WRITE_ENABLED";
        private static final String IS_LINKABLE = "IS_LINKABLE";
        private static final String IS_SQL_ALLOWED = "IS_SQL_ALLOWED";
        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaCubesRowset", new RowsetDefinition.Column[] {
                    new RowsetDefinition.Column(CATALOG_NAME, RowsetDefinition.Type.String, null, true, false, null),
                    new RowsetDefinition.Column(SCHEMA_NAME, RowsetDefinition.Type.String, null, true, true, null),
                    new RowsetDefinition.Column(CUBE_NAME, RowsetDefinition.Type.String, null, true, false, null),
                    new RowsetDefinition.Column(IS_DRILLTHROUGH_ENABLED, RowsetDefinition.Type.Boolean, null, false, false,
                            "Describes whether DRILLTHROUGH can be performed on the members of a cube"),
                    new RowsetDefinition.Column(IS_WRITE_ENABLED, RowsetDefinition.Type.Boolean, null, false, false,
                            "Describes whether a cube is write-enabled"),
                    new RowsetDefinition.Column(IS_LINKABLE, RowsetDefinition.Type.Boolean, null, false, false,
                            "Describes whether a cube can be used in a linked cube"),
                    new RowsetDefinition.Column(IS_SQL_ALLOWED, RowsetDefinition.Type.Boolean, null, false, false,
                            "Describes whether or not SQL can be used on the cube"),
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            final Connection connection = getConnection(properties);
            final Cube[] cubes = connection.getSchema().getCubes();
            for (int i = 0; i < cubes.length; i++) {
                Cube cube = cubes[i];
                Row row = new Row();
                row.set(CATALOG_NAME, connection.getCatalogName());
                row.set(SCHEMA_NAME, cube.getSchema());
                row.set(CUBE_NAME, cube.getName());
                row.set(IS_DRILLTHROUGH_ENABLED, true);
                row.set(IS_WRITE_ENABLED, false);
                row.set(IS_LINKABLE, false);
                row.set(IS_SQL_ALLOWED, false);
                emit(definition, row, saxHandler);
            }
        }
    }

    static class MdschemaDimensionsRowset extends Rowset {
        MdschemaDimensionsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaDimensionsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaFunctionsRowset extends Rowset {
        MdschemaFunctionsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaFunctionsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaHierarchiesRowset extends Rowset {
        MdschemaHierarchiesRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaHierarchiesRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaMeasuresRowset extends Rowset {
        MdschemaMeasuresRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaMeasuresRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaMembersRowset extends Rowset {
        MdschemaMembersRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaMembersRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaSetsRowset extends Rowset {
        MdschemaSetsRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaSetsRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            throw new UnsupportedOperationException();
        }
    }

    static class MdschemaPropertiesRowset extends Rowset {
        MdschemaPropertiesRowset(Properties restrictions, Properties properties) {
            super(restrictions, properties);
        }

        public static final RowsetDefinition definition = new RowsetDefinition(
                "MdschemaPropertiesRowset", new RowsetDefinition.Column[] {
                });

        public void unparse(SAXHandler saxHandler) throws SAXException {
            unparseArray(saxHandler, new PropertyDefinition[] {
                // todo: define standard properties
            });
        }

        private void unparseArray(SAXHandler saxHandler, Object[] objects) throws SAXException {
            for (int i = 0; i < objects.length; i++) {
                Object object = objects[i];
                emit(definition, object, saxHandler);
            }
        }
    }


}
