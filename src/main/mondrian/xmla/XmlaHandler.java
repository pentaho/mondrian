/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.rolap.RolapAggregationManager;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.RolapStar;
import mondrian.rolap.agg.CellRequest;
import mondrian.spi.CatalogLocator;
import mondrian.xmla.impl.DefaultSaxWriter;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.io.StringWriter;
import java.io.PrintWriter;


/**
 * An <code>XmlaHandler</code> responds to XML for Analysis (XML/A) requests.
 *
 * @author jhyde, Gang Chen
 * @version $Id$
 * @since 27 April, 2003
 */
public class XmlaHandler implements XmlaConstants {
    private static final Logger LOGGER = Logger.getLogger(XmlaHandler.class);

    private final Map<String, DataSourcesConfig.DataSource> dataSourcesMap;
    private final List<String> drillThruColumnNames = new ArrayList<String>();
    private CatalogLocator catalogLocator = null;

    private static final int ROW_SET     = 1;
    private static final int MD_DATA_SET = 2;

    private static final String ROW_SET_XML_SCHEMA = computeXsd(ROW_SET);
    private static final String EMPTY_ROW_SET_XML_SCHEMA =
                                    computeEmptyXsd(ROW_SET);

    private static final String MD_DATA_SET_XML_SCHEMA = computeXsd(MD_DATA_SET);
    private static final String EMPTY_MD_DATA_SET_XML_SCHEMA =
                                    computeEmptyXsd(MD_DATA_SET);

    private static final String NS_XML_SQL = "urn:schemas-microsoft-com:xml-sql";

    private static String computeXsd(int settype) {
        final StringWriter sw = new StringWriter();
        SaxWriter writer = new DefaultSaxWriter(new PrintWriter(sw), 3);
        writeDatasetXmlSchema(writer, settype);
        writer.flush();
        return sw.toString();
    }

    private static String computeEmptyXsd(int settype) {
        final StringWriter sw = new StringWriter();
        SaxWriter writer = new DefaultSaxWriter(new PrintWriter(sw), 3);
        writeEmptyDatasetXmlSchema(writer, settype);
        writer.flush();
        return sw.toString();
    }

    private static interface QueryResult {
        public void unparse(SaxWriter res) throws SAXException;
    }

    public XmlaHandler(
            DataSourcesConfig.DataSources dataSources,
            CatalogLocator catalogLocator) {
        this.catalogLocator = catalogLocator;
        Map<String, DataSourcesConfig.DataSource> map =
            new HashMap<String, DataSourcesConfig.DataSource>();
        if (dataSources != null) {
            for (DataSourcesConfig.DataSource ds : dataSources.dataSources) {
                if (map.containsKey(ds.getDataSourceName())) {
                    // This is not an XmlaException
                    throw Util.newError(
                        "duplicated data source name '" +
                            ds.getDataSourceName() + "'");
                }
                // Set parent pointers.
                for (DataSourcesConfig.Catalog catalog : ds.catalogs.catalogs) {
                    catalog.setDataSource(ds);
                }
                map.put(ds.getDataSourceName(), ds);
            }
        }
        dataSourcesMap = Collections.unmodifiableMap(map);
    }

    public Map<String, DataSourcesConfig.DataSource> getDataSourceEntries() {
        return dataSourcesMap;
    }

    /**
     * Processes a request.
     *
     * @param request  XML request, for example, "<SOAP-ENV:Envelope ...>".
     * @param response Destination for response
     */
    public void process(XmlaRequest request, XmlaResponse response)
            throws XmlaException {
        int method = request.getMethod();
        long start = System.currentTimeMillis();

        switch (method) {
        case METHOD_DISCOVER:
            discover(request, response);
            break;
        case METHOD_EXECUTE:
            execute(request, response);
            break;
        default:
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_METHOD_CODE,
                HSB_BAD_METHOD_FAULT_FS,
                new IllegalArgumentException(
                    "Unsupported XML/A method: " +method));
        }
        if (LOGGER.isDebugEnabled()) {
            long end = System.currentTimeMillis();
            LOGGER.debug("XmlaHandler.process: time = " +(end-start));
            LOGGER.debug("XmlaHandler.process: " +Util.printMemory());
        }
    }

    private void checkFormat(XmlaRequest request) throws XmlaException {
        // Check response's rowset format in request
        final Map<String, String> properties = request.getProperties();
        if (request.isDrillThrough()) {
            final String formatName =
                properties.get(PropertyDefinition.Format.name());
            Enumeration.Format format =
                valueOf(
                    Enumeration.Format.class,
                    formatName,
                    null);
            if (format != Enumeration.Format.Tabular) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_DRILL_THROUGH_FORMAT_CODE,
                    HSB_DRILL_THROUGH_FORMAT_FAULT_FS,
                    new UnsupportedOperationException(
                        "<Format>: only 'Tabular' allowed when drilling through"));
            }
        } else {
            final String formatName =
                properties.get(PropertyDefinition.Format.name());
            if (formatName != null) {
                Enumeration.Format format = valueOf(
                    Enumeration.Format.class, formatName, null);
                if (format != Enumeration.Format.Multidimensional &&
                    format != Enumeration.Format.Tabular) {
                    throw new UnsupportedOperationException(
                        "<Format>: only 'Multidimensional', 'Tabular' currently supported");
                }
            }
            final String axisFormatName =
                properties.get(PropertyDefinition.AxisFormat.name());
            if (axisFormatName != null) {
                Enumeration.AxisFormat axisFormat = valueOf(
                    Enumeration.AxisFormat.class, axisFormatName, null);

                if (axisFormat != Enumeration.AxisFormat.TupleFormat) {
                    throw new UnsupportedOperationException(
                        "<AxisFormat>: only 'TupleFormat' currently supported");
                }
            }
        }
    }

    private void execute(XmlaRequest request, XmlaResponse response)
            throws XmlaException {

        final Map<String, String> properties = request.getProperties();
        final String contentName =
            properties.get(PropertyDefinition.Content.name());
        // default value is SchemaData
        Enumeration.Content content =
            valueOf(Enumeration.Content.class, contentName, CONTENT_DEFAULT);

        // Handle execute
        QueryResult result;
        if (request.isDrillThrough()) {
            String tabFields =
                (String) properties.get(PropertyDefinition.TableFields.name());
            if (tabFields != null && tabFields.length() > 0) {
                // Presence of TABLE_FIELDS property initiates advanced
                // drill-through.
                result = executeColumnQuery(request);
            } else {
                result = executeDrillThroughQuery(request);
            }
        } else {
            result = executeQuery(request);
        }

        SaxWriter writer = response.getWriter();
        writer.startDocument();

        writer.startElement("xmla:ExecuteResponse", new String[] {
            "xmlns:xmla", NS_XMLA});
        writer.startElement("xmla:return");
        boolean rowset =
            request.isDrillThrough() ||
                Enumeration.Format.Tabular.name().equals(
                    request.getProperties().get(
                        PropertyDefinition.Format.name()));
        writer.startElement("root", new String[] {
            "xmlns",
            result == null ? NS_XMLA_EMPTY :
                rowset ? NS_XMLA_ROWSET :
                NS_XMLA_MDDATASET,
            "xmlns:xsi", NS_XSI,
            "xmlns:xsd", NS_XSD,
            "xmlns:EX", NS_XMLA_EX,
        });

        if ((content == Enumeration.Content.Schema)
                || (content == Enumeration.Content.SchemaData)) {
            if (result != null) {
                if (result instanceof MDDataSet_Tabular) {
                    MDDataSet_Tabular tabResult = (MDDataSet_Tabular) result;
                    tabResult.metadata(writer);
                } else if (rowset) {
                    writer.verbatim(ROW_SET_XML_SCHEMA);
                } else {
                    writer.verbatim(MD_DATA_SET_XML_SCHEMA);
                }
            } else {
                if (rowset) {
                    writer.verbatim(EMPTY_ROW_SET_XML_SCHEMA);
                } else {
                    writer.verbatim(EMPTY_MD_DATA_SET_XML_SCHEMA);
                }
            }
        }

        try {
            if ((content == Enumeration.Content.Data)
                    || (content == Enumeration.Content.SchemaData)) {
                if (result != null) {
                    result.unparse(writer);
                }
            }
        } catch (XmlaException xex) {
            throw xex;
        } catch (Throwable t) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_EXECUTE_UNPARSE_CODE,
                HSB_EXECUTE_UNPARSE_FAULT_FS,
                t);
        } finally {
            writer.endElement(); // root
            writer.endElement(); // return
            writer.endElement(); // ExecuteResponse
        }

        writer.endDocument();
    }

    /**
     * Computes the XML Schema for a dataset.
     *
     * @param writer SAX writer
     * @see RowsetDefinition#writeRowsetXmlSchema(SaxWriter)
     */
    static void writeDatasetXmlSchema(SaxWriter writer, int settype) {
        String setNsXmla = (settype == ROW_SET)
                ? XmlaConstants.NS_XMLA_ROWSET
                : XmlaConstants.NS_XMLA_MDDATASET;

        writer.startElement("xsd:schema", new String[] {
            "xmlns:xsd", XmlaConstants.NS_XSD,
            "targetNamespace", setNsXmla,
            "xmlns", setNsXmla,
            "xmlns:xsi", XmlaConstants.NS_XSI,
            "xmlns:sql", NS_XML_SQL,
            "elementFormDefault", "qualified"
        });

        // MemberType

        writer.startElement("xsd:complexType", new String[] {
            "name", "MemberType"
        });
        writer.startElement("xsd:sequence");
        writer.element("xsd:element", new String[] {
            "name", "UName",
            "type", "xsd:string",
        });
        writer.element("xsd:element", new String[] {
            "name", "Caption",
            "type", "xsd:string",
        });
        writer.element("xsd:element", new String[] {
            "name", "LName",
            "type", "xsd:string",
        });
        writer.element("xsd:element", new String[] {
            "name", "LNum",
            "type", "xsd:unsignedInt",
        });
        writer.element("xsd:element", new String[] {
            "name", "DisplayInfo",
            "type", "xsd:unsignedInt",
        });
        writer.startElement("xsd:sequence", new String[] {
            "maxOccurs", "unbounded",
            "minOccurs", "0",
        });
        writer.element("xsd:any", new String[] {
            "processContents", "lax",
            "maxOccurs", "unbounded",
        });
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:sequence
        writer.element("xsd:attribute", new String[] {
            "name", "Hierarchy",
            "type", "xsd:string",
        });
        writer.endElement(); // xsd:complexType name="MemberType"

        // PropType

        writer.startElement("xsd:complexType", new String[] {
            "name", "PropType",
        });
        writer.element("xsd:attribute", new String[] {
            "name", "name",
            "type", "xsd:string",
        });
        writer.endElement(); // xsd:complexType name="PropType"

        // TupleType

        writer.startElement("xsd:complexType", new String[] {
            "name", "TupleType"
        });
        writer.startElement("xsd:sequence", new String[] {
            "maxOccurs", "unbounded"
        });
        writer.element("xsd:element", new String[] {
            "name", "Member",
            "type", "MemberType",
        });
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType name="TupleType"

        // MembersType

        writer.startElement("xsd:complexType", new String[] {
            "name", "MembersType"
        });
        writer.startElement("xsd:sequence", new String[] {
            "maxOccurs", "unbounded",
        });
        writer.element("xsd:element", new String[] {
            "name", "Member",
            "type", "MemberType",
        });
        writer.endElement(); // xsd:sequence
        writer.element("xsd:attribute", new String[] {
            "name", "Hierarchy",
            "type", "xsd:string",
        });
        writer.endElement(); // xsd:complexType

        // TuplesType

        writer.startElement("xsd:complexType", new String[] {
            "name", "TuplesType"
        });
        writer.startElement("xsd:sequence", new String[] {
            "maxOccurs", "unbounded",
        });
        writer.element("xsd:element", new String[] {
            "name", "Tuple",
            "type", "TupleType",
        });
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        // CrossProductType

        writer.startElement("xsd:complexType", new String[] {
            "name", "CrossProductType",
        });
        writer.startElement("xsd:sequence");
        writer.startElement("xsd:choice", new String[] {
            "minOccurs", "0",
            "maxOccurs", "unbounded",
        });
        writer.element("xsd:element", new String[] {
            "name", "Members",
            "type", "MembersType"
        });
        writer.element("xsd:element", new String[] {
            "name", "Tuples",
            "type", "TuplesType"
        });
        writer.endElement(); // xsd:choice
        writer.endElement(); // xsd:sequence
        writer.element("xsd:attribute", new String[] {
            "name", "Size",
            "type", "xsd:unsignedInt"
        });
        writer.endElement(); // xsd:complexType

        // OlapInfo

        writer.startElement("xsd:complexType", new String[] {
            "name", "OlapInfo",
        });
        writer.startElement("xsd:sequence");

        { // <CubeInfo>
            writer.startElement("xsd:element", new String[] {
                "name", "CubeInfo"
            });
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence");

            { // <Cube>
                writer.startElement("xsd:element", new String[] {
                    "name", "Cube",
                    "maxOccurs", "unbounded"
                });
                writer.startElement("xsd:complexType");
                writer.startElement("xsd:sequence");

                writer.element("xsd:element", new String[] {
                    "name", "CubeName",
                    "type", "xsd:string"
                });

                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
                writer.endElement(); // xsd:element name=Cube
            }

            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=CubeInfo
        }
        { // <AxesInfo>
            writer.startElement("xsd:element", new String[] {
                "name", "AxesInfo"
            });
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence");
            { // <AxisInfo>
                writer.startElement("xsd:element", new String[] {
                    "name", "AxisInfo",
                    "maxOccurs", "unbounded"
                });
                writer.startElement("xsd:complexType");
                writer.startElement("xsd:sequence");

                { // <HierarchyInfo>
                    writer.startElement("xsd:element", new String[] {
                        "name", "HierarchyInfo",
                        "minOccurs", "0",
                        "maxOccurs", "unbounded"
                    });
                    writer.startElement("xsd:complexType");
                    writer.startElement("xsd:sequence");
                    writer.startElement("xsd:sequence", new String[] {
                        "maxOccurs", "unbounded"
                    });
                    writer.element("xsd:element", new String[] {
                        "name", "UName",
                        "type", "PropType"
                    });
                    writer.element("xsd:element", new String[] {
                        "name", "Caption",
                        "type", "PropType"
                    });
                    writer.element("xsd:element", new String[] {
                        "name", "LName",
                        "type", "PropType"
                    });
                    writer.element("xsd:element", new String[] {
                        "name", "LNum",
                        "type", "PropType"
                    });
                    writer.element("xsd:element", new String[] {
                        "name", "DisplayInfo",
                        "type", "PropType",
                        "minOccurs", "0",
                        "maxOccurs", "unbounded"
                    });
                    if (false) writer.element("xsd:element", new String[] {
                        "name", "PARENT_MEMBER_NAME",
                        "type", "PropType",
                        "minOccurs", "0",
                        "maxOccurs", "unbounded"
                    });
                    writer.endElement(); // xsd:sequence

                    // This is the Depth element for JPivot??
                    writer.startElement("xsd:sequence");
                    writer.element("xsd:any", new String[] {
                        "processContents", "lax",
                         "minOccurs", "0",
                        "maxOccurs", "unbounded"
                    });
                    writer.endElement(); // xsd:sequence

                    writer.endElement(); // xsd:sequence
                    writer.endElement(); // xsd:complexType
                    writer.element("xsd:attribute", new String[] {
                        "name", "name",
                        "type", "xsd:string",
                        "use", "required"
                    });
                    writer.endElement(); // xsd:element name=HierarchyInfo
                }
                writer.endElement(); // xsd:sequence
                writer.element("xsd:attribute", new String[] {
                    "name", "name",
                    "type", "xsd:string"
                });
                writer.endElement(); // xsd:complexType
                writer.endElement(); // xsd:element name=AxisInfo
            }
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=AxesInfo
        }

        // CellInfo

        { // <CellInfo>
            writer.startElement("xsd:element", new String[] {
                "name", "CellInfo"
            });
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence");
            writer.startElement("xsd:sequence", new String[] {
                "minOccurs", "0",
                "maxOccurs", "unbounded"
            });
            writer.startElement("xsd:choice");
            writer.element("xsd:element", new String[] {
                "name", "Value",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "FmtValue",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "BackColor",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "ForeColor",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "FontName",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "FontSize",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "FontFlags",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "FormatString",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "NonEmptyBehavior",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "SolveOrder",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "Updateable",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "Visible",
                "type", "PropType"
            });
            writer.element("xsd:element", new String[] {
                "name", "Expression",
                "type", "PropType"
            });
            writer.endElement(); // xsd:choice
            writer.endElement(); // xsd:sequence
            writer.startElement("xsd:sequence", new String[] {
                "maxOccurs", "unbounded",
                "minOccurs", "0"
            });
            writer.element("xsd:any", new String[] {
                "processContents", "lax",
                "maxOccurs", "unbounded"
            });
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=CellInfo
        }

        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        // Axes

        writer.startElement("xsd:complexType", new String[] {
            "name", "Axes"
        });
        writer.startElement("xsd:sequence", new String[] {
            "maxOccurs", "unbounded"
        });
        { // <Axis>
            writer.startElement("xsd:element", new String[] {
                "name", "Axis"
            });
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:choice", new String[] {
                "minOccurs", "0",
                "maxOccurs", "unbounded"
            });
            writer.element("xsd:element", new String[] {
                "name", "CrossProduct",
                "type", "CrossProductType"
            });
            writer.element("xsd:element", new String[] {
                "name", "Tuples",
                "type", "TuplesType"
            });
            writer.element("xsd:element", new String[] {
                "name", "Members",
                "type", "MembersType"
            });
            writer.endElement(); // xsd:choice
            writer.element("xsd:attribute", new String[] {
                "name", "name",
                "type", "xsd:string"
            });
            writer.endElement(); // xsd:complexType
        }
        writer.endElement(); // xsd:element
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        // CellData

        writer.startElement("xsd:complexType", new String[] {
            "name", "CellData"
        });
        writer.startElement("xsd:sequence");
        { // <Cell>
            writer.startElement("xsd:element", new String[] {
                "name", "Cell",
                "minOccurs", "0",
                "maxOccurs", "unbounded"
            });
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence", new String[] {
                "maxOccurs", "unbounded"
            });
            writer.startElement("xsd:choice");
            writer.element("xsd:element", new String[] {
                "name", "Value"
            });
            writer.element("xsd:element", new String[] {
                "name", "FmtValue",
                "type", "xsd:string"
            });
            writer.element("xsd:element", new String[] {
                "name", "BackColor",
                "type", "xsd:unsignedInt"
            });
            writer.element("xsd:element", new String[] {
                "name", "ForeColor",
                "type", "xsd:unsignedInt"
            });
            writer.element("xsd:element", new String[] {
                "name", "FontName",
                "type", "xsd:string"
            });
            writer.element("xsd:element", new String[] {
                "name", "FontSize",
                "type", "xsd:unsignedShort"
            });
            writer.element("xsd:element", new String[] {
                "name", "FontFlags",
                "type", "xsd:unsignedInt"
            });
            writer.element("xsd:element", new String[] {
                "name", "FormatString",
                "type", "xsd:string"
            });
            writer.element("xsd:element", new String[] {
                "name", "NonEmptyBehavior",
                "type", "xsd:unsignedShort"
            });
            writer.element("xsd:element", new String[] {
                "name", "SolveOrder",
                "type", "xsd:unsignedInt"
            });
            writer.element("xsd:element", new String[] {
                "name", "Updateable",
                "type", "xsd:unsignedInt"
            });
            writer.element("xsd:element", new String[] {
                "name", "Visible",
                "type", "xsd:unsignedInt"
            });
            writer.element("xsd:element", new String[] {
                "name", "Expression",
                "type", "xsd:string"
            });
            writer.endElement(); // xsd:choice
            writer.endElement(); // xsd:sequence
            writer.element("xsd:attribute", new String[] {
                "name", "CellOrdinal",
                "type", "xsd:unsignedInt",
                "use", "required"
            });
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=Cell
        }
        writer.endElement(); // xsd:sequence
        writer.endElement(); // xsd:complexType

        { // <root>
            writer.startElement("xsd:element", new String[] {
                "name", "root"
            });
            writer.startElement("xsd:complexType");
            writer.startElement("xsd:sequence", new String[] {
                "maxOccurs", "unbounded"
            });
            writer.element("xsd:element", new String[] {
                "name", "OlapInfo",
                "type", "OlapInfo"
            });
            writer.element("xsd:element", new String[] {
                "name", "Axes",
                "type", "Axes"
            });
            writer.element("xsd:element", new String[] {
                "name", "CellData",
                "type", "CellData"
            });
            writer.endElement(); // xsd:sequence
            writer.endElement(); // xsd:complexType
            writer.endElement(); // xsd:element name=root
        }

        writer.endElement(); // xsd:schema
    }

    static void writeEmptyDatasetXmlSchema(SaxWriter writer, int settype) {
        String setNsXmla = XmlaConstants.NS_XMLA_ROWSET;
        writer.startElement("xsd:schema", new String[] {
            "xmlns:xsd", XmlaConstants.NS_XSD,
            "targetNamespace", setNsXmla,
            "xmlns", setNsXmla,
            "xmlns:xsi", XmlaConstants.NS_XSI,
            "xmlns:sql", NS_XML_SQL,
            "elementFormDefault", "qualified"
        });

        writer.element("xsd:element", new String[] {
            "name", "root"
        });

        writer.endElement(); // xsd:schema
    }

    private QueryResult executeDrillThroughQuery(XmlaRequest request)
            throws XmlaException {

        checkFormat(request);

        DataSourcesConfig.DataSource ds = getDataSource(request);
        DataSourcesConfig.Catalog dsCatalog = getCatalog(request, ds);
        String role = request.getRole();
        final Map<String, String> properties = request.getProperties();

        final RolapConnection connection =
            (RolapConnection) getConnection(dsCatalog, role);

        final String statement = request.getStatement();
        final Query query = connection.parseQuery(statement);
        final Result result = connection.execute(query);
        Cell dtCell = result.getCell(new int[] {0, 0});

        if (!dtCell.canDrillThrough()) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_NOT_ALLOWED_CODE,
                HSB_DRILL_THROUGH_NOT_ALLOWED_FAULT_FS,
                Util.newError("Cannot do DrillThrough operation on the cell"));
        }

        String dtSql = dtCell.getDrillThroughSQL(true);
        java.sql.Connection sqlConn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            final String advancedFlag =
                properties.get(PropertyDefinition.AdvancedFlag.name());
            if ("true".equals(advancedFlag)) {
                final Position position = result.getAxes()[0].getPositions().get(0);
                Member[] members = position.toArray(new Member[position.size()]);

                CellRequest cellRequest =
                    RolapAggregationManager.makeRequest(members, false, false);
                List<MondrianDef.Relation> relationList =
                    new ArrayList<MondrianDef.Relation>();
                final RolapStar.Table factTable =
                    cellRequest.getMeasure().getStar().getFactTable();
                MondrianDef.Relation relation = factTable.getRelation();
                relationList.add(relation);

                for (RolapStar.Table table : factTable.getChildren()) {
                    relationList.add(table.getRelation());
                }
                List<String> truncatedTableList = new ArrayList<String>();
                sqlConn = connection.getDataSource().getConnection();
                stmt = sqlConn.createStatement();
                List<List<String>> fields = new ArrayList<List<String>>();

                Map<String, List<String>> tableFieldMap =
                    new HashMap<String, List<String>>();
                for (MondrianDef.Relation relation1 : relationList) {
                    final String tableName = relation1.toString();
                    List<String> fieldNameList = new ArrayList<String>();
                    // FIXME: Quote table name
                    dtSql = "SELECT * FROM " + tableName + " WHERE 1=2";
                    rs = stmt.executeQuery(dtSql);
                    ResultSetMetaData rsMeta = rs.getMetaData();
                    for (int j = 1; j <= rsMeta.getColumnCount(); j++) {
                        String colName = rsMeta.getColumnName(j);
                        boolean colNameExists = false;
                        for (int tableCount = 0; tableCount < fields.size(); tableCount++) {
                            List<String> prvField = fields.get(tableCount);
                            if (prvField.contains(colName)) {
                                colNameExists = true;
                                break;
                            }
                        }
                        if (!colNameExists) {
                            fieldNameList.add(rsMeta.getColumnName(j));
                        }
                    }
                    fields.add(fieldNameList);
                    String truncatedTableName =
                        tableName.substring(tableName.lastIndexOf(".") + 1);
                    truncatedTableList.add(truncatedTableName);
                    tableFieldMap.put(truncatedTableName, fieldNameList);
                }
                return new TabularRowSet(tableFieldMap, truncatedTableList);
            } else {
                int count = -1;
                if (MondrianProperties.instance().EnableTotalCount.booleanValue()) {
                    count = dtCell.getDrillThroughCount();
                }

                sqlConn = connection.getDataSource().getConnection();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("drill through sql: " + dtSql);
                }
                int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
                int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
                if (!sqlConn.getMetaData().supportsResultSetConcurrency(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {
                    // downgrade to non-scroll cursor, since we can
                    // fake absolute() via forward fetch
                    resultSetType = ResultSet.TYPE_FORWARD_ONLY;
                }
                rs = RolapUtil.executeQuery(
                    sqlConn, dtSql, -1, "XmlaHandler.executeDrillThroughQuery",
                    resultSetType,
                    resultSetConcurrency);
                return new TabularRowSet(
                    rs, request.drillThroughMaxRows(),
                    request.drillThroughFirstRowset(), count,
                    resultSetType);
            }
        } catch (XmlaException xex) {
            throw xex;
        } catch (SQLException sqle) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_SQL_CODE,
                HSB_DRILL_THROUGH_SQL_FAULT_FS,
                Util.newError(sqle, "Error in drill through"));
        } catch (RuntimeException e) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_SQL_CODE,
                HSB_DRILL_THROUGH_SQL_FAULT_FS,
                Util.newError(e, "Error in drill through"));
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (SQLException ignored) {
            }
            try {
                if (sqlConn != null && !sqlConn.isClosed()) sqlConn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    static class TabularRowSet implements QueryResult {
        private final String[] headers;
        private final List<Object[]> rows;
        private int totalCount;

        public TabularRowSet(
                ResultSet rs,
                int maxRows,
                int firstRowset,
                int totalCount,
                int resultSetType) throws SQLException {
            this.totalCount = totalCount;
            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();

            // populate header
            headers = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                headers[i] = md.getColumnLabel(i + 1);
            }

            // skip to first rowset specified in request
            int firstRow = (firstRowset <= 0 ? 1 : firstRowset);
            if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
                for (int i = 0; i < firstRow; ++i) {
                    if (!rs.next()) {
                        break;
                    }
                }
            } else {
                rs.absolute(firstRow);
            }

            // populate data
            rows = new ArrayList<Object[]>();
            maxRows = (maxRows <= 0 ? Integer.MAX_VALUE : maxRows);
            do {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                rows.add(row);
            } while (rs.next() && --maxRows > 0);
        }

        /**
         * Alternate constructor for advanced drill-through.
         *
         * @param tableFieldMap Map from table name to a list of the names of
         *      the fields in the table
         * @param tableList List of table names
         * @throws SQLException
         */
        public TabularRowSet(
            Map<String, List<String>> tableFieldMap, List<String> tableList)
            throws SQLException
        {
            List<String> headerList = new ArrayList<String>();
            for (String tableName : tableList) {
                List<String> fieldNames = tableFieldMap.get(tableName);
                for (String fieldName : fieldNames) {
                    headerList.add(tableName + "." + fieldName);
                }
            }
            headers = headerList.toArray(new String[headerList.size()]);
            rows = new ArrayList<Object[]>();
            Object[] row = new Object[headers.length];
            for (int k = 0; k < row.length; k++) {
                row[k] = k;
            }
            rows.add(row);
        }

        public void unparse(SaxWriter writer) throws SAXException {
            String[] encodedHeader = new String[headers.length];
            for (int i = 0; i < headers.length; i++) {
                // replace invalid XML element name, like " ", with "_x0020_" in
                // column headers, otherwise will generate a badly-formatted xml
                // doc.
                encodedHeader[i] = XmlaUtil.encodeElementName(headers[i]);
            }

            // write total count row if enabled
            if (totalCount >= 0) {
                String countStr = Integer.toString(totalCount);
                writer.startElement("row");
                for (String anEncodedHeader : encodedHeader) {
                    writer.startElement(anEncodedHeader);
                    writer.characters(countStr);
                    writer.endElement();
                }
                writer.endElement(); // row
            }

            for (Object[] row : rows) {
                writer.startElement("row");
                for (int i = 0; i < row.length; i++) {
                    writer.startElement(encodedHeader[i]);
                    Object value = row[i];
                    if (value == null) {
                        writer.characters("null");
                    } else {
                        String valueString = value.toString();
                        if (value instanceof Number) {
                            valueString =
                                XmlaUtil.normalizeNumericString(valueString);
                        }
                        writer.characters(valueString);
                    }
                    writer.endElement();
                }
                writer.endElement(); // row
            }
        }

		public TabularRowSet(ResultSet rs) throws SQLException {
			ResultSetMetaData md = rs.getMetaData();
			int columnCount = md.getColumnCount();

			// populate header
			headers = new String[columnCount];
			for (int i = 0; i < columnCount; i++) {
				headers[i] = md.getColumnName(i + 1);
			}

			// populate data
			rows = new ArrayList<Object[]>();
			while (rs.next()) {
				Object[] row = new Object[columnCount];
				for (int i = 0; i < columnCount; i++) {
					row[i] = rs.getObject(i + 1);
				}
				rows.add(row);
			}
		}
    }

    private QueryResult executeQuery(XmlaRequest request)
            throws XmlaException {
        final String statement = request.getStatement();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("mdx: \"" + statement + "\"");
        }

        if ((statement == null) || (statement.length() == 0)) {
            return null;
        } else {
            checkFormat(request);

            DataSourcesConfig.DataSource ds = getDataSource(request);
            DataSourcesConfig.Catalog dsCatalog = getCatalog(request, ds);
            String role = request.getRole();

            final Connection connection = getConnection(dsCatalog, role);

            final Query query;
            try {
                query = connection.parseQuery(statement);
            } catch (XmlaException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_PARSE_QUERY_CODE,
                    HSB_PARSE_QUERY_FAULT_FS,
                    ex);
            }
            final Result result;
            try {
                result = connection.execute(query);
            } catch (XmlaException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new XmlaException(
                    SERVER_FAULT_FC,
                    HSB_EXECUTE_QUERY_CODE,
                    HSB_EXECUTE_QUERY_FAULT_FS,
                    ex);
            }

            final String formatName = request.getProperties().get(
                    PropertyDefinition.Format.name());
            Enumeration.Format format = valueOf(Enumeration.Format.class, formatName,
                null);

            if (format == Enumeration.Format.Multidimensional) {
                return new MDDataSet_Multidimensional(result);
            } else {
                return new MDDataSet_Tabular(result);
            }
        }
    }

    /**
     * Deduces the XML datatype from the declared datatype
     * of the measure, if present. (It comes from the
     * "datatype" attribute of the "Measure" element.) If
     * not present, use the value type to guess.
     *
     * <p>The value type depends upon the RDBMS and the JDBC
     * driver, so it tends to produce inconsistent results
     * between platforms.
     *
     * @param cell Cell
     * @param value Value of the cell
     * @return XSD data type (e.g. "xsd:int", "xsd:double", "xsd:string")
     */
    protected static String deduceValueType(Cell cell, final Object value) {
        String datatype = (String)
                cell.getPropertyValue(Property.DATATYPE.getName());
        if (datatype != null) {
            if (datatype.equals("Integer")) {
                return "xsd:int";
            } else if (datatype.equals("Numeric")) {
                return "xsd:double";
            } else {
                return "xsd:string";
            }
        } else if (value instanceof Integer || value instanceof Long) {
            return "xsd:int";
        } else if (value instanceof Double || value instanceof BigDecimal) {
            return "xsd:double";
        } else {
            return "xsd:string";
        }
    }
    protected static String deduceValueType(Evaluator evaluator, 
                                            final Object value) {
        String datatype = (String)
                evaluator.getProperty(Property.DATATYPE.getName(), null);
        if (datatype != null) {
            if (datatype.equals("Integer")) {
                return "xsd:int";
            } else if (datatype.equals("Numeric")) {
                return "xsd:double";
            } else {
                return "xsd:string";
            }
        } else if (value instanceof Integer || value instanceof Long) {
            return "xsd:int";
        } else if (value instanceof Double || value instanceof BigDecimal) {
            return "xsd:double";
        } else {
            return "xsd:string";
        }
    }

    static abstract class MDDataSet implements QueryResult {
        protected final Result result;

        protected static final String[] cellProps = new String[] {
            "Value",
            "FmtValue",
            "FormatString"};

        protected static final String[] cellPropLongs = new String[] {
            Property.VALUE.name,
            Property.FORMATTED_VALUE.name,
            Property.FORMAT_STRING.name};

        protected static final String[] defaultProps = new String[] {
            "UName",
            "Caption",
            "LName",
            "LNum",
            "DisplayInfo",
            // Not in spec nor generated by SQL Server
//            "Depth"
            };
        protected static final Map<String, String> longPropNames = new HashMap<String, String>();

        static {
            longPropNames.put("UName", Property.MEMBER_UNIQUE_NAME.name);
            longPropNames.put("Caption", Property.MEMBER_CAPTION.name);
            longPropNames.put("LName", Property.LEVEL_UNIQUE_NAME.name);
            longPropNames.put("LNum", Property.LEVEL_NUMBER.name);
            longPropNames.put("DisplayInfo", Property.DISPLAY_INFO.name);
        }

        protected MDDataSet(Result result) {
            this.result = result;
        }
    }

    static class MDDataSet_Multidimensional extends MDDataSet {
        private Hierarchy[] slicerAxisHierarchies;

        protected MDDataSet_Multidimensional(Result result) {
            super(result);
        }

        public void unparse(SaxWriter writer) throws SAXException {
            olapInfo(writer);
            axes(writer);
            cellData(writer);
        }

        private void olapInfo(SaxWriter writer) {
            // What are all of the cube's hierachies
            Cube cube = result.getQuery().getCube();
            List<Hierarchy> hierarchyList = new ArrayList<Hierarchy>();
            Dimension[] dimensions = cube.getDimensions();
            for (Dimension dimension : dimensions) {
                Hierarchy[] hierarchies = dimension.getHierarchies();
                for (Hierarchy hierarchy : hierarchies) {
                    hierarchyList.add(hierarchy);
                }
            }

            writer.startElement("OlapInfo");
            writer.startElement("CubeInfo");
            writer.startElement("Cube");
            writer.startElement("CubeName");
            writer.characters(result.getQuery().getCube().getName());
            writer.endElement();
            writer.endElement();
            writer.endElement(); // CubeInfo

            // create AxesInfo for axes
            // -----------
            writer.startElement("AxesInfo");
            final Axis[] axes = result.getAxes();
            final QueryAxis[] queryAxes = result.getQuery().getAxes();
            //axisInfo(writer, result.getSlicerAxis(), "SlicerAxis");
            List<Hierarchy> axisHierarchyList = new ArrayList<Hierarchy>();
            for (int i = 0; i < axes.length; i++) {
                Hierarchy[] hiers =
                        axisInfo(writer, axes[i], queryAxes[i], "Axis" + i);
                for (Hierarchy hier : hiers) {
                    axisHierarchyList.add(hier);
                }

            }
            // Remove all seen axes.
            // What this does is for each hierarchy in one of the
            // axes, find its Dimension and then all of that Dimension's
            // Hierarchies (most of the time you are back to the original
            // Hierarchy) and then for each Hierarchy remove it from the
            // list of all Hierarchies.
            // NOTE: Don't know if this is correct!!
            for (Hierarchy hier1 : axisHierarchyList) {
                Dimension dim = hier1.getDimension();
                Hierarchy[] hiers = dim.getHierarchies();
                for (Hierarchy h : hiers) {
                    String uniqueName = h.getUniqueName();
                    for (Iterator<Hierarchy> it2 = hierarchyList.iterator(); it2
                        .hasNext();) {
                        Hierarchy hier2 = it2.next();
                        if (uniqueName.equals(hier2.getUniqueName())) {
                            it2.remove();
                            break;
                        }
                    }
                }
            }

            ///////////////////////////////////////////////
            // create AxesInfo for slicer axes
            //
            Hierarchy[] hierarchies =
                hierarchyList.toArray(new Hierarchy[hierarchyList.size()]);
            writer.startElement("AxisInfo",
                    new String[] { "name", "SlicerAxis"});
            final QueryAxis slicerAxis = result.getQuery().getSlicerAxis();
            writeHierarchyInfo(writer, hierarchies, getProps(slicerAxis));
            writer.endElement(); // AxisInfo
            slicerAxisHierarchies = hierarchies;
            //
            ///////////////////////////////////////////////


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

        private Hierarchy[] axisInfo(
                SaxWriter writer,
                Axis axis,
                QueryAxis queryAxis,
                String axisName) {

            writer.startElement("AxisInfo", new String[] { "name", axisName});

            Hierarchy[] hierarchies;
            Iterator<Position> it = axis.getPositions().iterator();
            if (it.hasNext()) {
                final Position position = it.next();
                List<Hierarchy> l = new ArrayList<Hierarchy>();
                for (Member member: position) {
                    l.add(member.getHierarchy());
                }
                hierarchies = l.toArray(new Hierarchy[l.size()]);
            } else {
                hierarchies = new Hierarchy[0];
                //final QueryAxis queryAxis = this.result.getQuery().axes[i];
                // TODO:
            }
            String[] props = getProps(queryAxis);
            writeHierarchyInfo(writer, hierarchies, props);

            writer.endElement(); // AxisInfo

            return hierarchies;
        }


        private void writeHierarchyInfo(
                SaxWriter writer,
                Hierarchy[] hierarchies,
                String[] props) {

            for (Hierarchy hierarchy : hierarchies) {
                writer.startElement(
                    "HierarchyInfo", new String[]{
                    "name", hierarchy.getName()
                });
                for (int k = 0; k < props.length; k++) {
                    final String prop = props[k];
                    String longPropName = longPropNames.get(prop);
                    if (longPropName == null) {
                        longPropName = prop;
                    }
                    writer.element(
                        prop, new String[]{
                        "name",
                        hierarchy.getUniqueName() + "." +
                            Util.quoteMdxIdentifier(longPropName),
                    });
                }
                writer.endElement(); // HierarchyInfo
            }
        }

        private void axes(SaxWriter writer) {
            writer.startElement("Axes");
            //axis(writer, result.getSlicerAxis(), "SlicerAxis");
            final Axis[] axes = result.getAxes();
            final QueryAxis[] queryAxes = result.getQuery().getAxes();
            for (int i = 0; i < axes.length; i++) {
                final String[] props = getProps(queryAxes[i]);
                axis(writer, axes[i], props, "Axis" + i);
            }

            ////////////////////////////////////////////
            // now generate SlicerAxis information
            //
            Hierarchy[] hierarchies = slicerAxisHierarchies;
            writer.startElement("Axis", new String[] { "name", "SlicerAxis"});
            writer.startElement("Tuples");
            writer.startElement("Tuple");

            final QueryAxis slicerAxis = result.getQuery().getSlicerAxis();

            Iterator<Position> it = 
                        result.getSlicerAxis().getPositions().iterator();
            it.hasNext();
            final List<Member> slicerMembers = it.next();
            for (Hierarchy hierarchy : hierarchies) {
                // Find which member is on the slicer. If it's not explicitly
                // there, use the default member.
                Member member = hierarchy.getDefaultMember();
                for (Member slicerMember : slicerMembers) {
                    if (slicerMember.getHierarchy().equals(hierarchy)) {
                        member = slicerMember;
                        break;
                    }
                }

                if (member != null) {
                    slicerAxis(writer, member, getProps(slicerAxis));
                } else {
                    LOGGER.warn(
                        "Can not create SlicerAxis: " +
                            "null default member for Hierarchy " +
                            hierarchy.getUniqueName());
                }
            }

            //
            ////////////////////////////////////////////

            writer.endElement(); // Tuple
            writer.endElement(); // Tuples
            writer.endElement(); // Axis



            writer.endElement(); // Axes
        }

        private String[] getProps(QueryAxis queryAxis) {
            if (queryAxis == null) {
                return defaultProps;
            }
            Id[] dimensionProperties = queryAxis.getDimensionProperties();
            if (dimensionProperties.length == 0) {
                return defaultProps;
            }
            String[] props = new String[defaultProps.length + dimensionProperties.length];
            System.arraycopy(defaultProps, 0, props, 0, defaultProps.length);
            for (int i = 0; i < dimensionProperties.length; i++) {
                props[defaultProps.length + i] =
                        dimensionProperties[i].toStringArray()[0];
            }
            return props;
        }

        private void axis(SaxWriter writer, Axis axis, String[] props, String axisName) {
            writer.startElement("Axis", new String[] { "name", axisName});
            writer.startElement("Tuples");

            List<Position> positions = axis.getPositions();
            Iterator<Position> pit = positions.iterator();
            Position prevPosition = null;
            Position position = pit.hasNext() ? pit.next() : null;
            Position nextPosition = pit.hasNext() ? pit.next() : null;
            while (position != null) {
                writer.startElement("Tuple");
                int k = 0;
                for (Member member: position) {
                    writer.startElement("Member", new String[] {
                        "Hierarchy", member.getHierarchy().getName()});
                    for (String prop1 : props) {
                        Object value;
                        final String prop = prop1;
                        String propLong = longPropNames.get(prop);
                        if (propLong == null) {
                            propLong = prop;
                        }
                        if (propLong.equals(Property.DISPLAY_INFO.name)) {
                            Integer childrenCard = (Integer) member
                              .getPropertyValue(Property.CHILDREN_CARDINALITY.name);
                            value = calculateDisplayInfo(prevPosition,
                                        nextPosition,
                                        member, k, childrenCard);
                        } else if (propLong.equals(Property.DEPTH.name)) {
                            value = member.getDepth();
                        } else {
                            value = member.getPropertyValue(propLong);
                        }
                        if (value != null) {
                            writer.startElement(prop); // Properties
                            writer.characters(value.toString());
                            writer.endElement(); // Properties
                        }
                    }
                    writer.endElement(); // Member
                }
                k++;
                writer.endElement(); // Tuple
                prevPosition = position;
                position = nextPosition;
                nextPosition = pit.hasNext() ? pit.next() : null;
            }
            writer.endElement(); // Tuples
            writer.endElement(); // Axis
        }

        private void slicerAxis(
                SaxWriter writer, Member member, String[] props) {
            writer.startElement("Member", new String[] {
                "Hierarchy", member.getHierarchy().getName()});
            for (String prop : props) {
                Object value;
                String propLong = longPropNames.get(prop);
                if (propLong == null) {
                    propLong = prop;
                }
                if (propLong.equals(Property.DISPLAY_INFO.name)) {
                    Integer childrenCard =
                        (Integer) member
                            .getPropertyValue(Property.CHILDREN_CARDINALITY.name);
                    // NOTE: don't know if this is correct for
                    // SlicerAxis
                    int displayInfo = 0xffff & childrenCard;
/*
                    int displayInfo =
                        calculateDisplayInfo((j == 0 ? null : positions[j - 1]),
                          (j + 1 == positions.length ? null : positions[j + 1]),
                          member, k, childrenCard.intValue());
*/
                    value = displayInfo;
                } else if (propLong.equals(Property.DEPTH.name)) {
                    value = member.getDepth();
                } else {
                    value = member.getPropertyValue(propLong);
                }
                if (value != null) {
                    writer.startElement(prop); // Properties
                    writer.characters(value.toString());
                    writer.endElement(); // Properties
                }
            }
            writer.endElement(); // Member
        }

        private int calculateDisplayInfo(Position prevPosition, Position nextPosition,
                Member currentMember, int memberOrdinal, int childrenCount) {
            int displayInfo = 0xffff & childrenCount;

            if (nextPosition != null) {
                String currentUName = currentMember.getUniqueName();
                String nextParentUName = nextPosition.get(memberOrdinal).getParentUniqueName();
                displayInfo |= (currentUName.equals(nextParentUName) ? 0x10000 : 0);
            }
            if (prevPosition != null) {
                String currentParentUName = currentMember.getParentUniqueName();
                String prevParentUName = prevPosition.get(memberOrdinal).getParentUniqueName();
                displayInfo |= (currentParentUName != null && currentParentUName.equals(prevParentUName) ? 0x20000 : 0);
            }
            return displayInfo;
        }

        private void cellData(SaxWriter writer) {
            writer.startElement("CellData");
            final int axisCount = result.getAxes().length;
            int[] pos = new int[axisCount];
            int[] cellOrdinal = new int[] {0};

            Evaluator evaluator = RolapUtil.createEvaluator(result.getQuery());
            int axisOrdinal = axisCount-1;
            recurse(writer, pos, axisOrdinal, evaluator, cellOrdinal);

            writer.endElement(); // CellData
        }
        private void recurse(SaxWriter writer, int[] pos, 
                int axisOrdinal, Evaluator evaluator, int[] cellOrdinal) {
            if (axisOrdinal < 0) {
                emitCell(writer, pos, evaluator, cellOrdinal[0]++);

            } else {
                Axis axis = result.getAxes()[axisOrdinal];
                List<Position> positions = axis.getPositions();
                int i = 0;
                for (Position position: positions) {
                    pos[axisOrdinal] = i;
                    evaluator.setContext(position);
                    recurse(writer, pos, axisOrdinal - 1, evaluator, cellOrdinal);
                    i++;
                }
            }
        }
        private void emitCell(SaxWriter writer, int[] pos, 
                            Evaluator evaluator, int ordinal) {
            Cell cell = result.getCell(pos);
            if (cell.isNull()) {
                // Ignore null cell like MS AS
                return;
            }

            writer.startElement("Cell", new String[] {
                "CellOrdinal", Integer.toString(ordinal)});
            for (int i = 0; i < cellProps.length; i++) {
                String cellPropLong = cellPropLongs[i];
                final Object value = cell.getPropertyValue(cellPropLong);

                if (value != null) {
                    if (cellPropLong.equals(Property.VALUE.name)) {
                        String valueType = deduceValueType(evaluator, value);
                        writer.startElement(cellProps[i], new String[] {"xsi:type", valueType});
                    } else {
                        writer.startElement(cellProps[i]);
                    }

                    String valueString = value.toString();

                    if (cellPropLong.equals(Property.VALUE.name) &&
                            value instanceof Number) {
                        valueString = XmlaUtil.normalizeNumericString(valueString);
                    }

                    writer.characters(valueString);
                    writer.endElement();
                }
            }
            writer.endElement(); // Cell
        }
    }

    static abstract class ColumnHandler {
        protected final String name;
        protected final String encodedName;

        protected ColumnHandler(String name) {
            this.name = name;
            this.encodedName = XmlaUtil.encodeElementName(this.name);
        }

        abstract void write(SaxWriter writer, Cell cell, Member[] members);
        abstract void metadata(SaxWriter writer);
    }


    /**
     * Callback to handle one column, representing the combination of a
     * level and a property (e.g. [Store].[Store State].[MEMBER_UNIQUE_NAME])
     * in a flattened dataset.
     */
    static class CellColumnHandler extends ColumnHandler {

        CellColumnHandler(String name) {
            super(name);
        }

        public void metadata(SaxWriter writer) {
            writer.element("xsd:element", new String[] {
                "minOccurs", "0",
                "name", encodedName,
                "sql:field", name,
            });
        }

        public void write(
                SaxWriter writer, Cell cell, Member[] members) {
            if (cell.isNull()) {
                return;
            }
            Object value = cell.getValue();
            String valueString = value.toString();
            String valueType = deduceValueType(cell, value);

            writer.startElement(encodedName, new String[] {
                "xsi:type", valueType});
            if (value instanceof Number) {
                valueString = XmlaUtil.normalizeNumericString(valueString);
            }
            writer.characters(valueString);
            writer.endElement();
        }
    }

    /**
     * Callback to handle one column, representing the combination of a
     * level and a property (e.g. [Store].[Store State].[MEMBER_UNIQUE_NAME])
     * in a flattened dataset.
     */
    static class MemberColumnHandler extends ColumnHandler {
        private final String property;
        private final Level level;
        private final int memberOrdinal;

        public MemberColumnHandler(
                String property, Level level, int memberOrdinal) {
            super(level.getUniqueName() + "." +
                    Util.quoteMdxIdentifier(property));
            this.property = property;
            this.level = level;
            this.memberOrdinal = memberOrdinal;
        }

        public void metadata(SaxWriter writer) {
            writer.element("xsd:element", new String[] {
                "minOccurs", "0",
                "name", encodedName,
                "sql:field", name,
                "type", "xsd:string",
            });
        }

        public void write(
                SaxWriter writer, Cell cell, Member[] members) {
            Member member = members[memberOrdinal];
            final int depth = level.getDepth();
            if (member.getDepth() < depth) {
                // This column deals with a level below the current member.
                // There is no value to write.
                return;
            }
            while (member.getDepth() > depth) {
                member = member.getParentMember();
            }
            final Object propertyValue = member.getPropertyValue(property);
            if (propertyValue == null) {
                return;
            }

            writer.startElement(encodedName);
            writer.characters(propertyValue.toString());
            writer.endElement();
        }
    }

    static class MDDataSet_Tabular extends MDDataSet {
        private final boolean empty;
        private final int[] pos;
        private final int axisCount;
        private int cellOrdinal;

        private static final Id[] MemberCaptionIdArray = {
            new Id(new Id.Segment(Property.MEMBER_CAPTION.name, Id.Quoting.QUOTED))
        };
        private final Member[] members;
        private final ColumnHandler[] columnHandlers;

        public MDDataSet_Tabular(Result result) {
            super(result);
            final Axis[] axes = result.getAxes();
            axisCount = axes.length;
            pos = new int[axisCount];

            // Count dimensions, and deduce list of levels which appear on
            // non-COLUMNS axes.
            boolean empty = false;
            int dimensionCount = 0;
            for (int i = axes.length - 1; i > 0; i--) {
                Axis axis = axes[i];
                if (axis.getPositions().size() == 0) {
                    // If any axis is empty, the whole data set is empty.
                    empty = true;
                    continue;
                }
                dimensionCount += axis.getPositions().get(0).size();
            }
            this.empty = empty;

            // Build a list of the lowest level used on each non-COLUMNS axis.
            Level[] levels = new Level[dimensionCount];
            List<ColumnHandler> columnHandlerList = new ArrayList<ColumnHandler>();
            int memberOrdinal = 0;
            if (!empty) {
                for (int i = axes.length - 1; i > 0; i--) {
                    final Axis axis = axes[i];
                    final QueryAxis queryAxis = result.getQuery().getAxes()[i];
                    final int z0 = memberOrdinal; // save ordinal so can rewind
                    final List<Position> positions = axis.getPositions();
                    int jj = 0;
                    for (Position position: positions) {
                        memberOrdinal = z0; // rewind to start
                        //final Member[] members = position.members;
                        for (Member member : position) {
                            if (jj == 0 ||
                                member.getLevel().getDepth() >
                                    levels[memberOrdinal].getDepth()) {
                                levels[memberOrdinal] = member.getLevel();
                            }
                        }
                        ++memberOrdinal;
                        jj++;
                    }

                    // Now we know the lowest levels on this axis, add
                    // properties.
                    Id[] dimProps = queryAxis.getDimensionProperties();
                    if (dimProps.length == 0) {
                        dimProps = MemberCaptionIdArray;
                    }
                    for (int j = z0; j < memberOrdinal; j++) {
                        Level level = levels[j];
                        for (int k = 0; k <= level.getDepth(); k++) {
                            final Level level2 =
                                    level.getHierarchy().getLevels()[k];
                            if (level2.isAll()) {
                                continue;
                            }
                            for (Id dimProp : dimProps) {
                                columnHandlerList.add(
                                    new MemberColumnHandler(
                                        dimProp.toStringArray()[0],
                                        level2,
                                        j));
                            }
                        }
                    }
                }
            }
            this.members = new Member[memberOrdinal];

            // Deduce the list of column headings.
            Axis columnsAxis = axes[0];
            for (Position position : columnsAxis.getPositions()) {
                String name = null;
                int j = 0;
                for (Member member : position) {
                    if (j == 0) {
                        name = member.getUniqueName();
                    } else {
                        name = name + "." + member.getUniqueName();
                    }
                    j++;
                }
                columnHandlerList.add(
                    new CellColumnHandler(name));
            }

            this.columnHandlers =
                columnHandlerList.toArray(
                    new ColumnHandler[columnHandlerList.size()]);
        }

        public void metadata(SaxWriter writer) {
            if (empty) {
                return;
            }
            writer.startElement("xsd:schema", new String[] {
                "xmlns:xsd", XmlaConstants.NS_XSD,
                "targetNamespace", NS_XMLA_ROWSET,
                "xmlns", NS_XMLA_ROWSET,
                "xmlns:xsi", XmlaConstants.NS_XSI,
                "xmlns:sql", NS_XML_SQL,
                "elementFormDefault", "qualified"
            });

            { // <root>
                writer.startElement("xsd:element", new String[] {
                    "name", "root"
                });
                writer.startElement("xsd:complexType");
                writer.startElement("xsd:sequence");
                writer.element("xsd:element", new String[] {
                    "maxOccurs", "unbounded",
                    "minOccurs", "0",
                    "name", "row",
                    "type", "row",
                });
                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
                writer.endElement(); // xsd:element name=root
            }

            { // xsd:simpleType name="uuid"
                writer.startElement("xsd:simpleType", new String[] {
                    "name", "uuid",
                });
                writer.startElement("xsd:restriction", new String[] {
                    "base", "xsd:string",
                });
                writer.element("xsd:pattern", new String[] {
                    "value", "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
                });
                writer.endElement(); // xsd:restriction
                writer.endElement(); // xsd:simpleType
            }

            { // xsd:complexType name="row"
                writer.startElement("xsd:complexType", new String[] {
                    "name", "row",
                });
                writer.startElement("xsd:sequence");
                for (ColumnHandler columnHandler : columnHandlers) {
                    columnHandler.metadata(writer);
                }
                writer.endElement(); // xsd:sequence
                writer.endElement(); // xsd:complexType
            }
            writer.endElement(); // xsd:schema
        }

        public void unparse(SaxWriter writer) throws SAXException {
            if (empty) {
                return;
            }
            cellData(writer);
        }

        private void cellData(SaxWriter writer) throws SAXException {
            cellOrdinal = 0;
            if (axisCount == 0) { // For MDX like: SELECT FROM Sales
                emitCell(writer, result.getCell(pos));
            } else {
                recurse(writer, axisCount - 1, 0);
            }
        }

        private void recurse(
                SaxWriter writer,
                int axis,
                final int headerOrdinal) throws SAXException {
            final List<Position> positions = result.getAxes()[axis].getPositions();
            int i = 0;
            for (Position position: positions) {
                pos[axis] = i;
                if (axis == 0) {
                    final Cell cell = result.getCell(pos);
                    emitCell(writer, cell);
                } else {
                    // Populate headers and values with levels.
                    int ho = headerOrdinal;
                    for (Member member : position) {
                        members[ho++] = member;
                    }

                    recurse(writer, axis - 1, ho);
                }
                i++;
            }
        }

        private void emitCell(SaxWriter writer, Cell cell) {
            ++cellOrdinal;
            Util.discard(cellOrdinal);

            // Ignore empty cells.
            final Object cellValue = cell.getValue();
            if (cellValue == null) {
                return;
            }

            writer.startElement("row");
            for (ColumnHandler columnHandler : columnHandlers) {
                columnHandler.write(writer, cell, members);
            }
            writer.endElement();
        }
    }

    private void discover(XmlaRequest request, XmlaResponse response)
            throws XmlaException {

        final RowsetDefinition rowsetDefinition =
            RowsetDefinition.valueOf(request.getRequestType());
        Rowset rowset = rowsetDefinition.getRowset(request, this);

        final String formatName =
            request.getProperties().get(PropertyDefinition.Format.name());
        Enumeration.Format format =
            valueOf(
                Enumeration.Format.class,
                formatName,
                Enumeration.Format.Tabular);
        if (format != Enumeration.Format.Tabular) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_DISCOVER_FORMAT_CODE,
                HSB_DISCOVER_FORMAT_FAULT_FS,
                new UnsupportedOperationException("<Format>: only 'Tabular' allowed in Discover method type"));
        }
        final String contentName =
            request.getProperties().get(PropertyDefinition.Content.name());
        // default value is SchemaData
        Enumeration.Content content =
            valueOf(Enumeration.Content.class, contentName, CONTENT_DEFAULT);

        SaxWriter writer = response.getWriter();
        writer.startDocument();

        writer.startElement("xmla:DiscoverResponse", new String[] {
            "xmlns:xmla", NS_XMLA});
        writer.startElement("xmla:return");
        writer.startElement("root", new String[] {
            "xmlns", NS_XMLA_ROWSET,
            "xmlns:xsi", NS_XSI,
            "xmlns:xsd", NS_XSD,
            "xmlns:EX", NS_XMLA_EX
        });

        if ((content == Enumeration.Content.Schema)
                || (content == Enumeration.Content.SchemaData)) {
            rowset.rowsetDefinition.writeRowsetXmlSchema(writer);
        }

        try {
            if ((content == Enumeration.Content.Data)
                    || (content == Enumeration.Content.SchemaData)) {
                rowset.unparse(response);
            }
        } catch (XmlaException xex) {
            throw xex;
        } catch (Throwable t) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DISCOVER_UNPARSE_CODE,
                HSB_DISCOVER_UNPARSE_FAULT_FS,
                t);

        } finally {
            // keep the tags balanced, even if there's an error
            writer.endElement();
            writer.endElement();
            writer.endElement();
        }

        writer.endDocument();
    }

    /**
     * Returns enum constant of the specified enum type with the given name.
     *
     * @param enumType Enumerated type
     * @param name Name of constant
     * @param defaultValue Default value if constant is not found
     * @return Value, or null if name is null or value does not exist
     */
    private <E extends Enum<E>> E valueOf(
        Class<E> enumType,
        String name, E defaultValue)
    {
        if (name == null) {
            return defaultValue;
        } else {
            try {
                return Enum.valueOf(enumType, name);
            } catch (IllegalArgumentException e) {
                return defaultValue;
            }
        }
    }

    /**
     * Gets a Connection given a catalog (and implicitly the catalog's data
     * source) and a user role.
     *
     * @param catalog Catalog
     * @param role User role
     * @return Connection
     * @throws XmlaException
     */
    protected Connection getConnection(
            DataSourcesConfig.Catalog catalog,
            String role)
            throws XmlaException {
        DataSourcesConfig.DataSource ds = catalog.getDataSource();

        Util.PropertyList connectProperties =
            Util.parseConnectString(catalog.getDataSourceInfo());

        String catalogUrl = catalogLocator.locate(catalog.definition);

        if (LOGGER.isDebugEnabled()) {
            if (catalogUrl == null) {
                LOGGER.debug("XmlaHandler.getConnection: catalogUrl is null");
            } else {
                LOGGER.debug("XmlaHandler.getConnection: catalogUrl=" + catalogUrl);
            }
        }

        connectProperties.put("catalog", catalogUrl);

        // Checking access
        if (!DataSourcesConfig.DataSource.AUTH_MODE_UNAUTHENTICATED.equalsIgnoreCase(ds.getAuthenticationMode()) && null == role) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_ACCESS_DENIED_CODE,
                HSB_ACCESS_DENIED_FAULT_FS,
                new SecurityException("Access denied for data source needing authentication")
                );
        }

        connectProperties.put("role", role);
        RolapConnection conn =
            (RolapConnection) DriverManager.getConnection(connectProperties, null, false);

if (LOGGER.isDebugEnabled()) {
if (conn == null) {
LOGGER.debug("XmlaHandler.getConnection: returning connection null");
} else {
LOGGER.debug("XmlaHandler.getConnection: returning connection not null");
}
}
        return conn;
    }

    /**
     * Get the DataSource associated with the request property or null if
     * one was not specified.
     *
     * @param request
     * @return
     * @throws XmlaException
     */
    public DataSourcesConfig.DataSource getDataSource(XmlaRequest request)
                throws XmlaException {
        Map<String, String> properties = request.getProperties();
        final String dataSourceInfo =
            properties.get(PropertyDefinition.DataSourceInfo.name());
        if (!dataSourcesMap.containsKey(dataSourceInfo)) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_CONNECTION_DATA_SOURCE_CODE,
                HSB_CONNECTION_DATA_SOURCE_FAULT_FS,
                Util.newError("no data source is configured with name '" + dataSourceInfo + "'"));
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("XmlaHandler.getDataSource: dataSourceInfo=" +
                    dataSourceInfo);
        }

        final DataSourcesConfig.DataSource ds =
            dataSourcesMap.get(dataSourceInfo);
        if (LOGGER.isDebugEnabled()) {
            if (ds == null) {
                // TODO: this if a failure situation
                LOGGER.debug("XmlaHandler.getDataSource: ds is null");
            } else {
                LOGGER.debug("XmlaHandler.getDataSource: ds.dataSourceInfo=" +
                    ds.getDataSourceInfo());
            }
        }
        return ds;
    }

    /**
     * Get the DataSourcesConfig.Catalog with the given catalog name from the
     * DataSource's catalogs if there is a match and otherwise return null.
     *
     * @param ds
     * @param catalogName
     * @return DataSourcesConfig.Catalog or null
     */
    public DataSourcesConfig.Catalog getCatalog(
            DataSourcesConfig.DataSource ds,
            String catalogName) {
        DataSourcesConfig.Catalog[] catalogs = ds.catalogs.catalogs;
        if (catalogName == null) {
            // if there is no catalog name - its optional and there is
            // only one, then return it.
            if (catalogs.length == 1) {
                return catalogs[0];
            }
        } else {
            for (DataSourcesConfig.Catalog dsCatalog : catalogs) {
                if (catalogName.equals(dsCatalog.name)) {
                    return dsCatalog;
                }
            }
        }
        return null;
    }

    /**
     * Get array of DataSourcesConfig.Catalog returning only one entry if the
     * catalog was specified as a property in the request or all catalogs
     * associated with the Datasource if there was no catalog property.
     *
     * @param request
     * @param ds
     * @return Array of DataSourcesConfig.Catalog
     */
    public DataSourcesConfig.Catalog[] getCatalogs(
            XmlaRequest request,
            DataSourcesConfig.DataSource ds) {

        Map<String, String> properties = request.getProperties();
        final String catalogName =
            properties.get(PropertyDefinition.Catalog.name());
        if (catalogName != null) {
            DataSourcesConfig.Catalog dsCatalog = getCatalog(ds, catalogName);
            return new DataSourcesConfig.Catalog[] { dsCatalog };
        } else {
            // no catalog specified in Properties so return them all
            return ds.catalogs.catalogs;
        }
    }

    /**
     * Returns the DataSourcesConfig.Catalog associated with the
     * catalog name that is part of the request properties or
     * null if there is no catalog with that name.
     *
     * @param request
     * @param ds
     * @return DataSourcesConfig Catalog or null
     * @throws XmlaException
     */
    public DataSourcesConfig.Catalog getCatalog(
            XmlaRequest request,
            DataSourcesConfig.DataSource ds)
                throws XmlaException {

        Map<String, String> properties = request.getProperties();
        final String catalogName =
            properties.get(PropertyDefinition.Catalog.name());
        DataSourcesConfig.Catalog dsCatalog = getCatalog(ds, catalogName);
        if ((dsCatalog == null) && (catalogName == null)) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_CONNECTION_DATA_SOURCE_CODE,
                HSB_CONNECTION_DATA_SOURCE_FAULT_FS,
                Util.newError("no catalog named '" + catalogName + "'"));
        }
        return dsCatalog;
    }

    private TabularRowSet executeColumnQuery(XmlaRequest request)
        throws XmlaException
    {
        checkFormat(request);

        DataSourcesConfig.DataSource ds = getDataSource(request);
        DataSourcesConfig.Catalog dsCatalog = getCatalog(request, ds);
        String role = request.getRole();

        final Connection connection = getConnection(dsCatalog, role);
        final String statement = request.getStatement();
        final Query query = connection.parseQuery(statement);
        final Result result = connection.execute(query);
        Cell dtCell = result.getCell(new int[] {0, 0});

        if (!dtCell.canDrillThrough()) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_NOT_ALLOWED_CODE,
                HSB_DRILL_THROUGH_NOT_ALLOWED_FAULT_FS,
                Util.newError("Cannot do DillThrough operation on the cell"));
        }

        final Map<String, String> properties = request.getProperties();
        String dtSql = null;
        java.sql.Connection sqlConn = null;
        Statement stmt = null;
        ResultSet rs = null;
        dtSql = dtCell.getDrillThroughSQL(true);

        String fromClause = " from ";
        String dtSqlNew = "select ";
        int index = dtSql.indexOf("from");
        String whereClause = " " + dtSql.substring(index);
        final String fieldNames = properties.get(PropertyDefinition.TableFields.name());
        StringTokenizer st = new StringTokenizer(fieldNames, ",");
        drillThruColumnNames.clear();
        while (st.hasMoreTokens()) {
            drillThruColumnNames.add(st.nextToken().toString());
        }

        // Create Select Clause
        for (int i = 0; i < drillThruColumnNames.size() - 1; i++) {
            dtSqlNew += drillThruColumnNames.get(i) + ",";
        }
        dtSqlNew += drillThruColumnNames.get(drillThruColumnNames.size() - 1) + " ";
        dtSqlNew += whereClause;
        dtSql = dtSqlNew;


        try {
            sqlConn = ((RolapConnection) connection).getDataSource().getConnection();
            stmt = sqlConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

            int count = -1;
            if (MondrianProperties.instance().EnableTotalCount.booleanValue()) {
                String temp = dtSql.toUpperCase();
                int fromOff = temp.indexOf("FROM");
                StringBuilder buf = new StringBuilder();
                buf.append("select count(*) ");
                buf.append(dtSql.substring(fromOff));

                String countSql = buf.toString();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Advanced drill through counting sql: " + countSql);
                }
                rs = stmt.executeQuery(countSql);
                rs.next();
                count = rs.getInt(1);
                rs.close();
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Advanced drill through sql: " + dtSql);
            }
            rs = stmt.executeQuery(dtSql);

            return new TabularRowSet(
                rs, request.drillThroughMaxRows(),
                request.drillThroughFirstRowset(), count,
                ResultSet.TYPE_FORWARD_ONLY);

        } catch (XmlaException xex) {
            throw xex;
        } catch (SQLException sqle) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_SQL_CODE,
                HSB_DRILL_THROUGH_SQL_FAULT_FS,
                Util.newError(sqle,
                    "Errors when executing Advanced DrillThrough sql '" + dtSql + "'"));
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
                if (sqlConn != null && !sqlConn.isClosed()) sqlConn.close();
            } catch (SQLException ignored) {
            }
        }
    }
}

// End XmlaHandler.java
