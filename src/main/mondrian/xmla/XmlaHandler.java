/*
// $Id$
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
import mondrian.rolap.RolapConnection;
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

    private final Map dataSourcesMap;
    private CatalogLocator catalogLocator = null;

    private static final int ROW_SET     = 1;
    private static final int MD_DATA_SET = 2;

    private static final String ROW_SET_XML_SCHEMA = computeXsd(ROW_SET);
    private static final String EMPTY_ROW_SET_XML_SCHEMA = 
                                    computeEmptyXsd(ROW_SET);

    private static final String MD_DATA_SET_XML_SCHEMA = computeXsd(MD_DATA_SET);
    private static final String EMPTY_MD_DATA_SET_XML_SCHEMA = 
                                    computeEmptyXsd(MD_DATA_SET);

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
        Map map = new HashMap();
        for (int i = 0; i < dataSources.dataSources.length; i++) {
            DataSourcesConfig.DataSource ds = dataSources.dataSources[i];
            if (map.containsKey(ds.getDataSourceName())) {
                // This is not an XmlaException
                throw Util.newError(
                        "duplicated data source name '" +
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
    public void process(XmlaRequest request, XmlaResponse response) 
            throws XmlaException {
        int method = request.getMethod();
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
    }

    private void execute(XmlaRequest request, XmlaResponse response)
            throws XmlaException {
        // Check response's rowset format in request
        String propertyName = null;
        try {
            if (request.isDrillThrough()) {
                propertyName = PropertyDefinition.Format.name;
                final String formatName = (String) request.getProperties().get(propertyName);
                Enumeration.Format format = Enumeration.Format.getValue(formatName);
                if (format != Enumeration.Format.Tabular) {
                    throw new XmlaException(
                        CLIENT_FAULT_FC,
                        HSB_DRILL_THROUGH_FORMAT_CODE, 
                        HSB_DRILL_THROUGH_FORMAT_FAULT_FS,
                        new UnsupportedOperationException("<Format>: only 'Tabular' allowed when drilling through"));
                }
            } else {
System.out.println("XmlaHandler.execute: TODO NOT checking format");
                propertyName = PropertyDefinition.Format.name;
                final String formatName = (String) request.getProperties().get(propertyName);
                if (formatName != null) {
                    Enumeration.Format format = Enumeration.Format.getValue(formatName);
                    if (format != Enumeration.Format.Multidimensional) {
System.out.println("XmlaHandler.execute: format NOT Enumeration.Format.Multidimensional");
                    //    throw new UnsupportedOperationException("<Format>: only 'Multidimensional' currently supported");
                    }
                }
                propertyName = PropertyDefinition.AxisFormat.name;
                final String axisFormatName = (String) request.getProperties().get(propertyName);
                if (axisFormatName != null) {
                    Enumeration.AxisFormat axisFormat = Enumeration.AxisFormat.getValue(axisFormatName);

                    if (axisFormat != Enumeration.AxisFormat.TupleFormat) {
System.out.println("XmlaHandler.execute: axisFormat NOT Enumeration.AxisFormat.TupleFormat");
                    //    throw new UnsupportedOperationException("<AxisFormat>: only 'TupleFormat' currently supported");
                    }
                }
            }
        } catch (Error e) {
            // Incredible, the EnumeratedValues getValue method throws an
            // java.lang.Error
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_UNSUPPORTED_OPERATION_CODE, 
                HSB_UNSUPPORTED_OPERATION_FAULT_FS,
                new UnsupportedOperationException(
                    "Property <" + propertyName + "> must be provided"));
        }
        final String contentName =
            (String) request.getProperties().get(PropertyDefinition.Content.name);
        // default value is SchemaData
        Enumeration.Content content = (contentName == null)
            ? CONTENT_DEFAULT
            : Enumeration.Content.getValue(contentName);

        // Handle execute
        QueryResult result = null;
        if (request.isDrillThrough()) {
            result = executeDrillThroughQuery(request);
        } else {
            result = executeQuery(request);
        }

        SaxWriter writer = response.getWriter();
        writer.startDocument();

        writer.startElement("xmla:ExecuteResponse", new String[] {
            "xmlns:xmla", NS_XMLA});
        writer.startElement("xmla:return");
        writer.startElement("root", new String[] {
            "xmlns", 
                (result == null) 
                    ?  NS_XMLA_EMPTY
                    : (request.isDrillThrough() 
                        ? NS_XMLA_ROWSET : NS_XMLA_MDDATASET),
            "xmlns:xsi", NS_XSI,
            "xmlns:xsd", NS_XSD,
            "xmlns:EX", NS_XMLA_EX
        });

        if ((content == Enumeration.Content.Schema)
                || (content == Enumeration.Content.SchemaData)) {
            if (result != null) {
                if (request.isDrillThrough()) {
                    writer.verbatim(ROW_SET_XML_SCHEMA);
                } else {
                    writer.verbatim(MD_DATA_SET_XML_SCHEMA);
                }
            } else {
                if (request.isDrillThrough()) {
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
            "xmlns:sql", "urn:schemas-microsoft-com:xml-sql",
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
/*
        writer.startElement("xsd:sequence", new String[] {
            "maxOccurs", "unbounded"
        });
*/

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
            "xmlns:sql", "urn:schemas-microsoft-com:xml-sql",
            "elementFormDefault", "qualified"
        });

            writer.element("xsd:element", new String[] {
                "name", "root"
            });

        writer.endElement(); // xsd:schema
    }

    private QueryResult executeDrillThroughQuery(XmlaRequest request)
            throws XmlaException {
        final String statement = request.getStatement();
        final Connection connection = getConnection(request);
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

        String dtSql = dtCell.getDrillThroughSQL(true);
        TabularRowSet rowset = null;
        java.sql.Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;



        try {
            conn = ((RolapConnection) connection).getDataSource().getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY);

            int count = -1;
            if (MondrianProperties.instance().EnableTotalCount.booleanValue()) {
                String temp = dtSql.toUpperCase();
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
                count = rs.getInt(1);
                rs.close();
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("drill through sql: " + dtSql);
            }
            rs = stmt.executeQuery(dtSql);
            rowset = new TabularRowSet(rs, request.drillThroughMaxRows(),
                                       request.drillThroughFirstRowset(), count);
        } catch (XmlaException xex) {
            throw xex;
        } catch (SQLException sqle) {
            throw new XmlaException(
                SERVER_FAULT_FC,
                HSB_DRILL_THROUGH_SQL_CODE, 
                HSB_DRILL_THROUGH_SQL_FAULT_FS,
                Util.newError(sqle, 
                    "Errors when executing DrillThrough sql '" + dtSql + "'"));
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

            // skip to first rowset specified in request
            rs.absolute(firstRowset <= 0 ? 1 : firstRowset);

            // populate data
            rows = new ArrayList();
            maxRows = (maxRows <= 0 ? Integer.MAX_VALUE : maxRows);
            do {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                rows.add(row);
            } while (rs.next() && --maxRows > 0);
        }

        public void unparse(SaxWriter writer) throws SAXException {
            String[] encodedHeader = new String[header.length];
            for (int i = 0; i < header.length; i++) {
                // replace invalid XML element name, like " ", with "_x0020_" in
                // column headers, otherwise will generate a badly-formatted xml
                // doc.
                encodedHeader[i] = XmlaUtil.encodeElementName(header[i]);
            }

            // write total count row if enabled
            if (totalCount >= 0) {
                String countStr = Integer.toString(totalCount);
                writer.startElement("row");
                for (int i = 0; i < encodedHeader.length; i++) {
                    writer.startElement(encodedHeader[i]);
                    writer.characters(countStr);
                    writer.endElement();
                }
                writer.endElement(); // row
            }

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

    private QueryResult executeQuery(XmlaRequest request) 
            throws XmlaException {
        final String statement = request.getStatement();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("mdx: \"" + statement + "\"");
        }

        if ((statement == null) || (statement.length() == 0)) {
            return null;
        } else {
            final Connection connection = getConnection(request);
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

            return new MDDataSet(result);
        }
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
            writer.startElement("AxisInfo", new String[] { "name", axisName});

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
                // TODO:
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
            writer.startElement("Axis", new String[] { "name", axisName});
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
            if (cell.isNull()) {
                // Ignore null cell like MS AS
                return;
            }

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

    private void discover(XmlaRequest request, XmlaResponse response)
            throws XmlaException {

        final RowsetDefinition rowsetDefinition =
            RowsetDefinition.getValue(request.getRequestType());
        Rowset rowset = rowsetDefinition.getRowset(request, this);

        try {
            final String formatName =
                (String) request.getProperties().get(PropertyDefinition.Format.name);
            Enumeration.Format format = Enumeration.Format.getValue(formatName);
            if (format != Enumeration.Format.Tabular) {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_DISCOVER_FORMAT_CODE, 
                    HSB_DISCOVER_FORMAT_FAULT_FS,
                    new UnsupportedOperationException("<Format>: only 'Tabular' allowed in Discover method type"));
            }
        } catch (Error e) {
            // Incredible, the EnumeratedValues getValue method throws an
            // java.lang.Error
        }
        final String contentName =
            (String) request.getProperties().get(PropertyDefinition.Content.name);
        // default value is SchemaData
        Enumeration.Content content = (contentName == null)
            ? CONTENT_DEFAULT
            : Enumeration.Content.getValue(contentName);

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
     * Returns a Mondrian connection as specified by a set of properties
     * (especially the "Connect string" property).
     */
    Connection getConnection(XmlaRequest request) 
                throws XmlaException {
        Map properties = request.getProperties();
        final String dataSourceInfo =
            (String) properties.get(PropertyDefinition.DataSourceInfo.name);
        if (!dataSourcesMap.containsKey(dataSourceInfo)) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_CONNECTION_DATA_SOURCE_CODE, 
                HSB_CONNECTION_DATA_SOURCE_FAULT_FS,
                Util.newError("no data source is configured with name '" + dataSourceInfo + "'"));
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("XmlaHandler.getConnection: dataSourceInfo=" + 
                    dataSourceInfo);
        }

        DataSourcesConfig.DataSource ds =
            (DataSourcesConfig.DataSource) dataSourcesMap.get(dataSourceInfo);
        if (LOGGER.isDebugEnabled()) {
            if (ds == null) {
                LOGGER.debug("XmlaHandler.getConnection: ds is null");
            } else {
                LOGGER.debug("XmlaHandler.getConnection: ds.dataSourceInfo=" + 
                    ds.getDataSourceInfo());
            }
        }
        Util.PropertyList connectProperties =
            Util.parseConnectString(ds.getDataSourceInfo());

        String catalog =
                catalogLocator.locate(connectProperties.get("catalog"));

        if (LOGGER.isDebugEnabled()) {
            if (catalog == null) {
                LOGGER.debug("XmlaHandler.getConnection: catalog is null");
            } else {
                LOGGER.debug("XmlaHandler.getConnection: catalog=" + catalog);
            }
        }

        connectProperties.put("catalog", catalog);

        // Checking access
        if (!DataSourcesConfig.DataSource.AUTH_MODE_UNAUTHENTICATED.equalsIgnoreCase(ds.getAuthenticationMode()) &&
                null == request.getRole()) {
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_ACCESS_DENIED_CODE, 
                HSB_ACCESS_DENIED_FAULT_FS,
                new SecurityException("Access denied for data source needing authentication")
                );
        }

        connectProperties.put("role", request.getRole());
        RolapConnection conn =
            (RolapConnection) DriverManager.getConnection(connectProperties, null, false);

        return conn;
    }

}

// End XmlaHandler.java
